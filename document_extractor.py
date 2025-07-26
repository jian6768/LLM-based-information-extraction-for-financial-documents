import torch
from langchain_docling import DoclingLoader
from langchain_docling.loader import ExportType
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain_openai import OpenAIEmbeddings, OpenAI
from langchain.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate, PromptTemplate
from langchain.chains import create_retrieval_chain

from pydantic import BaseModel, Field

from docling.document_converter import DocumentConverter
from docling.chunking import HybridChunker
from typing import Any, Dict, Iterable, List, Optional, Tuple
import os
from dotenv import load_dotenv
import pandas as pd



def create_response_schema(extraction_key, extracted_field_name, extracted_FY, units = "millions", data_type="float"):
    return ResponseSchema(name = f"{extraction_key}", description = f"{extracted_FY} {extracted_field_name} in {units}", type = data_type)

def create_extraction_key(extracted_field_name):
    field_name_split = extracted_field_name.split(" ")
    extraction_key = "_".join(field_name_split).lower()
    return extraction_key

def run_chain_with_retries(chain, input_dict,  max_retries = 5):
    for attempt in range(1,max_retries + 1):
        try:
            return chain.invoke(input_dict)
        except Exception as e:
            print(f"Attempt {attempt} failed. Retrying...")
            if attempt == max_retries:
                raise


class FinancialDocumentProcessor():

    def __init__(
        self,
        data_dir: str,
        model_name: str = "gpt-4o-mini",
        openai_api_key: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
        use_docling_loader: bool = False,
        save_markdown: bool = True,
        out_dir: Optional[str] = None,
    ) -> None:
        load_dotenv()
        self.data_dir = data_dir
        
        self.model_name = model_name
        self.api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.use_docling_loader = use_docling_loader
        self.save_markdown = save_markdown
        self.out_dir = out_dir or os.getcwd()

        self.ls_all_is_dict = []
        self.ls_all_bs_dict = []
        

        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not found in environment or passed explicitly.")

        self.llm = OpenAI(
            api_key=self.api_key,
            model=self.model_name,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
        )

        # Dataframes accumulate across files
        self.df_bs = pd.DataFrame(columns=[
            "ticker", "FP", "total_assets", "investment_properties", "total_debt",
            "total_liabilities", "net_assets", "nta_per_unit",
        ])
        self.df_is = pd.DataFrame(columns=[
            "ticker", "FP", "total_revenue", "direct_property_expense",
            "responsible_entity_fees", "funds_from_operations", "statutory_net_profit",
        ])


    # def ingest_data(self, file_names:list):

    #     #lost files in folder
    #     # file_names = os.listdir(os.path.join("datasets","co_presentations"))

    #     #Iterate over list of all file names. 
    #     for file_name in file_names:

    #         if file_name.endswith(".pdf"):
    #             print(f"Processing file: {file_name}")

    #             self.ticker = file_name.split("_")[0]
    #             self.FP = file_name.split("_")[-2]
                
    #             source = os.path.join("datasets","co_presentations", file_name)  # document per local path or URL
    #             # print(source)
            
    #             converter = DocumentConverter()
    #             self.result = converter.convert(source)


    def ingest_document(self, file_name: str):
        if file_name.endswith(".pdf"):
            print(f"Processing file: {file_name}")

            self.ticker = file_name.split("_")[0]
            self.FP = file_name.split("_")[-2]
            
            source = os.path.join("datasets","co_presentations", file_name)  # document per local path or URL
            # print(source)
        
            converter = DocumentConverter()
            self.result = converter.convert(source)
        else:
            print("Invalid document format")
        

    def extract_tables(self):
        ls_bs = []
        ls_is = []

        for idx, table in enumerate(self.result.document.tables):
            try:
                print(f"Table {idx}:")
                # if idx == 12:

                df: pd.DataFrame = table.export_to_dataframe()
                print(df)
                text = " ".join(df.columns.to_list() + df.iloc[:,0].astype(str).tolist())
                print(text)

                is_bs = False
                is_is = False

                if all(word in text for word in ["assets","liabilities"]):
                    is_bs = True
                
                if is_bs:
                    ls_bs.append(df)

                if all(word in text for word in ["income","expense"]):
                    is_is = True

                if is_is:
                    ls_is.append(df)

                

            except:
                print("Error")

        self.df_bs = ls_bs[0] if len(ls_bs) > 0 else None
        self.df_is = ls_is[0] if len(ls_is) > 0 else None

    def extract_information(self):

        #Set up fields to iterate through
        ls_bs_fields = ["Total assets", "Investment Properties","total debt", "total liabilities","net assets", "nta per unit"]
        ls_is_fields = ["Total Revenue", "Direct Property Expense", "Responsible Entity Fees", "Funds From Operations", "Statutory Net Profit" ]

        #Set up response schemas to use. 
        response_schemas_bs = [create_response_schema(create_extraction_key(field_name),field_name,"HY25") for field_name in ls_bs_fields]
        response_schemas_is = [create_response_schema(create_extraction_key(field_name),field_name,"HY25") for field_name in ls_is_fields]

                #Set up output parser to be used. Output parser will be set up using response schemas. Format instructions will be injected into prompt. 
        bs_output_parser = StructuredOutputParser.from_response_schemas(response_schemas_bs)
        is_output_parser = StructuredOutputParser.from_response_schemas(response_schemas_is)

        bs_format_instruction = bs_output_parser.get_format_instructions()
        is_format_instruction = is_output_parser.get_format_instructions()


        # Set up template to be used. 
        system_prompt = """
        You are a meticulous assistant that can answer questions about the content of the COF HY25 Results Presentation document. Return the answer in JSON format. Do not hallucinate.
        """

        #Template inclusive of format instructions extracted from output parser. 

        template = PromptTemplate.from_template(
            "{system_prompt}\n"
            "{format_instructions}\n"
            "Context information is below.\n---------------------\n{context}\n---------------------\n"
            "Given the context information and not prior knowledge, answer the query.\n"
            "Query: {input}",   
        )

        #Set up QA chain using template, llm and output_parser
        bs_chain = template | self.llm | bs_output_parser
        is_chain = template | self.llm | is_output_parser

        #set up dictionary to be be passed into the chain.
        input_dict_bs = {'input': "Extract designated balance sheet items","system_prompt": system_prompt,"context": self.df_bs.to_markdown() ,"format_instructions":bs_format_instruction}
        input_dict_is = {'input': "Extract designated income statement items","system_prompt": system_prompt,"context": self.df_is.to_markdown(),"format_instructions":is_format_instruction}

        bs_items_extracted = {}
        is_items_extracted = {}

        #Extract information from table
        bs_items_extracted = run_chain_with_retries(bs_chain,input_dict_bs)
        is_items_extracted = run_chain_with_retries(is_chain,input_dict_is)


        #If execution succeeds: 
        try:
            if bs_items_extracted:

                bs_items_extracted["ticker"] = self.ticker
                bs_items_extracted["FP"] = self.FP 
                bs_items_extracted["id"] = f"{self.ticker}_{self.FP}"

                is_items_extracted["ticker"] = self.ticker
                is_items_extracted["FP"] = self.FP 
                is_items_extracted["id"] = f"{self.ticker}_{self.FP}"


                
                # self.ls_all_bs_dict.append(bs_items_extracted)
                # self.ls_all_is_dict.append(is_items_extracted)

                
                return bs_items_extracted, is_items_extracted


        except Exception as e:
            print(f"Error during chain execution: {e}")

    
    def store_bs_item(self, bs_items_extracted):

        from utility.helper import insert_bs_item
        """
        Store the extracted data into MongoDB.
        """
        if bs_items_extracted:
            insert_bs_item(bs_items_extracted)
            
        else:
            print("No data to store.")
        
    def store_is_item(self, is_items_extracted):

        from utility.helper import insert_is_item
        """
        Store the extracted data into MongoDB.
        """
        if is_items_extracted:
            insert_is_item(is_items_extracted)
            
        else:
            print("No data to store.")

        
    def ingest_extract_save(self, file_name:str):

        self.ingest_document(file_name)
        self.extract_tables()
        extracted_info = self.extract_information()
        self.store_bs_item(extracted_info[0])
        self.store_is_item(extracted_info[1])

        return extracted_info


        