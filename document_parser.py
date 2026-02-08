from dotenv import load_dotenv; load_dotenv()
from typing import Optional
import os
from docling.document_converter import DocumentConverter
from pathlib import Path
import pandas as pd

class DocumentParser():

    def __init__(
        self,
        out_dir: Optional[str] = None,
        use_docling_loader: bool = True,
    ) -> None:
        
        self.use_docling_loader = use_docling_loader
        self.out_dir = out_dir or os.getcwd()

    def ingest_document(self, file_path: str):

        path = Path(file_path)
        filename = path.name
        stem = path.stem
        extension = path.suffix

        if path.suffix == ".pdf":

            print(f"Processing file: {filename}")

            self.ticker = stem.split("_")[0]
            self.FP = stem.split("_")[-2]

            converter = DocumentConverter()
            #self.result is of document result class. Within self.result class includes a Document class that has attributes like texts, tables and methods like export to markdown. 
            self.result = converter.convert(file_path)
            self.doc = self.result.document
            
            #export document as markdown
            self.doc.save_as_markdown(os.path.join(self.out_dir,"markdown",stem + ".md"))


            #Create table folder if it does not exist
            tables_folder_path = Path(os.path.join(self.out_dir,"tables"))
            tables_folder_path.mkdir(parents = True, exist_ok= True)

            for idx, table in enumerate(self.doc.tables):
                #docling now requires export to dataframe to take in doc. 
                df = table.export_to_dataframe(doc=self.doc)
                df.to_csv(tables_folder_path/(stem + f"-table-{idx}"))

        else:
            print("Invalid document format")
       