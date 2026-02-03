from elasticsearch7.helpers import bulk
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_core.documents import Document
import json
import os
from typing import Iterator

from file_conversion.pdfs import mets_parser
from logger import create_log
from managers import ElasticsearchManager, S3Manager
from model import Record, ESPage
from text_pipeline.text_cleaner import TextCleaner


class RecordEmbedder:
    def __init__(self, es_manager: ElasticsearchManager, storage_manager: S3Manager):
        self.storage_manager = storage_manager
        self.es_manager = es_manager
        self.embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
        self.logger = create_log(__name__)

        if not self.es_manager.client.indices.exists(index=ESPage.Index.name):
            ESPage.init(index=ESPage.Index.name)

    def embed(self, record: Record, barcode: str, batch_size: int = 8):
        document_buffer = []

        for page in self.get_pages(barcode):
            page_text = (
                self.storage_manager.get_object(
                    key=f"grin/{barcode}/{page.text_file.location}",
                    bucket=os.environ["PRIVATE_FILE_BUCKET"],
                )["Body"]
                .read()
                .decode("utf-8")
            )
            cleaned_text = (
                TextCleaner(text=page_text)
                .remove_non_printable_characters()
                .remove_redudant_newlines()
                .strip()
                .text
            )

            if not cleaned_text or len(cleaned_text) < 100:
                continue

            document = Document(
                page_content=cleaned_text,
                metadata={
                    "pageId": page.id,
                    "recordId": record.id,
                },
            )
            document_buffer.append(document)

            if len(document_buffer) >= batch_size:
                self._embed(document_buffer)
                document_buffer.clear()

        if document_buffer:
            self._embed(document_buffer)

        self.logger.info(f"Embedded full-text for {record}")

    def _embed(self, document_batch: list[Document]):
        texts = [document.page_content for document in document_batch]
        embeddings = self.embedder.embed_documents(texts)
        actions = []

        for document, embedding in zip(document_batch, embeddings):
            page = ESPage(
                text=document.page_content,
                record_id=document.metadata["recordId"],
                page_id=document.metadata["pageId"],
                embedding=embedding,
            )
            page.meta.id = f"{page.record_id}_{page.page_id}"

            actions.append(page.to_action())

        bulk(self.es_manager.client, actions)

    def get_mets_file(self, barcode: str) -> mets_parser.METSFile:
        return mets_parser.METSFile.from_mets_str(
            self.storage_manager.get_object(
                key=f"grin/{barcode}/NYPL_{barcode}.xml",
                bucket=os.environ["PRIVATE_FILE_BUCKET"],
            )["Body"].read()
        )

    def get_pages(self, barcode: str) -> Iterator[mets_parser.Page]:
        return self.get_mets_file(barcode).iter_pages()
