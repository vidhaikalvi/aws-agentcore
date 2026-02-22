from pathlib import Path
from rank_bm25 import BM25Okapi
import datetime as dt
import json
from dataclasses import dataclass


def char_ngrams(text, n):
    """Generate character-level n-grams"""
    text = text.lower()
    return [text[i : i + n] for i in range(len(text) - n + 1)]


@dataclass
class KYCQueryEngine:
    data: list[dict]
    text_fields_to_index: list[str]
    ngram_size: int = 2
    unique_id_field: str | None = None

    def __post_init__(self):

        self.field_indexes = {}

        for field in self.text_fields_to_index:
            corpus = [str(doc.get(field, "")) for doc in self.data]
            tokenized_corpus = [char_ngrams(doc, 2) for doc in corpus]
            self.field_indexes[field] = BM25Okapi(tokenized_corpus)

    @classmethod
    def from_json_lines(
        cls,
        file_path: str | Path,
        text_fields_to_index: list[str],
        unique_id_field: str | None = None,
        ngram_size: int = 2,
    ):
        with open(file_path, "r", encoding="utf-8") as f:
            data_rows = [json.loads(line) for line in f]
        return cls(data_rows, text_fields_to_index, ngram_size, unique_id_field)

    def query_bm25(self, text: str, query_field: str, top_n: int = 5) -> list[dict]:
        """
        Query the BM25 index and return the top N results with their scores.

        Args:
            text (str): The query string to search for.
            top_n (int, optional): Number of top results to return. Defaults to 5.

        Returns:
            list[dict]: List of top N records matching the query.
        """

        if query_field not in self.field_indexes:
            raise ValueError(
                f"Field '{query_field}' is not indexed. Available fields: {list(self.field_indexes.keys())}"
            )
        else:
            index = self.field_indexes[query_field]

        tokenized_query = char_ngrams(text, self.ngram_size)
        scores = index.get_scores(tokenized_query)
        top_n_indices = scores.argsort()[-top_n:][::-1]

        res = [self.data[i] for i in top_n_indices]

        return res

    def query_unique_id(self, unique_id_value: str) -> dict | None:
        """
        Query the dataset for a record with the specified unique ID.

        Args:
            unique_id_value (str): The unique ID value to search for.

        Returns:
            dict | None: The record matching the unique ID, or None if not found.
        """
        if self.unique_id_field is None:
            raise ValueError("Unique ID field is not set.")

        for record in self.data:
            if record.get(self.unique_id_field) == unique_id_value:
                return record
        return None
