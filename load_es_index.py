import argparse
import time
from typing import List, Dict, Union, Iterator

import attr

from es_service.index import ESIndex
from utils.load_data import load_events_from_folder, load_event_with_embedding, Event
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class IndexLoader:
    """
    load document index to Elasticsearch
    """

    def __init__(self, index, docs):

        self.index_name = index
        self.docs: Union[Iterator[Dict], List[Dict]] = docs

    def load(self) -> None:
        st = time.time()
        logger.info(f"Building index ...")
        ESIndex(self.index_name, self.docs)
        logger.info(
            f"=== Built {self.index_name} in {round(time.time() - st, 2)} seconds ==="
        )

    @staticmethod
    def iter_events(events: Iterator[List[Event]]):
        for eves in events:
            for e in eves:
                yield attr.asdict(e)

    @classmethod
    def from_folder(cls, index_name: str, folder_path: str) -> "IndexLoader":
        # without embedding
        try:
            return IndexLoader(
                index_name, cls.iter_events(load_events_from_folder(folder_path))
            )
        except FileNotFoundError:
            raise Exception(f"Cannot find {folder_path}!")

    @classmethod
    def from_docs_jsonl(cls, index_name: str, docs_jsonl: str) -> "IndexLoader":
        try:
            return IndexLoader(index_name, load_event_with_embedding(docs_jsonl))
        except FileNotFoundError:
            raise Exception(f"Cannot find {docs_jsonl}!")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--index_name",
        required=True,
        type=str,
        help="name of the ES index",
    )
    parser.add_argument(
        "--data_path",
        required=True,
        type=str,
        help="path to the annotation folder/file path",
    )

    args = parser.parse_args()
    idx_loader = IndexLoader.from_docs_jsonl(args.index_name, args.data_path)
    idx_loader.load()


if __name__ == "__main__":
    main()
