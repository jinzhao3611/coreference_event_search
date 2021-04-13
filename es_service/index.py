from typing import Iterator, Dict, Union, Sequence

from elasticsearch_dsl import Index

from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk

from es_service.doc_template import Event


class ESIndex(object):
    def __init__(
        self,
        index_name: str,
        docs: Union[Iterator[Dict], Sequence[Dict]],
    ):
        # connect to your host (for elasticsearch)
        connections.create_connection(hosts=["localhost"], timeout=100, alias="default")
        self.index = index_name
        es_index = Index(self.index)

        # delete existing index that has the same name
        if es_index.exists():
            es_index.delete()
        es_index.document(Event)

        es_index.create()
        if docs is not None:
            self.load(docs)

    def load(self, events: Union[Iterator[Dict], Sequence[Dict]]):
        # es_docs = []
        def iter_doc(events: Union[Iterator[Dict], Sequence[Dict]]):
            for i, doc in enumerate(events):
                es_doc = Event(_id=i)
                es_doc.doc_id = doc["doc_id"]
                es_doc.trigger_word = doc["trigger_word"]
                es_doc.trigger_id = doc["trigger_id"]
                es_doc.context = doc["context"]
                es_doc.sentence = doc["sentence"]
                es_doc.source = doc["source"]
                es_doc.context_vec = doc["context_vec"]
                yield es_doc
                # es_docs.append(es_doc)

        # bulk insertion
        bulk(
            connections.get_connection(),
            (d.to_dict(include_meta=True, skip_empty=False) for d in iter_doc(events)),
        )
