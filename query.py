from typing import List
import json

from elasticsearch_dsl import Search
from elasticsearch_dsl.query import Match, MatchAll, ScriptScore, Ids
from elasticsearch_dsl.connections import connections
from embedding_service.client import EmbeddingClient
from elasticsearch import Elasticsearch  # get the total event number
from utils.load_data import load_events_from_folder
import collections
from es_service.doc_template import Event

encoder = EmbeddingClient(host="localhost", port="8080")


def generate_script_score_query(query_vector: List[float], vector_name: str):
    """
    generate an ES query that match all documents based on the cosine similarity
    :param query_vector: text embedding
    :param vector_name: embedding type
    :return:
    """
    q_script = ScriptScore(
        query={"match_all": {}},
        script={
            "source": f"cosineSimilarity(params.query_vector, '{vector_name}') + 1.0",
            "params": {"query_vector": query_vector},
        },
    )
    return q_script


def search(index, query):
    s = Search(using="default", index=index).query(query)[
        :15
        ]  # initialize a query and return top ten results
    response = s.execute()
    ems_wz_info = []
    for hit in response:
        # print(
        #     hit.meta.id, round(hit.meta.score, 4), hit.trigger_word, hit.source, hit.doc_id, hit.trigger_id,
        #     hit.context, hit.sentence, sep="\t"
        # )  # print the document id that is assigned by ES index, score and title
        ems_wz_info.append({'query_id': hit.meta.id,
                            'score': round(hit.meta.score, 4),
                            'trigger': hit.trigger_word,
                            'source': hit.source,
                            'doc_id': hit.doc_id,
                            'trigger_id': hit.trigger_id,
                            'event_mention': hit.context,
                            'sentence': hit.sentence,
                            })
    return ems_wz_info


def get_queries():
    events = []
    trigger_id_mapping = collections.defaultdict(list)
    connections.create_connection(hosts=["localhost"], timeout=100, alias="default")
    for i in range(170055):
        event = Event.get(i, index='covid_events_index')
        trigger_id_mapping[event.trigger_word].append(event.meta.id)
        if (i + 1) % 1000 == 0:
            print(i)

    sorted_trigger_id_mapping = sorted(trigger_id_mapping.items(), key=lambda p: len(p[1]), reverse=True)

    # trigger_freq = sorted([e.trigger_word for e in events], key=lambda x: (counts[x], x), reverse=True)
    # events_by_freq_trigger = sorted(events, key=lambda x: (counts[x.trigger_word], x.trigger_word), reverse=True)

    return sorted_trigger_id_mapping


def get_candidates(sorted_trigger_id_mapping):
    connections.create_connection(hosts=["localhost"], timeout=100, alias="default")
    jsonl_file = open('/Users/jinzhao/Desktop/coref_candidates.jsonl', 'w', encoding='utf-8')
    for trigger, ids in sorted_trigger_id_mapping:
        print(f"writing events of \"{trigger}\" to jsonl ...")
        for i in ids:
            query = Event.get(i, index='covid_events_index')
            query_dict = {'query_id': query.meta.id,
                          'trigger': query.trigger_word,
                          'source': query.source,
                          'score': -1,
                          'doc_id': query.doc_id,
                          'trigger_id': query.trigger_id,
                          'event_mention': query.context,
                          'sentence': query.sentence,
                          }
            q_vector = generate_script_score_query(
                query.context_vec, "context_vec"
            )  # score documents based on cosine similarity

            candidates = search("covid_events_index", q_vector)  # search

            query_info = {'query': query_dict, 'candidates': candidates}
            jsonl_file.write(json.dumps(query_info) + '\n')

    # query_text = ["Johnson, 55, was admitted to St. Thomas' Hospital on April 5"]
    # query_text = ["Addressing the nation via a video conference , Prime Minister Narendra Modi made it clear that the threat could only be combatted with the full cooperation of the people , a token demonstration of which , he said , would be to light candles , lamps and mobile torches for nine minutes on April 5 at 9 pm ."]


if __name__ == "__main__":
    connections.create_connection(hosts=["localhost"], timeout=100, alias="default")
    # q_match_all = MatchAll()  # match all documents
    # q_basic = Match(
    #     context={"query": "doctor"}
    # )  # match "D.C" in the title field of the index, using BM25 as default
    q_match_ids = Ids(values=[170054])  # match ids
    # es = Elasticsearch([{'host': 'localhost', 'port': '9200', 'timeout': 60}])
    # print(es.cat.count("covid_events_index", params={"format": "json"}))
    # query_vector = encoder.encode(['This is a precautionary step , as the Prime Minister continues to have persistent symptoms of coronavirus ten days after testing positive for the virus , " a Downing Street spokesperson said .']).tolist()[
    #     0
    # ]  # get the embedding for the query text
    # q_vector = generate_script_score_query(
    #     query_vector, "context_vec"
    # )  # score documents based on cosine similarity
    # search("covid_events_index", q_match_ids)  # search

    sorted_trigger_id_mapping = get_queries()
    get_candidates(sorted_trigger_id_mapping)
    # e = Event.get(0, index='covid_events_index')
    # print(type(e.meta.id))
