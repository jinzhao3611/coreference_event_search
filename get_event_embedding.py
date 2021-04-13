import json
import attr
from embedding_service.client import EmbeddingClient
from utils.load_data import load_events_from_folder

encoder = EmbeddingClient(
    host="localhost", port="8080"
)  # connect to the embedding server


def attach_embeddings_to_event(folder_path: str, out_file: str):
    out_file_obj = open(out_file, "w", encoding="utf-8")
    for i, events in enumerate(load_events_from_folder(folder_path)):
        contexts = [event.context for event in events]
        events = [attr.asdict(event) for event in events]
        context_embeddings = encoder.encode(contexts).tolist()
        assert len(events) == len(context_embeddings)

        for embed, event in zip(context_embeddings, events):
            event["context_vec"] = embed
            out_file_obj.write(json.dumps(event) + "\n")
        if i % 10 == 0:
            print(f"Loaded {i} document ...")


if __name__ == "__main__":
    folder_path = "data/covid_event_for_event_coref"
    out_jsonl_file = "data/events_with_embed.jsonl"
    attach_embeddings_to_event(folder_path, out_jsonl_file)
