import json
import os
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Union, Iterator

import attr

FILE_PREFIX = "bert-base-cased_"
FILE_SUFFIX = "_auto_nodes"
EASY_TO_READ = "txt_easy_to_read.txt"


@attr.s(frozen=True)
class Event:
    trigger_word: str = attr.attrib()
    trigger_id: str = attr.attrib()
    context: str = attr.attrib()
    sentence: str = attr.attrib()
    doc_id: int = attr.attrib()
    source: str = attr.attrib()

    @classmethod
    def from_sentence(
        cls,
        trigger_id: str,
        start: int,
        end: int,
        trigger_word: str,
        snt_tokens: List[str],
        doc_id: int,
        source: str,
    ):
        context = get_context_window_from_trigger(start, end, snt_tokens)
        return cls(
            trigger_word, trigger_id, context, " ".join(snt_tokens), doc_id, source
        )


def get_context_window_from_trigger(
    start: int, end: int, snt_tokens: List[str], size: int = 3
) -> str:
    context_words = snt_tokens[max(0, int(start) - size) : int(end) + size + 1]
    return " ".join(context_words)


def get_events_from_doc(
    sentences: Dict[str, list], triggers: Dict[str, str], doc_id: int, source: str
) -> List[Event]:
    events = []
    for trigger_id in triggers:
        snt_id, start, end = trigger_id.split("_")
        snt_tokens = sentences[snt_id]
        event = Event.from_sentence(
            trigger_id,
            int(start),
            int(end),
            triggers[trigger_id],
            snt_tokens,
            doc_id,
            source,
        )
        events.append(event)
    return events


def load_events_from_file(annotation_file: Union[str, os.PathLike]):
    source = Path(annotation_file).stem.split(".")[0][len(FILE_PREFIX) :][
        : -len(FILE_SUFFIX)
    ]
    with open(annotation_file, "r", encoding="utf-8") as f:
        doc_id = 0
        sentences = defaultdict(list)
        triggers = defaultdict(str)
        snt_line = False
        event_line = False
        for line in f:
            line = line.strip()
            if not line:
                yield get_events_from_doc(sentences, triggers, doc_id, source)
                sentences = defaultdict(list)
                triggers = defaultdict(list)
                doc_id += 1
                snt_line = False
                event_line = False
                continue
            elif line == "SNT_LIST":
                snt_line = True
                event_line = False
                continue
            elif line == "EDGE_LIST":
                snt_line = False
                event_line = True
                continue
            else:
                if snt_line:
                    snt_id, snt = line.split("\t")
                    sentences[snt_id] = snt.split()
                elif event_line:
                    event_id, trigger, *other = line.split("\t")
                    triggers[event_id] = trigger
                else:
                    raise ValueError(f"unknown line: {line}")


def load_events_from_folder(annotation_folder: str):
    for file_name in os.listdir(annotation_folder):
        if file_name.endswith(EASY_TO_READ):
            yield from load_events_from_file(
                Path(annotation_folder).joinpath(file_name)
            )


def load_event_with_embedding(jl_path: Union[str, os.PathLike]) -> Iterator[Dict]:
    with open(jl_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            yield json.loads(line)


if __name__ == "__main__":
    pass
