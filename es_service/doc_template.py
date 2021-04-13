from elasticsearch_dsl import (
    Document,
    Text,
    Keyword,
    DenseVector,
)


# document index structure
class Event(Document):
    trigger_word = Text()
    trigger_id = Keyword()
    doc_id = Keyword()
    context = Text()
    sentence = Text()
    source = Text()
    context_vec = DenseVector(dims=768)  # sentence BERT embedding

    def save(self, *args, **kwargs):
        return super(Event, self).save(*args, **kwargs)


if __name__ == "__main__":
    pass
