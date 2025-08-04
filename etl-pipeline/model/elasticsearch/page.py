from elasticsearch_dsl import Document, Text, Object, DenseVector


class Page(Document):
    text = Text()
    metadata = Object()
    embedding = DenseVector(dims=768)

    class Index:
        name = "full_text_index"

    def to_action(self):
        return {
            "_op_type": "index",
            "_index": self.Index.name,
            "_id": self.meta.id,
            "_source": self.to_dict(),
        }
