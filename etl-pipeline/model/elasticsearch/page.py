from elasticsearch7_dsl import Document, Text, Integer, Keyword, DenseVector


class Page(Document):
    text = Text()
    record_id = Integer()
    page_id = Keyword()
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
