from elasticsearch_dsl import InnerDoc, Document, Date, analyzer, Keyword, Text


plain_ascii = analyzer(
    "plain_ascii", tokenizer="standard", filter=["lowercase", "stop", "asciifolding"]
)


class BaseDoc(Document):
    date_created = Date()
    date_modified = Date()

    def save(self, **kwargs):
        for rel in dir(self):
            if isinstance(getattr(self, rel), set):
                setattr(self, rel, list(getattr(self, rel)))
        return super().save(**kwargs)


class BaseInner(InnerDoc):
    date_created = Date()
    date_modified = Date()

    def save(self, **kwargs):
        for rel in dir(self):
            if isinstance(getattr(self, rel), set):
                setattr(self, rel, list(getattr(self, rel)))
        return super().save(**kwargs)


class PerLanguageField(InnerDoc):
    language = Keyword()
    default = Text(
        analyzer="plain_ascii",
        fields={"icu": {"type": "text", "analyzer": "icu_analyzer"}},
    )
    en = Text(analyzer="english")
    de = Text(analyzer="german")
    fr = Text(analyzer="french")
    sp = Text(analyzer="spanish")
    po = Text(analyzer="polish")
    nl = Text(analyzer="dutch")
    it = Text(analyzer="italian")
    da = Text(analyzer="danish")
    ar = Text(analyzer="arabic")
    zh = Text(analyzer="smartcn")
    el = Text(analyzer="greek")
    hi = Text(analyzer="hindi")
    fa = Text(analyzer="persian")
    ja = Text(analyzer="kuromoji")
    ru = Text(analyzer="russian")
    th = Text(analyzer="thai")
