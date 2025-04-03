import os

from elasticsearch.client.ingest import IngestClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl import connections, Index
from elastic_transport import ConnectionTimeout

from model import ESWork
from logger import create_log

logger = create_log(__name__)


class ElasticsearchManager:
    OP_TYPE = "index"

    def __init__(self, index=None):
        self.index = index or os.environ.get("ELASTICSEARCH_INDEX", None)
        self.client = None

    def create_elastic_connection(
        self, scheme=None, host=None, port=None, user=None, pswd=None
    ):
        scheme = scheme or os.environ.get("ELASTICSEARCH_SCHEME", None)
        host = host or os.environ.get("ELASTICSEARCH_HOST", None)
        port = port or os.environ.get("ELASTICSEARCH_PORT", None)
        user = user or os.environ.get("ELASTICSEARCH_USER", None)
        pswd = pswd or os.environ.get("ELASTICSEARCH_PSWD", None)
        timeout = int(os.environ.get("ELASTICSEARCH_TIMEOUT", 5))

        creds = "{}:{}@".format(user, pswd) if user and pswd else ""

        # Allowing multple hosts for a ES connection
        multi_hosts = []

        if "," not in host:
            host = "{}://{}{}:{}".format(scheme, creds, host, port)

            multi_hosts.append(host)

        else:
            host_list = host.split(", ")

            for i in host_list:
                multi_hosts.append("{}://{}{}:{}".format(scheme, creds, i, port))

        connection_config = {
            "hosts": multi_hosts,
            "timeout": timeout,
            "retry_on_timeout": True,
            "max_retries": 3,
        }

        self.client = connections.create_connection(**connection_config)
        self.es = Elasticsearch(**connection_config)

    def create_elastic_search_ingest_pipeline(self):
        es_ingest_client = IngestClient(self.client)

        self.construct_language_pipeline(
            es_ingest_client,
            "title_language_detector",
            "Work title language detection",
            field="title.",
        )

        self.construct_language_pipeline(
            es_ingest_client,
            "alt_title_language_detector",
            "Work alt_title language detection",
            prefix="_ingest._value.",
        )

        self.construct_language_pipeline(
            es_ingest_client,
            "edition_title_language_detector",
            "Edition title language detection",
            prefix="_ingest._value.",
            field="title.",
        )

        self.construct_language_pipeline(
            es_ingest_client,
            "edition_sub_title_language_detector",
            "Edition subtitle language detection",
            prefix="_ingest._value.",
            field="sub_title.",
        )

        self.construct_language_pipeline(
            es_ingest_client,
            "subject_heading_language_detector",
            "Subject heading language detection",
            prefix="_ingest._value.",
            field="heading.",
        )

        es_ingest_client.put_pipeline(
            id="foreach_alt_title_language_detector",
            body={
                "description": "loop for parsing alt_titles",
                "processors": [
                    {
                        "foreach": {
                            "field": "alt_titles",
                            "processor": {
                                "pipeline": {
                                    "name": "alt_title_language_detector",
                                }
                            },
                        }
                    }
                ],
            },
        )

        es_ingest_client.put_pipeline(
            id="edition_language_detector",
            body={
                "description": "loop for parsing edition fields",
                "processors": [
                    {
                        "pipeline": {
                            "name": "edition_title_language_detector",
                            "ignore_failure": True,
                        }
                    },
                    {
                        "pipeline": {
                            "name": "edition_sub_title_language_detector",
                            "ignore_failure": True,
                        }
                    },
                ],
            },
        )

        es_ingest_client.put_pipeline(
            id="language_detector",
            body={
                "description": "Full language processing",
                "processors": [
                    {
                        "pipeline": {
                            "name": "title_language_detector",
                            "ignore_failure": True,
                        }
                    },
                    {
                        "pipeline": {
                            "name": "foreach_alt_title_language_detector",
                            "ignore_failure": True,
                        }
                    },
                    {
                        "foreach": {
                            "field": "editions",
                            "processor": {
                                "pipeline": {
                                    "name": "edition_language_detector",
                                    "ignore_failure": True,
                                }
                            },
                        }
                    },
                    {
                        "foreach": {
                            "field": "subjects",
                            "ignore_missing": True,
                            "processor": {
                                "pipeline": {
                                    "name": "subject_heading_language_detector",
                                    "ignore_failure": True,
                                }
                            },
                        }
                    },
                ],
            },
        )

    def create_elastic_search_index(self):
        es_index = Index(self.index)

        if es_index.exists() is False:
            ESWork.init(index=self.index)

    @staticmethod
    def construct_language_pipeline(client, id, description, prefix="", field=""):
        pipeline_body = {
            "description": description,
            "processors": [
                {
                    "inference": {
                        "model_id": "lang_ident_model_1",
                        "inference_config": {"classification": {"num_top_classes": 3}},
                        "field_map": {"{}{}default".format(prefix, field): "text"},
                        "target_field": "{}_ml.lang_ident".format(prefix),
                        "on_failure": [{"remove": {"field": "_ml"}}],
                    }
                },
                {
                    "rename": {
                        "field": "{}_ml.lang_ident.predicted_value".format(prefix),
                        "target_field": "{}{}language".format(prefix, field),
                    }
                },
                {
                    "set": {
                        "field": "tmp_lang",
                        "value": "{{{{{}{}language}}}}".format(prefix, field),
                        "override": True,
                    }
                },
                {
                    "set": {
                        "field": "tmp_score",
                        "value": "{{{{{}_ml.lang_ident.prediction_score}}}}".format(
                            prefix
                        ),
                        "override": True,
                    }
                },
                {
                    "script": {
                        "lang": "painless",
                        "source": 'ctx.supported = (["en", "de", "fr", "sp", "po", "nl", "it", "da", "ar", "zh", "el", "hi", "fa", "ja", "ru", "th"].contains(ctx.tmp_lang))',
                    }
                },
                {
                    "script": {
                        "lang": "painless",
                        "source": "ctx.threshold = Float.parseFloat(ctx.tmp_score) > 0.7",
                    }
                },
                {
                    "set": {
                        "if": "ctx.supported && ctx.threshold",
                        "field": "{}{}{{{{{}{}language}}}}".format(
                            prefix, field, prefix, field
                        ),
                        "value": "{{{{{}{}default}}}}".format(prefix, field),
                        "override": False,
                    }
                },
                {
                    "remove": {
                        "field": [
                            "{}_ml".format(prefix),
                            "tmp_lang",
                            "tmp_score",
                            "threshold",
                            "supported",
                        ]
                    }
                },
            ],
        }

        client.put_pipeline(id=id, body=pipeline_body)

    def save_work_records(self, works, attempt=1):
        try:
            save_res = bulk(
                self.es, self._upsert_generator(works), raise_on_error=False
            )

            logger.debug(save_res)

            if save_res[1]:
                for err in save_res[1]:
                    logger.error(
                        "Type: {}, Reason: {}".format(
                            err[self.OP_TYPE]["error"]["type"],
                            err[self.OP_TYPE]["error"]["reason"],
                        )
                    )
        except ConnectionTimeout as e:
            logger.error("ElasticSearch connection timeout")
            logger.debug(e)

            if attempt < 3:
                logger.info("Retrying batches")

                attempt += 1
                first_work_half, second_work_half = self._split_work_batch(works)

                self.save_work_records(first_work_half, attempt=attempt)
                self.save_work_records(second_work_half, attempt=attempt)
            else:
                logger.warning("Exceeded retry limit for batch {}".format(works))

    def _upsert_generator(self, works):
        for work in works:
            logger.debug("Saving {}".format(work))

            yield {
                "_op_type": self.OP_TYPE,
                "_index": self.index,
                "_id": work.uuid,
                "pipeline": "language_detector",
                "_source": work.to_dict(),
            }

    def delete_work_records(self, uuids):
        delete_res = bulk(self.es, self._delete_generator(uuids), raise_on_error=False)
        logger.debug(delete_res)

    def _delete_generator(self, uuids):
        for uuid in uuids:
            logger.debug("Deleting {}".format(uuid))

            yield {"_op_type": "delete", "_index": self.index, "_id": uuid}

    @staticmethod
    def _split_work_batch(works):
        work_count = len(works)
        pivot_point = int(work_count / 2)

        return works[:pivot_point], works[pivot_point:]
