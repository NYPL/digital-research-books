from elastic_transport import ConnectionTimeout
import pytest

from managers import ElasticsearchManager
from model import ESWork


class TestElasticsearchManager:
    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch.dict(
            "os.environ",
            {
                "ELASTICSEARCH_INDEX": "testES",
                "ELASTICSEARCH_HOST": "host",
                "ELASTICSEARCH_PORT": "port",
                "ELASTICSEARCH_TIMEOUT": "1000",
                "ELASTICSEARCH_SCHEME": "http",
                "ELASTICSEARCH_USER": "testUser",
                "ELASTICSEARCH_PSWD": "testPswd",
            },
        )

        return ElasticsearchManager()

    @pytest.fixture
    def test_instance_2(self, mocker):
        mocker.patch.dict(
            "os.environ",
            {
                "ELASTICSEARCH_INDEX": "testES",
                "ELASTICSEARCH_HOST": "host1, host2, host3",
                "ELASTICSEARCH_PORT": "port",
                "ELASTICSEARCH_TIMEOUT": "1000",
                "ELASTICSEARCH_SCHEME": "http",
                "ELASTICSEARCH_USER": "testUser",
                "ELASTICSEARCH_PSWD": "testPswd",
            },
        )

        return ElasticsearchManager()

    def test_initializer(self, test_instance):
        assert test_instance.index == "testES"

    def test_create_elastic_connection_success(self, test_instance, mocker):
        mock_connection = mocker.patch("managers.elasticsearch.connections")
        mock_client = mocker.patch("managers.elasticsearch.Elasticsearch")

        test_instance.create_elastic_connection()

        mock_connection.create_connection.assert_called_once_with(
            hosts=["http://testUser:testPswd@host:port"],
            timeout=1000,
            retry_on_timeout=True,
            max_retries=3,
        )

        mock_client.assert_called_once_with(
            hosts=["http://testUser:testPswd@host:port"],
            timeout=1000,
            retry_on_timeout=True,
            max_retries=3,
        )

    def test_create_elastic_connection_multi_host_success(
        self, test_instance_2, mocker
    ):
        mock_connection = mocker.patch("managers.elasticsearch.connections")
        mock_client = mocker.patch("managers.elasticsearch.Elasticsearch")

        test_instance_2.create_elastic_connection()

        mock_connection.create_connection.assert_called_once_with(
            hosts=[
                "http://testUser:testPswd@host1:port",
                "http://testUser:testPswd@host2:port",
                "http://testUser:testPswd@host3:port",
            ],
            timeout=1000,
            retry_on_timeout=True,
            max_retries=3,
        )

        mock_client.assert_called_once_with(
            hosts=[
                "http://testUser:testPswd@host1:port",
                "http://testUser:testPswd@host2:port",
                "http://testUser:testPswd@host3:port",
            ],
            timeout=1000,
            retry_on_timeout=True,
            max_retries=3,
        )

    def test_create_eLastic_search_index_execute(self, test_instance, mocker):
        test_index_con = mocker.patch("managers.elasticsearch.Index")
        test_index = mocker.MagicMock()
        test_index_con.return_value = test_index
        test_index.exists.return_value = False

        mock_init = mocker.patch.object(ESWork, "init")

        test_instance.create_elastic_search_index()

        test_index_con.assert_called_once_with("testES")
        test_index.exists.assert_called_once()
        mock_init.assert_called_once()

    def test_create_elastic_search_index_skip(self, test_instance, mocker):
        test_index_con = mocker.patch("managers.elasticsearch.Index")
        test_index = mocker.MagicMock()
        test_index_con.return_value = test_index
        test_index.exists.return_value = True

        mock_init = mocker.patch.object(ESWork, "init")

        test_instance.create_elastic_search_index()

        test_index_con.assert_called_once_with("testES")
        test_index.exists.assert_called_once()
        mock_init.assert_not_called()

    def test_delete_work_records(self, test_instance, mocker):
        test_instance.es = "mock_client"
        mock_bulk = mocker.patch("managers.elasticsearch.bulk")
        mock_gen = mocker.patch.object(ElasticsearchManager, "_delete_generator")
        mock_gen.return_value = "generator"

        test_instance.delete_work_records(["uuid1", "uuid2", "uuid3"])

        mock_gen.assert_called_once_with(["uuid1", "uuid2", "uuid3"])
        mock_bulk.assert_called_once_with(
            "mock_client", "generator", raise_on_error=False
        )

    def test_delete_generator(self, test_instance):
        delete_stmts = [out for out in test_instance._delete_generator([1, 2, 3])]

        assert delete_stmts == [
            {"_op_type": "delete", "_index": "testES", "_id": 1},
            {"_op_type": "delete", "_index": "testES", "_id": 2},
            {"_op_type": "delete", "_index": "testES", "_id": 3},
        ]

    def test_save_work_records_success(self, test_instance, mocker):
        test_instance.es = "mock_client"
        mock_bulk = mocker.patch("managers.elasticsearch.bulk")
        mock_gen = mocker.patch.object(ElasticsearchManager, "_upsert_generator")
        mock_gen.return_value = "generator"

        mock_bulk.return_value = (
            2,
            [{"index": {"error": {"type": "testing", "reason": "testing"}}}],
        )

        test_instance.save_work_records(["work1", "work2", "work3"])

        mock_gen.assert_called_once_with(["work1", "work2", "work3"])
        mock_bulk.assert_called_once_with(
            "mock_client", "generator", raise_on_error=False
        )

    def test_save_work_records_retry_failure(self, test_instance, mocker):
        test_instance.es = "mock_client"
        mock_bulk = mocker.patch("managers.elasticsearch.bulk")
        mock_gen = mocker.patch.object(ElasticsearchManager, "_upsert_generator")
        mock_gen.return_value = "generator"

        mock_bulk.side_effect = ConnectionTimeout("testing")

        work_array = [f"work{i}" for i in range(6)]

        test_instance.save_work_records(work_array)

        mock_gen.assert_has_calls(
            [
                mocker.call(work_array),
                mocker.call(work_array[:3]),
                mocker.call(work_array[:1]),
                mocker.call(work_array[1:3]),
                mocker.call(work_array[3:]),
                mocker.call(work_array[3:4]),
                mocker.call(work_array[4:]),
            ]
        )
        mock_bulk.assert_has_calls(
            [mocker.call("mock_client", "generator", raise_on_error=False)] * 7
        )

    def test_save_work_records_retry_success(self, test_instance, mocker):
        test_instance.es = "mock_client"
        mock_bulk = mocker.patch("managers.elasticsearch.bulk")
        mock_gen = mocker.patch.object(ElasticsearchManager, "_upsert_generator")
        mock_gen.return_value = "generator"

        mock_bulk.side_effect = [
            ConnectionTimeout("testing"),
            ("testing", False),
            ("testing", False),
        ]

        work_array = [f"work{i}" for i in range(5)]

        test_instance.save_work_records(work_array)

        mock_gen.assert_has_calls(
            [
                mocker.call(work_array),
                mocker.call(work_array[:2]),
                mocker.call(work_array[2:]),
            ]
        )

        mock_bulk.assert_has_calls(
            [mocker.call("mock_client", "generator", raise_on_error=False)] * 3
        )

    def test_save_work_records_retry_partial_failure(self, test_instance, mocker):
        test_instance.es = "mock_client"
        mock_bulk = mocker.patch("managers.elasticsearch.bulk")
        mock_gen = mocker.patch.object(ElasticsearchManager, "_upsert_generator")
        mock_gen.return_value = "generator"

        mock_bulk.side_effect = [
            ConnectionTimeout("testing"),  # 0, 1, 2, 3, 4
            ("testing", False),  # 0, 1
            ConnectionTimeout("testing"),  # 2, 3, 4
            ConnectionTimeout("testing"),  # 2
            ("testing", False),  # 3, 4
        ]

        work_array = [f"work{i}" for i in range(5)]

        test_instance.save_work_records(work_array)

        mock_gen.assert_has_calls(
            [
                mocker.call(work_array),
                mocker.call(work_array[:2]),
                mocker.call(work_array[2:]),
                mocker.call(work_array[2:3]),
                mocker.call(work_array[3:]),
            ]
        )

        mock_bulk.assert_has_calls(
            [mocker.call("mock_client", "generator", raise_on_error=False)] * 5
        )

    def test_upsert_generator(self, test_instance, mocker):
        mock_work = mocker.MagicMock(uuid=1)
        mock_work.to_dict.return_value = "mock_work"
        upsert_stmts = [out for out in test_instance._upsert_generator([mock_work])]

        assert upsert_stmts == [
            {
                "_op_type": "index",
                "_index": "testES",
                "_id": 1,
                "_source": "mock_work",
                "pipeline": "language_detector",
            }
        ]

    def test_create_elastic_search_ingest_pipeline(self, test_instance, mocker):
        mock_ingest = mocker.MagicMock()
        mock_client = mocker.patch("managers.elasticsearch.IngestClient")
        mock_client.return_value = mock_ingest

        mock_constructor = mocker.patch.object(
            ElasticsearchManager, "construct_language_pipeline"
        )

        test_instance.create_elastic_search_ingest_pipeline()

        mock_client.assert_called_once_with(None)

        mock_constructor.assert_has_calls(
            [
                mocker.call(
                    mock_ingest,
                    "title_language_detector",
                    "Work title language detection",
                    field="title.",
                ),
                mocker.call(
                    mock_ingest,
                    "alt_title_language_detector",
                    "Work alt_title language detection",
                    prefix="_ingest._value.",
                ),
                mocker.call(
                    mock_ingest,
                    "edition_title_language_detector",
                    "Edition title language detection",
                    prefix="_ingest._value.",
                    field="title.",
                ),
                mocker.call(
                    mock_ingest,
                    "edition_sub_title_language_detector",
                    "Edition subtitle language detection",
                    prefix="_ingest._value.",
                    field="sub_title.",
                ),
                mocker.call(
                    mock_ingest,
                    "subject_heading_language_detector",
                    "Subject heading language detection",
                    prefix="_ingest._value.",
                    field="heading.",
                ),
            ]
        )

        mock_ingest.put_pipeline.assert_has_calls(
            [
                mocker.call(
                    id="foreach_alt_title_language_detector",
                    body={
                        "description": "loop for parsing alt_titles",
                        "processors": [
                            {
                                "foreach": {
                                    "field": "alt_titles",
                                    "processor": {
                                        "pipeline": {
                                            "name": "alt_title_language_detector"
                                        }
                                    },
                                }
                            }
                        ],
                    },
                ),
                mocker.call(
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
                ),
                mocker.call(
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
                ),
            ]
        )

    def test_construct_language_pipeline(self, mocker):
        mock_client = mocker.MagicMock()

        ElasticsearchManager.construct_language_pipeline(
            mock_client, "test_pipeline", "Testing", "prefix.", "field."
        )

        test_body = {
            "description": "Testing",
            "processors": [
                {
                    "inference": {
                        "model_id": "lang_ident_model_1",
                        "inference_config": {"classification": {"num_top_classes": 3}},
                        "field_map": {"prefix.field.default": "text"},
                        "target_field": "prefix._ml.lang_ident",
                        "on_failure": [{"remove": {"field": "_ml"}}],
                    },
                },
                {
                    "rename": {
                        "field": "prefix._ml.lang_ident.predicted_value",
                        "target_field": "prefix.field.language",
                    }
                },
                {
                    "set": {
                        "field": "tmp_lang",
                        "value": "{{prefix.field.language}}",
                        "override": True,
                    }
                },
                {
                    "set": {
                        "field": "tmp_score",
                        "value": "{{prefix._ml.lang_ident.prediction_score}}",
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
                        "field": "prefix.field.{{prefix.field.language}}",
                        "value": "{{prefix.field.default}}",
                        "override": False,
                    }
                },
                {
                    "remove": {
                        "field": [
                            "prefix._ml",
                            "tmp_lang",
                            "tmp_score",
                            "threshold",
                            "supported",
                        ]
                    }
                },
            ],
        }

        mock_client.put_pipeline.assert_called_once_with(
            id="test_pipeline", body=test_body
        )

    def test_split_work_batch_even(self):
        first_half, second_half = ElasticsearchManager._split_work_batch(
            [i for i in range(10)]
        )

        assert first_half == [i for i in range(5)]
        assert second_half == [i for i in range(5, 10)]

    def test_split_work_batch_odd(self):
        first_half, second_half = ElasticsearchManager._split_work_batch(
            [i for i in range(13)]
        )

        assert first_half == [i for i in range(6)]
        assert second_half == [i for i in range(6, 13)]
