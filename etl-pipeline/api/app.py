from elasticsearch7.exceptions import RequestError
from flasgger import Swagger
from flask import Flask
from flask_cors import CORS
import json
import os
from sqlalchemy.exc import DataError
from waitress import serve

from logger import create_log
from .blueprints import (
    chat_blueprint,
    search,
    work,
    works,
    info,
    edition,
    editions,
    items_blueprint,
    utils,
    link,
    links,
    opds,
    collection,
    collections,
    citation,
    fulfill,
)
from .utils import APIUtils
from .db import Session

logger = create_log(__name__)

BLUEPRINTS = [
    chat_blueprint,
    search,
    work,
    works,
    info,
    edition,
    editions,
    items_blueprint,
    utils,
    link,
    links,
    opds,
    collection,
    collections,
    citation,
    fulfill,
]


class API:
    def __init__(self, db_engine, redis_client):
        self.app = Flask(__name__)

        CORS(self.app)
        Swagger(self.app, template=json.load(open("swagger.v4.json", "r")))

        # TODO: rename "SQL_ENGINE"
        self.app.config["DB_CLIENT"] = db_engine
        self.app.config["REDIS_CLIENT"] = redis_client
        self.app.config["READER_VERSION"] = os.environ["READER_VERSION"]

        self._register_blueprints()

        # Close the ORM session at end of request to create thread-safe/thread-scoped DB ORM sessions
        # see: https://flask.palletsprojects.com/en/stable/appcontext/#events-and-signals
        @self.app.teardown_appcontext
        def shutdown_db_session(exception=None):
            Session.remove()

    def _register_blueprints(self):
        for blueprint in BLUEPRINTS:
            self.app.register_blueprint(blueprint)

    def run(self):
        if os.environ.get("STAGE") == "development":
            self.app.config["ENV"] = "development"
            self.app.config["DEBUG"] = True
            self.app.run(host=os.environ.get("API_HOST"), port=5050)
        else:
            serve(self.app, host="0.0.0.0", port=80)

    def create_error_responses(self):
        @self.app.errorhandler(404)
        def page_not_found(error):
            logger.exception("Page not found")
            return APIUtils.formatResponseObject(
                404, "pageNotFound", {"message": "Requested page does not exist"}
            )

        # TODO: we should be handling ElasticSearch and SQLAlchemy errors upstream
        @self.app.errorhandler(DataError)
        def sql_alchemy_data_error(error):
            logger.exception("Internal SQLAlchemy error")
            return APIUtils.formatResponseObject(
                500, "dataError", {"message": "Encountered fatal database error"}
            )

        @self.app.errorhandler(RequestError)
        def es_request_error(error):
            logger.exception("Invalid parameter passed to ElasticSearch")
            return APIUtils.formatResponseObject(
                400,
                "requestError",
                {"message": error.info["error"]["root_cause"][0]["reason"]},
            )
