from flask import Flask
from flasgger import Swagger
import json
import pytest

from api.blueprints.drbInfo import info


class TestInfoBlueprint:
    def test_apiInfo(self, mocker):
        mocker.patch.dict("os.environ", {"ENVIRONMENT": "test"})

        flaskApp = Flask("test")
        flaskApp.testing = True
        Swagger(flaskApp, template=json.load(open("swagger.v4.json", "r")))
        flaskApp.register_blueprint(info)

        testClient = flaskApp.test_client()
        apiResp = testClient.get("/", follow_redirects=True)

        assert apiResp.status_code == 200
        assert apiResp.mimetype == "text/html"
