from flask import Blueprint, url_for, redirect

from logger import create_log

logger = create_log(__name__)

info = Blueprint("info", __name__, url_prefix="/")


@info.route("/", methods=["GET"])
def api_info():
    """redirect "/" to "/apidocs" (flasgger auto-generated Swagger API docs)"""
    logger.debug("Redirecting to API docs")

    # NOTE: "flasgger.apidocs" refers to the 'apidocs' view function defined in \
    # the "flasgger" blueprint that is implicitly created by `Swagger(app)`
    return redirect(url_for("flasgger.apidocs"))
