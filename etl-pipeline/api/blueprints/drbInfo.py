from flask import Blueprint, url_for, redirect

from logger import create_log

logger = create_log(__name__)

info = Blueprint("info", __name__, url_prefix="/")


@info.route("/", methods=["GET"])
def api_info():
    logger.debug("Redirecting to API docs")

    return redirect(url_for("flasgger.apidocs"))
