from flask import Blueprint


items_blueprint = Blueprint("items", __name__, url_prefix="/items")

# Import /items routes
from .item_read import item_read
