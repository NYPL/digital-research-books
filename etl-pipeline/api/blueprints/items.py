from flask import Blueprint

items_blueprint = Blueprint("items", __name__, url_prefix="/items")

# Import your /items routes here
from . import item_search
from . import item_read
