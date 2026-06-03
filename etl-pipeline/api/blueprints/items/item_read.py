import base64

# shared code imports
from managers import DBManager
from model import Item, Record

# api imports
from ...utils import APIUtils
from . import items_blueprint

# TODO: since the unpack functions are used in multiple places they should be \
# moved to a shared code location.

RESPONSE_TYPE = "itemRead"


@items_blueprint.route("/<item_id>/read/<page_id>", methods=["GET"])
def item_read(item_id, page_id):
    with DBManager() as db_manager:
        record_source_id = (
            db_manager.session.query(Record.source_id)
            .join(Item, Item.record_id == Record.id)
            .filter(Item.id == int(item_id))
            .scalar()
        )
        barcode = record_source_id.split("|")[0]

    return APIUtils.formatResponseObject(
        200,
        RESPONSE_TYPE,
        {
            "barcode": barcode,
        },
    )
