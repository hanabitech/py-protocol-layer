import json

from bson.objectid import ObjectId
from pika.exceptions import AMQPConnectionError
from retry import retry

from main.config import get_config_by_name
from main.logger.custom_logging import log, log_error
from main.models import init_database, get_mongo_collection
from main.models.catalog import SearchType
from main.repository import mongo
from main.service.search import add_search_catalogues, add_incremental_search_catalogues, update_on_search_dump_status
from main.utils.rabbitmq_utils import create_channel, declare_queue, consume_message, open_connection


def consume_fn(message_string):
    try:
        payload = json.loads(message_string)
        log(f"Got the payload {payload}!")

        doc_id = payload["doc_id"]
        collection = get_mongo_collection('on_search_dump')
        on_search_payload = mongo.collection_find_one(collection, {"_id": ObjectId(doc_id)})
        if on_search_payload and on_search_payload['context']['bpp_id'] not in ['ref-app-seller-staging-v2.ondc.org']:
            on_search_payload.pop("id", None)
            if payload["request_type"] == SearchType.FULL.value:
                update_on_search_dump_status(doc_id, "IN-PROGRESS")
                add_search_catalogues(on_search_payload)
                update_on_search_dump_status(doc_id, "FINISHED")
            elif payload["request_type"] == SearchType.INC.value:
                update_on_search_dump_status(doc_id, "IN-PROGRESS")
                add_incremental_search_catalogues(on_search_payload)
                update_on_search_dump_status(doc_id, "FINISHED")
    except Exception as e:
        log_error(f"Something went wrong with consume function - {e}!")


@retry(AMQPConnectionError, delay=5, jitter=(1, 3))
def run_consumer():
    init_database()
    queue_name = get_config_by_name('RABBITMQ_QUEUE_NAME')
    connection = open_connection()
    channel = create_channel(connection)
    declare_queue(channel, queue_name)
    consume_message(connection, channel, queue_name=queue_name,
                    consume_fn=consume_fn)


if __name__ == "__main__":
    run_consumer()
