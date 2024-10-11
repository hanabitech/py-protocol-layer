import json
from pymongo import MongoClient, GEOSPHERE, TEXT, ASCENDING

from main.config import get_config_by_name
from main.logger.custom_logging import log

mongo_client = None
mongo_db = None


class JsonObject:
    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__)


def initialize_before_calls(app):
    from flask import request, g

    @app.before_request
    def set_page(page=1):
        page = int(request.args.get('page', 1))
        g.page = page


def init_database():
    global mongo_client, mongo_db
    if mongo_client is not None and mongo_db is not None:
        return
    database_host = get_config_by_name('MONGO_DATABASE_HOST')
    database_port = get_config_by_name('MONGO_DATABASE_PORT')
    database_name = get_config_by_name('MONGO_DATABASE_NAME')
    mongo_client = MongoClient(database_host, database_port, maxPoolSize=10)
    mongo_db = mongo_client[database_name]
    log(f"Connection to mongodb://{database_host}:{database_port} is successful!")
    create_all_indexes()
    log(f"Created indexes if not already present!")


def create_all_indexes():
    ensure_index(get_mongo_collection("on_search_items"), "id", TEXT, "id_index")
    ensure_index(get_mongo_collection("location"), "gps", GEOSPHERE, "gps_2dsphere")

def ensure_index(collection, field, index_type, index_name):
    existing_indexes = collection.index_information()
    if index_name not in existing_indexes:
        collection.create_index([(field, index_type)], name=index_name)
        print(f"Index {index_name} created on {field} with type {index_type}.")
    else:
        print(f"Index {index_name} already exists.")

def get_mongo_collection(collection_name):
    # check if database is initialized
    global mongo_client, mongo_db
    if mongo_client is None or mongo_db is None:
        init_database()
    return mongo_db[collection_name]
