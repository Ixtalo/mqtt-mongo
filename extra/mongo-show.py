
from pprint import pprint
from pymongo import MongoClient

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'mqtt'   ## database name



mongo = MongoClient(host=MONGO_HOST, port=MONGO_PORT, username='root', password='example')

pprint(mongo.server_info())
pprint(list(mongo.list_databases()))
pprint(mongo.list_database_names())


db_messages = mongo.get_database(MONGO_DB).messages
for msg in db_messages.find():
    pprint(msg)
