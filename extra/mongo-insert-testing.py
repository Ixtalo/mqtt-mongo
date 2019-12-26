

from pymongo import MongoClient

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'test'   ## database name



mongo = MongoClient(host=MONGO_HOST, port=MONGO_PORT, username='root', password='example')

db_messages = mongo.get_database(MONGO_DB).messages

data = {
    'desc' : 'only for testing',
    'value' : 123.123,
    'value_s' : '456.45',
    'value_b' : b'321.321',
}


print(db_messages.insert_one(data).inserted_id)
