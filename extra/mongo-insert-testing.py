

from pymongo import MongoClient

MONGO_DB = 'test'   ## database name



mongo = MongoClient()
db_messages = mongo[MONGO_DB].messages  ## <MONGO_DB>.messages  (messages collection)

data = {
    'desc' : 'only for testing',
    'value' : 123.123,
    'value_s' : '456.45',
    'value_b' : b'321.321',
}


print(db_messages.insert_one(data).inserted_id)