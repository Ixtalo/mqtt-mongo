
from pprint import pprint

from pymongo import MongoClient

MONGO_DB = 'mqtt'   ## database name



mongo = MongoClient()
db_messages = mongo[MONGO_DB].messages  ## <MONGO_DB>.messages  (messages collection)

for msg in db_messages.find():
    pprint(msg)