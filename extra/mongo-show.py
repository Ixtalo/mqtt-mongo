
from pymongo import MongoClient
from pprint import pprint

MONGO_DB = 'mqtt'   ## database name



mongo = MongoClient()
db = mongo[MONGO_DB].messages  ## <MONGO_DB>.messages  (messages collection)

for msg in db.find():
    pprint(msg)