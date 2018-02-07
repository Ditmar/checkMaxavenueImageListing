import re
from pymongo import MongoClient

def update():
	client = MongoClient('mongodb://localhost:27017')
	db = client.max_api
	list = db.listings
	file = open("total.txt","r")
	for line in file:
		pattern = re.compile("=[0-9]{5,8}")
		mlsId = pattern.findall(line)[0][1:]
		r = list.update({'mlsId': mlsId}, {'$set': {'active':False}})
		print r
update()