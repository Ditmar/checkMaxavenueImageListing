from PIL import Image
from pymongo import MongoClient
import urllib, cStringIO
from datetime import datetime
from bson.objectid import ObjectId
import subprocess
import os
import logging
import time
import re
from daemon import runner

class CleanMongDb():
	def __init__(self, logger):
		self.logger = logger
		self.client = MongoClient('mongodb://localhost:27017')
		self.db = self.client.max_api
	def checkImages (self, min_data, max_data):
		list = self.db.listings
		log = open('result.txt','w')
		log2 = open('correct.txt','w')
		querylist = list.find({'primary_photo':{'$regex':'.*jpg*'}})
		c = 0
		data = []
		for i in querylist:
			data.append({'mlsId':i['mlsId'],'primary_photo':i['primary_photo'], 'modified':i['modified']})
		total = len(data[min_data:max_data])
		for item in data[min_data:max_data]:
			mlsid = item['mlsId']
			photo = item['primary_photo']
			porcentaje = int((float(c) / float(total)) * 100)
			print porcentaje + "%"
			try:
				file = cStringIO.StringIO(urllib.urlopen(str(photo)).read())
				img = Image.open(file)
				print ('Correct !' )
				self.logger.info(str(int(c+min_data)) + ') Correct: MLSID =' + mlsid + 'date'+ str(item['modified']) +'\n')
			except IOError:
				print ('======================================> ERROR !' ) 
				self.logger.info(str(int(c+min_data)) + ') Error: MLSID =' + mlsid + ' date'+ str(item['modified']) +'\n')
			c = 1 + c
		print('=========> Finish <=============')
		self.client.close()
	def checkDuplicatesAndDelete(self, min_data, max_data):
		self.logger.info("Start To Process from  %s to %s "%(min_data, max_data))
		list = self.db.listings
		querylist = list.find({'listingId':{'$regex':'\d'}})[min_data:max_data]
		data = []
		for i in querylist:
			data.append({'mlsId':i['mlsId'], 'created_at': i['created_at'], 'listingId': i['listingId']})
		total = float(len(data))
		avanced = float(0)
		for item in data:
			listing_Id = item['listingId']
			#listing_Id = item[]
			result = list.find({"listingId": listing_Id})
			porcentaje = ((avanced) / (total) * 100)
			avanced = avanced + 1
			print("Cheking list  "+ str(avanced) + " Advanced " + str(porcentaje) + "%")
			if (result.count() > 1):
				self.logger.info("Find duplicate listing, Check which the newest in  " + str(avanced))
				id_arrays_eliminates = []
				for e in result:
					id_arrays_eliminates.append(e)
				the_last = id_arrays_eliminates[0]
				for item_last in id_arrays_eliminates:
					if (the_last["created_at"] < item_last["created_at"]):
						the_last = item_last
				id_arrays_eliminates.remove(the_last)
					#datetime_object = datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S')
				for ids in id_arrays_eliminates:
					try:
						full = ids["full"]
					except KeyError:
						full = "none"
					self.logger.info("* Remove " + str(ids["_id"]) + " MlsId = "+ ids["mlsId"] + " listingId = " + ids["listingId"] + " created = " + str(ids["created_at"]) + " full = " + full + '\n')
					list.remove({"_id" : ids["_id"]})
		log.close()
		del data[:]
		self.client.close()
class App():
   def __init__(self):
      self.stdin_path      = '/dev/null'
      self.stdout_path     = '/dev/tty'
      self.stderr_path     = '/dev/tty'
      self.pidfile_path    =  '/var/run/test.pid'
      self.pidfile_timeout = 5
   def run(self):
      process = CleanMongDb(logger)
      process.checkDuplicatesAndDelete(0,150000)
      while True:
         time.sleep(259200)
if __name__ == '__main__':
   app = App()
   logger = logging.getLogger("testlog")
   logger.setLevel(logging.INFO)
   formatter = logging.Formatter("%(asctime)s - %(name)s - %(message)s")
   handler = logging.FileHandler("./log.log")
   handler.setFormatter(formatter)
   logger.addHandler(handler)
   serv = runner.DaemonRunner(app)
   serv.daemon_context.files_preserve=[handler.stream]
   serv.do_action()