from PIL import Image
from pymongo import MongoClient
import urllib, cStringIO
from datetime import datetime
from bson.objectid import ObjectId
#URL = "https://s3-us-west-2.amazonaws.com/cdn.simplyrets.com/properties/abor/photos/22262080/1.jpg"  2102 ) mlsid 8087528 8087528
#2251
client = MongoClient('mongodb://localhost:27017')
db = client.max_api
def checkImages (min_data, max_data):
	list = db.listings
	log = open('result.txt','w')
	log2 = open('correct.txt','w')
	querylist = list.find({'primary_photo':{'$regex':'.*jpg*'}})
	c = 0
	data = []
	print('Preparing data to parser please wait')
	for i in querylist:
		data.append({'mlsId':i['mlsId'],'primary_photo':i['primary_photo'], 'modified':i['modified']})
	total = len(data[min_data:max_data])
	for item in data[min_data:max_data]:
		mlsid = item['mlsId']
		photo = item['primary_photo']
		porcentaje = int((float(c) / float(total)) * 100)
		print(total)
		print(str(int(c+min_data)) + ' ) mlsid '+ mlsid +' '+ str(porcentaje) + '% Checking: ' + str(photo))
		try:
			file = cStringIO.StringIO(urllib.urlopen(str(photo)).read())
			img = Image.open(file)
			print ('Correct !' )
			log2.write(str(int(c+min_data)) + ') Correct: MLSID =' + mlsid + 'date'+ str(item['modified']) +'\n')
		except IOError:
			print ('======================================> ERROR !' ) 
			log.write(str(int(c+min_data)) + ') Error: MLSID =' + mlsid + ' date'+ str(item['modified']) +'\n')
		c = 1 + c
	print('=========> Finish <=============')
	log.close()
	log2.close()
	client.close()
def checkDuplicatesAndDelete(min_data, max_data):
	list = db.listings
	log = open('duplicates.txt','w')
	querylist = list.find({'listingId':{'$regex':'\d'}})
	data = []
	print('Preparing data to parser, check all DB please wait...')
	for i in querylist:
		data.append({'mlsId':i['mlsId'], 'created_at': i['created_at'], 'listingId': i['listingId']})
	total = float(len(data[min_data:max_data]))
	avanced = float(0)
	for item in data[min_data:max_data]:
		listing_Id = item['listingId']
		#listing_Id = item[]
		porcentaje = ((avanced) / (total) * 100)
		print("Cheking list  "+ str(avanced) + " Advanced " + str(porcentaje) + "%")
		result = list.find({"listingId": listing_Id})
		avanced = avanced + 1
		if (result.count() > 1):
			print("Find duplicate listing, Check which the newest")
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
				print("* " + str(ids["_id"]) + " MlsId = "+ ids["mlsId"] + " listingId = " + ids["listingId"] + " created = " + str(ids["created_at"]) + " full = " + full)
				log.write("* Remove " + str(ids["_id"]) + " MlsId = "+ ids["mlsId"] + " listingId = " + ids["listingId"] + " created = " + str(ids["created_at"]) + " full = " + full + '\n')
				list.remove({"_id" : ids["_id"]})
	log.close()
	client.close()
checkDuplicatesAndDelete(0, 70000)
