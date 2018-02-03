from PIL import Image
from pymongo import MongoClient
import urllib, cStringIO
URL = "https://s3-us-west-2.amazonaws.com/cdn.simplyrets.com/properties/abor/photos/22262080/1.jpg"
file = cStringIO.StringIO(urllib.urlopen(URL).read())
client = MongoClient("mongodb://localhost:27017")
db = client.max_api
#primary_photo
try:
    img = Image.open(file)
    print ("Correct")
except IOError:
    print ("ERROR")
