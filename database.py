import pymongo
import sys
from config import MONGO_URI, DATABASE_NAME

def makeNewConnection():

    try:
        client = pymongo.MongoClient(MONGO_URI)
        
        client.admin.command('ping')

        print("Successfully connected to MongoDB")

        db = client[DATABASE_NAME]

        return db
    
    except Exception as e:
        print("Error connecting to MongoDB: " + {e})
        sys.exit("Could not connect to the database.")

db = makeNewConnection()

dim_patient = db["patient"]
dim_station = db["station"]
dim_image = db["image"]
dim_protocol = db["protocol"]
dim_date = db["date"]
fact_table_study = db["study"]

