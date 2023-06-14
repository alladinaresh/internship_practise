from pymongo import MongoClient
from faker import Faker
import time
#Importing Faker for creating fake data
faker = Faker()
connection_string = "mongodb://localhost:27017"

#Making connection between mongodb with python
def create_connection_and_return_client(connection_string):
    try:
        client = MongoClient(connection_string)
        print("Connecting Successfully...")
        return client
    except Exception as Err:
        return Err


#Creating a database
def creating_database(client):
    try:
        database = client["sample_db"]
        print("Database created Successfully....")
        return database
    except Exception as Err:
        return Err


#Creating a collection in a database
def creating_collection(database):
    try:
        collection = database.create_collection("workers_details")
        print("Collection created successfully......")
        return collection
    except Exception as Err:
        return Err


#Inserting data(documents) into the collection
def inserting_data_in_database(collection_name):
# Generate and insert the data
    try:
        for i in range(1, 10000):
            name = faker.name()
            first_name = faker.first_name()
            last_name = faker.last_name()
            address = faker.address()
            email = faker.email()
            phone_number = faker.phone_number()
            sentence = faker.sentence()

            data = {
                'name': name,
                'first_name': first_name,
                'last_name': last_name,
                'address': address,
                'email': email,
                'phone_number': phone_number,
                'sentence': sentence,
            }
            collection_name.insert_one(data)
            if i % 10000 == 0:
                print(f"Inserted {i} records.")
    except Exception as Err:
        print(f"Error is {Err}")

#Creating indexing to a texted field and searching some data
def creating_indexing_and_searching_data(collection):
    try:
        collection.create_index([("sentence","text")])
        print("Indexing Created Successfully")
        value = collection.find({"$text":{"$search":"\"doctor political\"","$caseSensitive":True}})
        for i in value:
            print(i)
    except Exception as Err:
        print(Err)

#compound indexing(Before)
def data_search(collection_name):
    try:
        value = collection_name.find({"name":{'$eq':'Christoper'}})
        for i in value:
            print(i)
    except Exception as Err:
        print(Err)


# After
def creating_text_index(collection_name):
    try:
        collection_name.create_index({"name":1,"email":1})
        print("Index Created Successfully")
    except Exception as Err:
        print(Err)

def data_search_after_indexing(collection_name):
    try:
        value = collection_name.find({"name":{'$eq':'Christoper'}})
        for i in value:
            print(i)
    except Exception as Err:
        print(Err)



client = create_connection_and_return_client(connection_string)
database = creating_database(client)
collection_name = creating_collection(database)
print(collection_name)
inserting_data_in_database(collection_name)
creating_indexing_and_searching_data(collection_name)
creating_text_index(collection_name)
start_time = time.time()
data_search(collection_name)
overall_time = time.time() - start_time
print(f' Before index Execution Time is: {overall_time} seconds')