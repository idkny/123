from pymongo import MongoClient, errors, ASCENDING
 


class MongoConnect:
    def __init__(self, client_name, db_name, collection_name):
        self.client_name = client_name
        self.client = MongoClient(self.client_name)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.db_name = db_name
        self.collection_name = collection_name

    def connect(self):

        try:
            if self.client.server_info():
                return True

        except (
            errors.ServerSelectionTimeoutError,
            errors.ConnectionFailure,
        ) as err:
            print(err)

    def insert_document(self, insert_document):

        try:
            self.collection.insert_one(insert_document)
            return True
        except errors.WriteConcernError as wce:
            print(wce)
            return False
        except errors.WriteError as we:
            print(we)
            return False

    def prevent_duplicate(self):
        self.collection.create_index([("created_at", ASCENDING)], unique=True)
   
    def get_missing_docs(self, time_stamp):

        data = self.collection.find({"created_at": {"$gt": time_stamp}}).sort(
            "created_at", 1
        )
        event = [e for e in list(data)]
        return event

    def get_all_docs(self):

        data = self.collection.find()
        events = list(data)
        return events

    def count_docs(self):

        num_of_docs = self.collection.count_documents({})
        print(f"{num_of_docs} docs in coll")
        return num_of_docs
