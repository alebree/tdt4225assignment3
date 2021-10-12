from pprint import pprint
from DbConnector import DbConnector


# get relevant users with activity data from textfile and return list with string user IDs
def relevant_users():
    with open('labeled_ids.txt') as file:
        lables = file.readlines()
        lables = [line.rstrip() for line in lables]
    return lables


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll_users(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def insert_documents_users(self, collection_name):
        labeled_user = relevant_users()
        for user in range(182):
            user = f"{user:03d}"
            if user in labeled_user:
                docs = [
                    {
                        "_id": user,
                        "has_labels": 1,
                    }
                ]
                collection = self.db[collection_name]
                collection.insert_many(docs)
            else:
                docs = [
                    {
                        "_id": user,
                        "has_labels": 0,
                    }
                ]
                collection = self.db[collection_name]
                collection.insert_many(docs)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            print(doc)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)


def main():
    program = None
    try:
        program = ExampleProgram()
        program.create_coll_users(collection_name="User")
        program.show_coll()
        program.insert_documents_users(collection_name="User")
        program.fetch_documents(collection_name="User")
        # program.drop_coll(collection_name="User")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
