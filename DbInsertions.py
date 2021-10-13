import csv
import os
from pprint import pprint
import pandas as pd

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

    def create_coll(self, collection_name):
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

    def insert_activity_and_trackpoints(self, collection1_name, collection2_name):
        users = []
        id = 0
        id1 = 0
        for user in range(182):
            users.append(f"{user:03d}")

        for (root, dirs, files) in os.walk('Data'):
            user = str(root[5:8])

            if root[9:] == "Trajectory":
                print('Current User: ', user)
                # Now we are in the right folder
                for filename in files:
                    # read file and skip the 6 headerrows - very important
                    pltfile = pd.read_csv(str(root) + '\\' + filename, header=None, skiprows=[0, 1, 2, 3, 4, 5])
                    pltfile = pltfile.values.tolist()

                    if len(pltfile) > 2500:
                        # print("More than 2500, skip this activity")
                        continue
                    else:
                        # insert activity here
                        activity_insert = [user, str(pltfile[0][5]) + ' ' + str(pltfile[0][6]),
                                           str(pltfile[-1][5]) + ' ' + str(pltfile[-1][6])]
                        # create the doc that will be inserted into the Activity document
                        docs = [
                            {
                                "_id": id,
                                "user_id": user,
                                "start_date_time": activity_insert[1],
                                "end_date_time": activity_insert[2]
                            }
                        ]
                        collection = self.db[collection1_name]
                        collection.insert_many(docs)

                        # get the activity ID from the last insert
                        activity_id = id
                        # insert Trackpoints with the same activity ID
                        trackpoint_insert_list = []
                        docs_trackpoint = []
                        for i in range(len(pltfile)):
                            trackpoint_insert = (
                            activity_id, pltfile[i][0], pltfile[i][1], pltfile[i][3], pltfile[i][4],
                            str(pltfile[i][5]) + ' ' + str(pltfile[i][6]))
                            trackpoint_insert_list.append(trackpoint_insert)
                            temporal_doc = [
                                {
                                    "_id": id1,
                                    "activity_id": activity_id,
                                    "lat": trackpoint_insert[1],
                                    "lon": trackpoint_insert[2],
                                    "altitude": trackpoint_insert[3],
                                    "date_days": trackpoint_insert[4],
                                    "date_time": trackpoint_insert[5]
                                }
                            ]
                            docs_trackpoint.append(temporal_doc)
                            id1 += 1
                        for i in docs_trackpoint:
                            collection = self.db[collection2_name]
                            collection.insert_many(i)
                            print(i)
                        id += 1

    def match_activity_labels(self):
        labels = relevant_users()
        for (root, dirs, files) in os.walk('Data'):
            user = str(root[5:8])
            if user in labels:
                if files[0] == 'labels.txt':
                    print('Current User: ' + user)
                    with open(str(root) + '\labels.txt') as file:
                        activities = []
                        for line in csv.reader(file,
                                               delimiter='\t'):  # You can also use delimiter="\t" rather than giving a dialect.
                            line.insert(0, user)
                            activities.append(tuple(line))
                        # delete the column titles
                        activities.pop(0)
                    # check for each activity in the labels.txt file
                    for check in activities:
                        select_query = "SELECT * FROM Activity WHERE user_id = \'%s\' AND start_date_time = \'%s\' AND end_date_time = \'%s\' ;" % (
                        check[0], check[1], check[2])
                        self.cursor.execute(select_query)
                        rows = self.cursor.fetchall()
                        #  if there is a match update this Activity with the transportation mode
                        if rows:
                            update_query = "UPDATE Activity SET transportation_mode = \'%s\' WHERE id = \'%s\';" % (
                            check[3], rows[0][0])
                            self.cursor.execute(update_query)
                            self.db_connection.commit()
                            # print(update_query)
            else:
                pass
        return

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

    def create_drop(self):
        # for testing the insertions faster
        self.drop_coll(collection_name="Activity")
        self.drop_coll(collection_name="TrackPoint")
        self.create_coll(collection_name="Activity")
        self.create_coll(collection_name="TrackPoint")


def main():
    program = None
    try:
        program = ExampleProgram()
        # program.create_coll(collection_name="User")
        # program.create_coll(collection_name="Activity")
        # program.create_coll(collection_name="TrackPoint")
        # program.insert_activity_and_trackpoints(collection1_name="Activity", collection2_name="TrackPoint")
        # program.create_drop()
        # program.show_coll()
        # program.insert_documents_users(collection_name="User")
        # program.fetch_documents(collection_name="User")
        # program.drop_coll(collection_name="Activity")
        # program.drop_coll(collection_name="TrackPoint")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
