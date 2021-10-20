from pprint import pprint

import numpy as np

from DbConnector import DbConnector


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def query_1(self):
        print("Counting documents...")
        result1 = self.db.User.count_documents({})
        result2 = self.db.Activity.count_documents({})
        result3 = self.db.TrackPoint.count_documents({})

        print(result1, result2, result3)

    def query_2(self):
        print("Starting count...")
        result = 0
        max = 1
        min = 1
        for user in range(182):
            user = f"{user:03d}"
            temp = self.db.Activity.count_documents({"user_id": user})
            result = temp + result
            if temp > max:
                max = temp
            if temp < min:
                min = temp

        average = result / 182
        print(average, max, min)

    def query_3(self):
        print("Counting...")
        dic = {}
        dic_sorted = {}
        user_list = []
        position = 0
        for user in range(182):
            user = f"{user:03d}"
            temp = self.db.Activity.count_documents({"user_id": user})
            dic[temp] = user
        dic_sorted = sorted(dic.items(), reverse=True)
        for i in dic_sorted:
            user_list.append(i[1])
            position = position + 1
            if position > 9:
                break
        print(user_list)

    def query_7(self):
        print("Calculating...")
        total_users = []
        taxi_users = []
        for user in range(182):
            user = f"{user:03d}"
            total_users.append(user)
            rows = self.db.Activity.find({"user_id": user, "transportation_mode": "taxi"})
            for row in rows:
                if row["user_id"] not in taxi_users:
                    taxi_users.append(row["user_id"])
        print(sorted(list(set(total_users) - set(taxi_users))))





def main():
    program = None
    try:
        program = ExampleProgram()
        program.query_1()
        program.query_2()
        program.query_3()
        program.query_7()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()