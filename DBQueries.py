import os
from pprint import pprint

import numpy as np
import geopy.distance
from DbConnector import DbConnector
from datetime import datetime, timedelta
from geopy.distance import geodesic


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def query_1(self):
        print("Query 1: Starting...")
        result1 = self.db.User.count_documents({})
        result2 = self.db.Activity.count_documents({})
        result3 = self.db.TrackPoint.count_documents({})

        print(result1, result2, result3)

    def query_2(self):
        print("Query 2: Starting...")
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
        print("Query 3: Starting...")
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

    # Find the number of users that have started the activity in one day and ended the activity the next day
    def query_4(self):
        print("Query 4: Starting...")
        users = []
        rows = self.db.Activity.find({})
        for doc in rows: 
            start = date_time_obj = datetime.strptime(doc['start_date_time'][:10], '%Y-%m-%d')
            end = date_time_obj = datetime.strptime(doc['end_date_time'][:10], '%Y-%m-%d')

            # when end_date minus 1 day is the start date add this user to a list
            if end - timedelta(days=1) == start:
                user = doc['user_id']
                if user not in users:
                    users.append(user)
        print(users)
        print('Amount of relevant users:', len(users))
        return

    def query_6(self):
        print("Query 6: Starting...")
        dot1 = (39.97548, 116.33031)
        time1 = "2008-08-24 15:38:00"
        time1 = datetime.strptime(time1, '%Y-%m-%d %H:%M:%S')
        covid_users = []
        for user in range(182):
            user = f"{user:03d}"
            print("Starting ", user)
            activity_ids = []
            for x in self.db.Activity.find({"user_id": user}):
                activity_ids.append(x["_id"])
            for activity in activity_ids:
                for x in self.db.TrackPoint.find({"activity_id": activity}):
                    dot2 = (x["lat"], x["lon"])
                    time2 = x["date_time"]
                    time2 = datetime.strptime(time2, '%Y-%m-%d %H:%M:%S')
                    if geodesic(dot1, dot2).m < 100 and time1.year == time2.year and time1.month ==\
                            time2.month and time1.day == time2.day and time1.hour ==\
                            time2.hour and time1.minute == time2.minute:
                        if user not in covid_users:
                            covid_users.append(user)
                            
        print(covid_users)

    def query_7(self):
        print("Query 7: Starting...")
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

    def query_8(self):
        print("Query 8: Starting...")
        transportation_array = []
        dic = {}
        dic_num = {}
        transportation_rows = []
        for user in range(182):
            user = f"{user:03d}"
            rows = self.db.Activity.find({"user_id": user})
            for row in rows:
                if row["transportation_mode"] not in transportation_array and row["transportation_mode"] is not None:
                    transportation_array.append(row["transportation_mode"])
        for j in transportation_array:
            dic[j] = []
            dic_num[j] = 0
        for user in range(182):
            user = f"{user:03d}"
            for i in transportation_array:
                row = self.db.Activity.find({"user_id": user, "transportation_mode": i})
                for rows in row:
                    transportation_rows.append(rows)
        for j in transportation_rows:
            if j["user_id"] not in dic[j["transportation_mode"]]:
                dic[j["transportation_mode"]].append(j["user_id"])
                dic_num[j["transportation_mode"]] = dic_num[j["transportation_mode"]] + 1
        print(dic_num)

    def query_10(self):
        print("Query 10: Starting...")
        rows = []
        for x in self.db.Activity.find({"user_id": "112", "transportation_mode": "walk"}):
            rows.append(x)
        activity_ids = []
        for i in rows:
            activity_ids.append(i["_id"])
        total_distance = 0
        # interate over all relevant activities
        for i in activity_ids:
            gps_points = []
            tpoints = []
            for y in self.db.TrackPoint.find({"activity_id" : i}):
                tpoints.append(y)
            for j in tpoints:
                gps_points.append((j["lat"], j["lon"]))
            gps_points = tuple(gps_points)

            distance_activity = 0
            for count in range(len(gps_points) - 1):
                distance_activity += geopy.distance.distance(gps_points[count], gps_points[count + 1]).km
            total_distance += distance_activity
        return print('Total distance walked: ', total_distance)

        # only sum up altitude when next TrackPoint is higher than the one before

    def query_11(self):
        print("Query 11: Starting...")
        user_altitude_dic = {}
        # First get all activites from one user
        for user in range(182):
            user = f"{user:03d}"
            activity_ids = []
            for x in self.db.Activity.find({"user_id": user}):
                activity_ids.append(x["_id"])
            total_altitude_user = 0
            # Get the altitudes for every activity of the User
            for activity in activity_ids:
                rows = []
                for x in self.db.TrackPoint.find({"activity_id": activity}):
                    rows.append(x["altitude"])
                total_altitude_activity = 0
                for i in range(0, len(rows)):
                    if i == 0:
                        continue
                    # print(rows[i])
                    if rows[i] == -777:
                        continue
                    if rows[i] > rows[i - 1]:
                        # print(rows[i])
                        total_altitude_activity += rows[i] - rows[i - 1]

                # add the gained altitude of every activity of the user to the total user gained altitude
                total_altitude_user += total_altitude_activity
                # convert feet to meters for the solution
                total_altitude_user = total_altitude_user / 3.2808
            # add all user to a dictionary
            user_altitude_dic[user] = total_altitude_user

        sorted_dic = dict(sorted(user_altitude_dic.items(), key=lambda item: item[1], reverse=True))
        return print('Solution in decending order: ', sorted_dic)

    def query_12(self):
        print("Query 12: Starting...")
        # dictionary with invalid activities per user
        invalid_activities_users = {}
        # First get all activities from one user
        for user in range(182):
            user = f"{user:03d}"
            activity_ids = []
            for x in self.db.Activity.find({"user_id": user}):
                activity_ids.append(x["_id"])

            invalid_activities = []
            # Get the TrackPoint data for one activity
            for activity in activity_ids:
                rows = []
                for x in self.db.TrackPoint.find({"activity_id": activity}):
                    rows.append(datetime.strptime(x["date_time"], '%Y-%m-%d %H:%M:%S'))
                invalid_activity = False
                for i in range(0, len(rows)):
                    # skip first entry
                    if i == 0:
                        continue
                    time_difference = rows[i] - rows[i - 1]
                    # check if consecutive trackpoint is earlier ---> never actually happens but still
                    if time_difference < timedelta(minutes=0):
                        invalid_activity = True
                    # check if consecutive trackpoint is more than 5 min later than previoous trackpoint
                    if time_difference > timedelta(minutes=5):
                        # print(time_difference)
                        # print('Invalid Activity', activity)
                        invalid_activity = True
                    else:
                        pass

                if invalid_activity is True:
                    invalid_activities.append(activity)

            if len(invalid_activities) > 0:
                invalid_activities_users[user] = invalid_activities

        # print(invalid_activities_users)
        print(len(invalid_activities_users))
        for key, value in invalid_activities_users.items():
            print(key, len([item for item in value if item]))

def main():
    program = None
    try:
        program = ExampleProgram()
        program.query_4()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
