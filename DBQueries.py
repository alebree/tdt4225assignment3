from pprint import pprint
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

    


def main():
    program = None
    try:
        program = ExampleProgram()
        program.query_1()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()