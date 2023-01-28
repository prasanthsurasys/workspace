import pymongo
import logging

logging.basicConfig(level=logging.ERROR, filename="mongo_tracker.log", format="%(asctime)s %(levelname)s %(message)s")


class MongoCrud:
    """ suitcase the each operation methods """

    def __init__(self, c_url, db_name, collect_name):
        try:
            self.client = pymongo.MongoClient(c_url)  # establish server connection
        except Exception as e:
            logging.error(f'establish connection error {e}')
        else:
            print('Establish the connection')
        self.db = self.checkExitsDatabase(db_name)
        self.col_data = self.checkExitsCollection(collect_name)

    def checkExitsDatabase(self, db_name):
        """ check the database if already exits or not """
        try:
            if db_name in self.client.list_database_names():
                print(f'record DB is {db_name} found \n')
                return self.client[db_name]
            print(f'create new database {db_name} is not found ')
            return self.client[db_name]
        except Exception as dbe:
            logging.error(f"Database error is  {dbe}")

    def checkExitsCollection(self, col_name):
        """ check the collection if already exits or not """
        try:
            if col_name in self.db.list_collection_names():
                print(f'record collection is {col_name} found \n')
                return self.db[col_name]
            print(f'create new collection {col_name} is not found ')
            return self.db[col_name]
        except Exception as cole:
            logging.error(f"collection error is {cole}")
