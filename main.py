from mongo import MongoCrud
import csv


class CrudOperation(MongoCrud):
    def __init__(self, c_url, db_name, collect_name, mog_filename):
        MongoCrud.__init__(self, c_url, db_name, collect_name)
        self.mo_filename = mog_filename

    def bulk_insert_mango(self):
        """ insert bulk document into database"""
        insert_doc = list(mongo_instant.convert_csv_to_json_format())
        try:
            return "successfully inserted " if self.col_data.insert_many(insert_doc) else "failed"
        except Exception as bex:
            raise Exception(f"Error found in bulk insertion block is {bex}")

    def insert_mongo(self, insert_doc):
        """ Insert document to the collection"""
        try:
            return "successfully inserted " if self.col_data.insert_one(insert_doc) else "failed"
        except Exception as m_e:
            raise Exception(f"Error found in insertion block is {m_e}")

    def find_mongo(self):
        """ Cursor object iterate all the data from database """
        try:
            return list(self.col_data.find())
        except Exception as fex:
            raise Exception(f"Error found in find mongo suit is {fex}")

    def filter_mongo(self, mon_obj):
        """ Filter object get particular data with dict form"""
        try:
            return list(self.col_data.find(mon_obj))
        except Exception as fmex:
            raise Exception(f" Error found in filter mongo suit is {fmex}")

    def update_mongo(self, find_obj, up_obj):
        """ Update document with dict format """
        try:
            return self.col_data.update_many(find_obj, {"$set": up_obj})
        except Exception as up_ex:
            raise Exception(f"error found in delete mongo suit is {up_ex}")

    def delete_mongo(self, del_obj):
        """ Delete particular document with dict form"""
        try:
            return self.col_data.delete_many(del_obj)
        except Exception as dex:
            raise Exception(f'error found in delete mongo suit is {dex}')

    def convert_csv_to_json_format(self):
        """Generate as a dictionary form"""
        with open(self.mo_filename) as carbon_data:
            for i in csv.DictReader(carbon_data, delimiter=';'):
                yield i


if __name__ == "__main__":
    cloud_url = "mongodb+srv://test:test@cluster0.3cjugmy.mongodb.net/?retryWrites=true&w=majority"
    database_name = "carbon"
    collection_name = "col_corb"
    mo_filename = './carbon_nanotubes.csv'
    mongo_instant = CrudOperation(cloud_url, database_name, collection_name, mog_filename=mo_filename)
    # mongo_instant.bulk_insert_mango()
    # print(mongo_instant.filter_mongo({'Chiral indice n': '2'}))
    # print(mongo_instant.update_mongo(find_obj={'Chiral indice n': '2'}, up_obj={'Chiral indice n': '01'}))
    # print(mongo_instant.delete_mongo({'Chiral indice n': '01'}))

