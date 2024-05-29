import pymongo


class MongoGtw:
    def __init__(self, mongo_uri) -> None:
        self.mongo_uri = mongo_uri

    def get_client(self):
        return pymongo.MongoClient(self.mongo_uri)
