from pymongo import MongoClient # type: ignore

class MongoDBSingleton:
    _instance = None

    @staticmethod
    def get_instance():
        if MongoDBSingleton._instance is None:
            MongoDBSingleton._instance = MongoClient("mongodb://mongodb:27017/")['dvdrental']
        return MongoDBSingleton._instance