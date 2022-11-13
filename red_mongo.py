import time
from moongo import MongoConnect
from reedis import RedisConnect

if __name__ == "__main__":

    mongo_get = MongoConnect("mongodb://localhost:27017", "test_kfk", "event")
    redis_get = RedisConnect()
    while True:
        redis_count = redis_get.count_keys()
        if redis_count == 0:
            all_mongo_events = mongo_get.get_all_docs()
            redis_get.digest(all_mongo_events)
        else:
            redis_last_event = redis_get.get_last()
            missing_events = mongo_get.get_missing_docs(redis_last_event)
            redis_get.digest(missing_events)
        time.sleep(10)



"""
   print("-----------------")
            print("before insertion")
            redis_count = redis_get.count_keys()
            mongo_get.count_docs()
            redis_last_event = redis_get.get_last()
            missing_events = mongo_get.get_missing_docs(redis_last_event)
            redis_get.digest(missing_events)
            print("-----------------")
            print("after insertion")
            redis_count = redis_get.count_keys()
            mongo_get.count_docs()
"""