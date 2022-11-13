import redis
import re


class RedisConnect:

    redis_client = redis.Redis(host="localhost", port=6379, db=0)

    def init(self):
        pass

    def connect(self):
        try:
            check_conn = self.redis_client.ping()
            if check_conn:
                return True
        except redis.exceptions.ConnectionError as err:
            print(f"redis connection failure: {err}")
            return False

    def digest(self, events):
        try:
            for event in events:           
                self.redis_client.lpush("events", str(event))
        except redis.ResponseError as err:
            print(err)

    def get_last(self):
            try:
                last = self.redis_client.lrange("events", 0, 0)
                time = re.findall(
                    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{0,6}", last[0].decode()
                )
                return str(time[0])
            except redis.exceptions.AskError as err:
                print(err)
                return False

    def count_keys(self):
        try:
            num_of_keys = self.redis_client.llen("events")
            print(f"{num_of_keys} keys in cache")
            return num_of_keys
        except redis.exceptions.AskError as err:
            print(err)
            return False
