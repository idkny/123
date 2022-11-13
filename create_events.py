from faker import Faker
import datetime


fake = Faker()


class EventGenerator:

    event_id = 0

    def __init__(self):
        pass

        EventGenerator.event_id += 1

    def create_new_event(self):
       

        data = {
            "event_id": EventGenerator.event_id,
            "name": fake.name(),
            "street": fake.street_address(),
            "city": fake.city_suffix(),
            "post_code": fake.postcode(),
            "country_code": fake.country_code(),
            "country": fake.country(),
            "created_at": datetime.datetime.now(),
        }

        return data
