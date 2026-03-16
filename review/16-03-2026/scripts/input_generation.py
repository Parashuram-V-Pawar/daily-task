from faker import Faker
import pandas as pd
import random
import uuid
import json
import time
# user_id (PK)
# String
# notification_id (SK)
# String
# message
# String
# timestamp
# Number
def input_generation(n):
    faker = Faker()
    print("Generating json file...")

    data = []
    for i in range(n):
        record = {
            "user_id" : f"USR00{i}",
            "notification_id" : str(uuid.uuid4()),
            "message" : faker.sentence(nb_words=5),
            "time_stamp" : faker.date_time_between(start_date='-30d', end_date='now').isoformat()
        }
            
        data.append(record)

        with open('data.json', "w") as writer:
            json.dump(data, writer, indent = 4)
    print("JSON file generation completed...")

if __name__ == "__main__":
    input_generation(100)
