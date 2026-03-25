from src.query_executor import *
import logging
from faker import Faker
import random
from datetime import datetime, timedelta

f = Faker()

num_of_users = 10
num_of_events = 25
num_of_time = 25

events = ['Login', 'Logout', 'Click','Idle','Sleep']

def random_timestamp():
    startime = datetime(2025, 1, 1)
    endtime = datetime(2026, 3, 24)
    delta = endtime - startime
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return startime+timedelta(seconds=random_seconds)


try:
    logging.info("Inserting values to the table user..")
    for i in range(0, num_of_users):
        user_values = """
            INSERT INTO user_dim(user_id, user_name, user_email) VALUES 
            ({},'{}','{}')
        """.format(i+1, f.name(), f.email())
        query = run_query(user_values)
    logging.info("Inserting values to the table user completed..")

    logging.info("Inserting values to the table event..")
    for i in range(0, num_of_events):
        event_values = """
            INSERT INTO event_dim(event_id, event_type, user_id) VALUES 
            ({},'{}',{})
        """.format(i+1, random.choice(events), random.randint(1, num_of_users+1))
        query = run_query(event_values)
    logging.info("Inserting values to the table event completed..")

    logging.info("Inserting values to the table time..")
    for i in range(0, num_of_time):
        timestamp = random_timestamp()
        time_values = """
            INSERT INTO time_dim(time_id, event_time, event_id) VALUES 
            ({},'{}',{})
        """.format(i+1, timestamp, random.randint(1, num_of_events+1))
        query = run_query(event_values)
    logging.info("Inserting values to the table time completed..")
except Exception as e:
    logging.info(f"Error occurred: {e}")
