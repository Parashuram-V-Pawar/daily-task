from src.query_executor import *
import logging

logging.basicConfig(level=logging.INFO)

logging.info("Creating user table..")
create_user_table = """
    CREATE TABLE IF NOT EXISTS user_dim(
        user_id BIGINT,
        user_name VARCHAR(50),
        user_email VARCHAR(254)
        );
"""
query = run_query(create_user_table)
logging.info("Created table user...")


logging.info("Creating event table..")
create_event_table = """
    CREATE TABLE IF NOT EXISTS event_dim(
        event_id BIGINT,
        event_type VARCHAR(50),
        user_id BIGINT
        );
"""
query = run_query(create_event_table)
logging.info("Created table event...")


logging.info("Creating time table..")
create_time_table = """
    CREATE TABLE IF NOT EXISTS time_dim(
        time_id BIGINT,
        event_time TIMESTAMP,
        event_id BIGINT
        );
"""
query = run_query(create_time_table)
logging.info("Created table time...")