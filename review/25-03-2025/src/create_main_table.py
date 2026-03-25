from src.query_executor import *
import logging

logging.basicConfig(level=logging.INFO)


logging.info("Creating activity_fact table..")
create_activity_fact_table = """
    CREATE TABLE activity_fact AS SELECT
        u.user_id,
        e.event_id,
        e.event_type,
        t.time_id,
        t.event_time
    FROM user_dim as u
    INNER JOIN event_dim as e
    ON e.user_id = u.user_id
    INNER JOIN time_dim as t
    ON t.event_id = e.event_id;
"""
query = run_query(create_activity_fact_table)
logging.info("Created table activity_fact...")