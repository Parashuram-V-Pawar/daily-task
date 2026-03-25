from src.query_executor import *
import logging

logging.basicConfig(level=logging.INFO)

def queries():
    # Count of events per user
    logging.info("Query execution started..")
    query = """
        SELECT user_id, COUNT(*) as number_of_events
        FROM activity_fact
        GROUP BY user_id
        ORDER BY COUNT(*) DESC
    """
    result = run_query(query)
    logging.info("Query execution completed...")
    logging.info("Printing result..")
    print_result(result)
    logging.info("Result printed...")

    # Most frequent event type
    logging.info("Query execution started..")
    query = """
        SELECT a.event_id, e.event_type
        FROM activity_fact as a
        INNER JOIN event_dim as e
        ON e.event_id = a.event_id
        GROUP BY a.event_id, e.event_type
        ORDER BY COUNT(a.event_id)
        LIMIT 1
    """
    result = run_query(query)
    logging.info("Query execution completed...")
    logging.info("Printing result..")
    print_result(result)
    logging.info("Result printed...")

# user_summary
def summary_query_func(user_id):
    logging.info("Summary Query execution started..")
    summary_query = """
    WITH event_counts AS (
        SELECT e.event_type, COUNT(*) AS count
        FROM activity_fact AS a
        JOIN event_dim AS e
        ON a.event_id = e.event_id
        WHERE a.user_id = {}
        GROUP BY e.event_type
    )   
    SELECT 
        COUNT(*) AS total_events,
        (
            SELECT event_type
            FROM event_counts
            ORDER BY count DESC
            LIMIT 1
        ) AS most_frequent_event
    FROM activity_fact
    WHERE user_id = {}; 
""".format(user_id, user_id)
    result = run_query(summary_query)
    logging.info("Query execution completed...")
    print(result)

    data = result["Records"][0]
    return {
        "total_events": data[0]['longValue'],
        "most_frequent_event": data[1]['stringValue']
    }