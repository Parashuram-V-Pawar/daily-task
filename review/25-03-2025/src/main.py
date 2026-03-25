from fastapi import APIRouter, FastAPI
import logging
from src.query_implementation import summary_query_func

logging.basicConfig(level=logging.INFO)

router = FastAPI()

@router.get('/user/activity-summary/{user_id}')
def get_user_activities(user_id: int):
    return summary_query_func(user_id)