from fastapi import APIRouter
from agent.main import read_csv_file_live

router = APIRouter()

@router.get("/")
def get_users():
  read_csv_file_live()
  return {
    "message": "successfully calculated sentiment and added in output"
  }