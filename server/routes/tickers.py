from fastapi import APIRouter
from agent.main import read_csv_file_live
from main import main, crawler

router = APIRouter()

@router.get("/")
def get_users():
  read_csv_file_live()
  return {
    "message": "successfully calculated sentiment and added in output"
  }

@router.get("/start-crawler")
def get_crawler():
  response = crawler()
  return response



  # start crawler -> get output in output.csv -> then trigger read_csv_file_live()