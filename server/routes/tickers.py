from fastapi import APIRouter
from agent.main import read_csv_file_live
from main import main, crawler
from server.model.ticker import crawlerConfig

router = APIRouter()

@router.get("/")
def get_users():
  read_csv_file_live()
  return {
    "message": "successfully calculated sentiment and added in output"
  }

@router.post("/start-crawler")
def get_crawler(_crawlerConfig: crawlerConfig):

  subreddit = _crawlerConfig.subreddit
  limit = _crawlerConfig.limit

  response = crawler(subreddit=subreddit, limit=limit)
  return response



  # start crawler -> get output in output.csv -> then trigger read_csv_file_live()