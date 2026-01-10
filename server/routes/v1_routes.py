from server.main import app
from services.healt_services import health_services
from services.get_agent_data import get_agent_data

@app.get("/health")
def health():
  return health_services()


@app.get("/get-data")
def getData():
  get_agent_data()
  return { "status": "ok"}
    