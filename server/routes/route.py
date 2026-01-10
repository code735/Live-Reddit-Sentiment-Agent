from server.main import app
from services.healt_services import health_services

@app.get("/health")
def health():
  return health_services()
  