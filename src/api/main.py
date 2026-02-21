from fastapi import FastAPI

from src.api.registry_routes import router as registry_router
from src.api.scheduler_routes import router as scheduler_router

app = FastAPI(
    title="Cadence",
    description="California law enforcement intelligence aggregation platform",
    version="0.1.0",
)

app.include_router(registry_router)
app.include_router(scheduler_router, prefix="/health")


@app.get("/health")
async def health_check():
    return {"status": "ok"}
