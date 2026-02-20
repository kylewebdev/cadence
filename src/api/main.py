from fastapi import FastAPI

from src.api.registry_routes import router as registry_router

app = FastAPI(
    title="Cadence",
    description="California law enforcement intelligence aggregation platform",
    version="0.1.0",
)

app.include_router(registry_router)


@app.get("/health")
async def health_check():
    return {"status": "ok"}
