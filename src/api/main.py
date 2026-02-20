from fastapi import FastAPI

app = FastAPI(
    title="Cadence",
    description="California law enforcement intelligence aggregation platform",
    version="0.1.0",
)


@app.get("/health")
async def health_check():
    return {"status": "ok"}
