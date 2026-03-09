from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from greengraph.database import create_db_and_tables
from greengraph.routes import graphs_router

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield


app = FastAPI(
    title="GreenGraph",
    description="A graph editor with LiteGraph.js frontend",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(graphs_router)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


def run():
    uvicorn.run("greengraph.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    run()
