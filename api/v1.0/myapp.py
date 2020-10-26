from fastapi import FastAPI
import mednet
import atexit
from fastapi.middleware.cors import CORSMiddleware

uri = "neo4j://localhost:7687"
user = "neo4j"
password = "GraphNet"
neo_db = mednet.Mednet(uri, user, password)


def exit_application():
    neo_db.close()

app = FastAPI()
origins = [
    "http://localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Hello World"}

atexit.register(exit_application)