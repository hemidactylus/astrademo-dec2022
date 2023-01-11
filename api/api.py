import os
import datetime
import uuid
import requests
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv('./.env')

ASTRA_DB_APPLICATION_TOKEN = os.environ['ASTRA_DB_APPLICATION_TOKEN']
ASTRA_DB_ID = os.environ['ASTRA_DB_ID']
ASTRA_DB_REGION = os.environ['ASTRA_DB_REGION']

REST_API_URL = f'https://{ASTRA_DB_ID}-{ASTRA_DB_REGION}.apps.astra.datastax.com/api/rest/v2/keyspaces/chatroom/messages'
HEADERS = {'X-Cassandra-Token': ASTRA_DB_APPLICATION_TOKEN}

app = FastAPI()

app.add_middleware(
  CORSMiddleware,
  allow_origins=['http://localhost:3000'],
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)


@app.post('/post/{room}')
async def newPost(room, request: Request):
    body = await request.json()
    message = body['message']
    sender = body['sender']
    #
    id = str(uuid.uuid1())
    when = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    #
    insertion = requests.post(
        REST_API_URL,
        headers=HEADERS,
        json={
            'id': id,
            'when': when,
            'message': message,
            'sender': sender,
            'room': room,
        }
    )
    assert(insertion.status_code == 201)
    return {'ok': True}

@app.get('/post/{room}')
async def getPosts(room):
    # get a max number (the most recent)
    messages = requests.get(
        REST_API_URL,
        headers=HEADERS,
        params={
            'where': '{"room": {"$eq": "%s"}}' % room,
            'page-size': 20,
            'sort': '{"id": "DESC"}',
        },
    )
    assert(messages.status_code == 200)
    # we discard pagination stuff
    return messages.json()['data']
