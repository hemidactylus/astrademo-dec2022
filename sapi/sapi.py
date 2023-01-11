import time
import os
import base64
import io
import json
import re
import pulsar
import asyncio

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

import avro.schema
from avro.io import BinaryDecoder, DatumReader

app = FastAPI()

app.add_middleware(
  CORSMiddleware,
  allow_origins=['http://localhost:3000'],
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)

###
service_url = "pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651"
admin_url = "https://pulsar-aws-useast1.api.streaming.datastax.com"
token = "TOKEN"
topic_name = "TOPIC_NAME"


def getSchema():
    data_schema = json.load(open('schema.json'))

    keyschema_json = data_schema["key"]
    valueschema_json = data_schema["value"]

    # the namespaces start with numbers and AVRO doesn't like it
    # so strip them out for now
    key_namespace = keyschema_json["namespace"]
    key_namespace = re.sub("\d.*_", "", key_namespace)
    keyschema_json["namespace"] = key_namespace
    value_namespace = valueschema_json["namespace"]
    value_namespace = re.sub("\d.*_", "", value_namespace)
    valueschema_json["namespace"] = value_namespace
    keyAvroSchema = avro.schema.parse(json.dumps(keyschema_json))
    valueAvroSchema = avro.schema.parse(json.dumps(valueschema_json))

    return keyAvroSchema, valueAvroSchema

keyAvroSchema, valueAvroSchema = getSchema()

keyAvroReader = DatumReader(keyAvroSchema)
valueAvroReader = DatumReader(valueAvroSchema)

client = pulsar.Client(service_url, authentication=pulsar.AuthenticationToken(token))

###

def tryReceive(consumer):
    try:
        msg = consumer.receive(5)
        return msg
    except Exception as e:
        if 'timeout' in str(e).lower():
            return None
        else:
            raise e

###

@app.websocket('/updates/{userId}')
async def ws(ws: WebSocket, userId: str):
    await ws.accept()
    consumer = client.subscribe(
        topic="persistent://" + topic_name,
        subscription_name=userId,
    )
    print('Accepted %s' % userId)
    try:
        while True:
            msg = tryReceive(consumer)
            if msg is not None:

                # The PartitionKey is Base64 Encoded, so it needs to be decoded
                msgKey = msg.partition_key()
                msgKey_decoded = base64.b64decode(msgKey)

                messageKey_bytes = io.BytesIO(msgKey_decoded)
                keydecoder = BinaryDecoder(messageKey_bytes)
                msgKey = keyAvroReader.read(keydecoder)

                message_bytes = io.BytesIO(msg.data())
                decoder = BinaryDecoder(message_bytes)
                msgValue = valueAvroReader.read(decoder)

                msgToSend = {
                    'message': msgValue['message'],
                    'room': msgKey['room'],
                    'id': str(msgKey['id']),
                    'sender': msgValue['sender'],
                    'when': msgValue['when'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                }
                msgJson = json.dumps(msgToSend)
                print('Sending to %s: "%s"' % (userId, msgJson))
                await ws.send_text(msgJson)
                consumer.acknowledge(msg)

            await asyncio.sleep(5)
    except WebSocketDisconnect:
        print('Disconnected', userId)
