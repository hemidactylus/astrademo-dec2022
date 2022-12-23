import base64
import io
import json
import re
import time
from urllib.request import Request, urlopen

import avro.schema
import pulsar
from avro.io import BinaryDecoder, DatumReader

import logging

import time
import os
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

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
token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NzE3NTY2MjEsImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50OzIyNzk3YWEzLTM5ODgtNDU3ZS05ZmMyLWQ5MTY5MTJmYzM2MDtjM1JsYm1GdWRBPT07NzBkMzRiNzRiZiIsInRva2VuaWQiOiI3MGQzNGI3NGJmIn0.IAb-5VlGQ5HfdoYPCFUkmT-7TY26NDjBp7dunBk1LOOSjCQuxMHVRRPu-y5kHcWRSXcEqQxC_E1-SIt5HiXK9E9Tfq51mC3j8t7BiVXFvjdgTKpddLMqt6gNB5AUdbC4TD5JQv9w2W8_zmgKwaCE2twwLAQj59esgIibco8LTKuHvy0NDLDJeRUgWcUoDppIcIc6QN5MXZNqJ_em0yF1j-s4BZdo3mWVCGhsznQcWQttTnKhC1REOaNDXmnH8RTZuMnFQsspcXxzYqTv63OQZWRvet7o8mk6nNgmOoYCuMh9cPjQYdAR72URw4DelZQ7YWRNttOFf2h50VRLz5URQg"
topic_name = "stenant/astracdc/data-0fe0bfe6-5405-4d95-8d5c-715a317ea736-chatroom.messages"

def http_get(url):
    req = Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("Authorization", "Bearer " + token)
    return urlopen(req).read()


def getSchema():
    # schema_url = "%s/admin/v2/schemas/%s/schema" % (admin_url, topic_name)
    # topic_schema = http_get(schema_url).decode("utf-8")
    # This isn't great
    # the data part of the json has extra back slashes
    # topic_schema = topic_schema.replace("\\", "")
    # topic_schema = topic_schema.replace('data":"', 'data":')
    # topic_schema = topic_schema.replace('}","properties', '},"properties')

    # logging.info("Topic'{}' Schema='{}'".format(topic_name, topic_schema))

    # schema_json = json.loads(topic_schema)
    schema_json = json.load(open('schema.json'))

    data_schema = schema_json #["data"]

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
