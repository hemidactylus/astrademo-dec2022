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

logging.basicConfig(
    format='%(asctime)s.%(msecs)05d %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

service_url = "pulsar+ssl://pulsar-aws-useast1.streaming.datastax.com:6651"
admin_url = "https://pulsar-aws-useast1.api.streaming.datastax.com"
token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NzE3NTE1NjYsImlzcyI6ImRhdGFzdGF4Iiwic3ViIjoiY2xpZW50OzIyNzk3YWEzLTM5ODgtNDU3ZS05ZmMyLWQ5MTY5MTJmYzM2MDtiWGxrWlcxdmRHVnVZVzUwOzYxMTE4M2FkZGUiLCJ0b2tlbmlkIjoiNjExMTgzYWRkZSJ9.soqiEnuV2WmcKoA-1W2CEIdNVkGF00Vk-Ibo8doNoVoup2P2xuYexrunDSsibF108ibhr3puTuznopAwg0N_6O97LVnGYphXvbpygzNTa51skgSFbSJsj65dB0jT2irfKyyketl1WWHbq3b431NbYy8_SKfB9EMSRukcW-P5aFlTUL5XRGjH0ycHNL-6Iw5Q5bl6wW9Ah9v-gFs7AasCkCaAx_OCEqsYbgUWLAc4pV0nTZPk6qKbqKfxLzk7D-vVLlvGTNx3_9el9SJjkbNqr0h1nbWR9InK5M1otDprumMm0DaUOkYAyMo8XnftOBy3hF3BcrXYsJpDyXyGojJzcg"
topic_name = "mydemotenant/astracdc/data-0fe0bfe6-5405-4d95-8d5c-715a317ea736-chatroom.messages"
subscription_name = "my-subscription"


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

consumer = client.subscribe(
    topic="persistent://" + topic_name,
    subscription_name=subscription_name,
    initial_position=pulsar.InitialPosition.Earliest,
)

waitingForMsg = True
while waitingForMsg:
    try:
        msg = consumer.receive()

        # The PartitionKey is Base64 Encoded, so it needs to be decoded
        msgKey = msg.partition_key()
        msgKey_decoded = base64.b64decode(msgKey)

        messageKey_bytes = io.BytesIO(msgKey_decoded)
        keydecoder = BinaryDecoder(messageKey_bytes)
        msgKey = keyAvroReader.read(keydecoder)

        message_bytes = io.BytesIO(msg.data())
        decoder = BinaryDecoder(message_bytes)
        msgvalue = valueAvroReader.read(decoder)

        logging.info("Received message key='{}' value='{}'".format(msgKey, msgvalue))

        # logging.info("Received message")

        # Acknowledging the message to remove from message backlog
        consumer.acknowledge(msg)

        # waitingForMsg = False
    except:
        logging.info("Still waiting for a message...")

   

client.close()