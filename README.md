# Steps

### DB and Admin token

make sure (AWS) `us-east-1` for later cdc (ultrasink)

### astra cli

get astra cli working with token configured

### astra-cli create/config DB and keyspace + env

_Note_: append `--config ultrasink` to astra commands to use that account.

```
astra db create workshops --if-not-exist -k chatroom --wait
```

```
astra db create-dotenv workshops -k chatroom
. .env
```

```
astra db cqlsh workshops -f cql/initialize.cql
```

### api

(in virtualenv `astra_demo_38`)

```
cd api
pip install fastapi uvicorn[standard] python-dotenv requests
```

WRITE: create `api.py`

```
uvicorn api:app
```

can test with curl:

```
curl -XPOST localhost:8000/post/food -d '{"message": "hi2", "sender": "ada"}'
curl localhost:8000/post/food | jq
```

### client

```
cd client
npx create-react-app chatclient
cd chatclient
npm install axios
npm start
```

WRITE tweak client code

Milestone 1 reached.

### CDC boutade, Astra

in astra UI, AWS "useast1" to create a streaming tenant

go to database and CDC. "Enable CDC": pick tenant, keyspace, table.
"initializing", check for "running" (1 minute).

### CDC, another Py API

go to topics in streaming, get a streaming token in settings tab for tenant.

connect, python, either <consumer> or <cdc consumer> tabs?

```
pip install pulsar-client==2.10.2 avro
cd sapi
```

#### WRITE AND TWEAK, API

copy the sample cdc reader
get the schema from astra streaming UI (!) (after at least one msg is written with cdc on), save and replace reading from it and no asking the api (skip "data", see sapi0.py)
this runs and captures everything.

Then write the websocket api and paste parts of the cdc examples in it.

```
uvicorn sapi:app --port 8001
```

### CDC, more in the client

create the ws and attach it. the big trouble is avoiding duplicates (no uuids around)

# References

https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_client.py

# Frustrations

astrapy: no docs, did not work as in the test code (test_client.py, no .rest and other).
Abandoned astrapy and did direct requests.

swagger and timestamps, no doc. differs from cql formats. no info on stargate site, nor astra site, nothing. Only solution found here: https://datastax.slack.com/archives/CUTBC5AUF/p1667959454730679 (Nov 9th!)

rest v2, where is it said that 'sort' must be fed something like
    {"when": "DESC"}
???

cdc and python: sample code gets a 404 on the schema endpoint, have to copy paste a json, parse it, etc. One feels much left alone.

(pulsar) docs tell you to use AvroSchema("Record" instance), nowhere is it said there's a second missing arg and examples found around don't work. (see e.g. https://github.com/ta1meng/pulsar-python-avro-schema-examples/blob/main/example01_simple_schema/SimpleSchema.py but same on https://pulsar.apache.org/docs/2.10.x/client-libraries-python/#simple-definition , see `schema=AvroSchema(Example))` line)

In Astra UI, CDC does not "Initializing" -> "Running" by itself, you have to refresh.
