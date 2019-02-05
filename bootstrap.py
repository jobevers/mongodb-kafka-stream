import pymongo
import json
import subprocess
import kafka
import time
import bson


collections = {
    "rf",
    "adt",
    "lab",
    "ecg",
    "vf",
    "labMuse",
    "nf",
}

conn = pymongo.MongoClient('mongodb://localhost:27017/?replicaSet=rs0')
conn.admin.drop_collection('mirth')

db = conn.mirth

timestamps = {}

for coll in collections:
    db[coll].insert_one({'msg': 'first'})
    timestamps[coll] = time.time()
    time.sleep(1)

for coll in collections:
    timestamp = bson.Timestamp(int(timestamps[coll]), 0)
    with db[coll].watch(start_at_operation_time=timestamp) as stream:
        for change in stream:
            timestamps[coll] = change['clusterTime']
            break

producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')


def create_msg(coll):
    data = {
        'schema': {
            'type': 'struct',
            'optional': False,
            'fields': [
                {'type': 'int32', 'optional': True, 'field': 'timestamp'},
                {'type': 'int32', 'optional': True, 'field': 'order'}],
            'name': f'mongodbschema_mirth_{coll}'
            },
        'payload': {
            'timestamp': timestamps[coll].time,
            'order': timestamps[coll].inc,
        }
    }
    return json.dumps(data).encode('utf-8')


for coll in collections:
    topic = f'mongo_mirth_{coll}'
    subprocess.run(['./kafka_2.11-2.1.0/bin/kafka-topics.sh', '--delete',
                    '--topic', topic, '--zookeeper', 'localhost:2181'])
    subprocess.run(['./kafka_2.11-2.1.0/bin/kafka-topics.sh', '--create',
                    '--partitions', '1', '--topic', topic, '--replication-factor', '1',
                    '--zookeeper', 'localhost:2181'])
    producer.send(topic, create_msg(coll))
