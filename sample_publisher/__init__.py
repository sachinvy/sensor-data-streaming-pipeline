from configuration import simple_settings as ss
from confluent_kafka import Producer
import random as rn
from numpy import random as nprn
import json
import logging
from datetime import datetime
import pytz
import time

IST = pytz.timezone('Asia/Kolkata')

class Publisher(object):
    def __init__(self):
        self._producer_config = {
            'bootstrap.servers': ss.KAFKA_BROKER, #usually of the form cell-1.streaming.<region>.oci.oraclecloud.com:9092
        }
        self._producer = Producer(self._producer_config)

    def _get_dummy_records(cls, num_of_records=1):

        for _ in range(num_of_records):
            record = {
                "id" : rn.randint(100000,100100),
                "timestamp" : datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                "location" : {
                    "long" : rn.randint(-180,180),
                    "lat"   : rn.randint(-90,90)
                },
                "temperature" : rn.randint(30,100),
                "vibration"  :rn.randint(10,20),
                "noise"      :rn.randint(20,30),
                "image"      :nprn.rand(16,16).tolist()
            }

            yield record

    def send_records(self, num_of_records=1, data=None):
        if data is None:
            for record in self._get_dummy_records(num_of_records):
                # time.sleep(1)
                self._producer.produce(value=json.dumps(record),topic=ss.KAFKA_INPUT_TOPIC)
                self._producer.flush()
                # print(record)
        else:
            self._producer.produce(value=data,topic=ss.KAFKA_INPUT_TOPIC)
            self._producer.flush()

        logging.info("Send {} messages to kafka topic {}".format(num_of_records,ss.KAFKA_INPUT_TOPIC))





