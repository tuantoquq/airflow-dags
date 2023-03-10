import json
import numpy
import pandas
import yaml
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
from typing import List


def load_configs(path="dao/config.yml"):
    with open(path, "r") as stream:
        try:
            config = yaml.safe_load(stream)
            return config
        except yaml.YAMLError as exc:
            print(exc)


class KafkaConnection:
    def __init__(self, client_id="sonmt"):
        config = load_configs()
        bootstrap_servers = config["kafka_bootstrap_servers"]
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id=client_id
        )
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)

    def create_new_topic(self, topic_names: str or List[str]):
        topic_list = []
        if type(topic_names) == list:
            for topic in topic_names:
                topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

        if type(topic_names) == str:
            topic_list = [NewTopic(name=topic_names, num_partitions=1, replication_factor=1)]

        self.admin_client.create_topics(new_topics=topic_list)

    def get_existed_list_topics(self):
        list_topics = self.consumer.topics()
        return list(list_topics)

    def produce_df_to_kafka(self, topic: str, df: pandas.DataFrame):
        existed_list_topics = self.get_existed_list_topics()
        if topic not in existed_list_topics:
            self.create_new_topic(topic)

        cols = df.columns
        row_data = {}

        for ind in df.index:
            for col in cols:
                if type(df[col][ind]) is numpy.int64:
                    row_data[col] = int(df[col][ind])
                else:
                    row_data[col] = df[col][ind]

            self.producer.send(topic, row_data)



