from airflow import DAG

from airflow.decorators import task
from airflow.operators.python import PythonOperator

from abc import abstractmethod
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from typing import Dict, List
import requests

from pydruid.client import *
from pydruid.utils.aggregators import *
from pydruid.utils.filters import Dimension
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = "kafka.default.svc.cluster.local:9092"
DRUID_BROKER = "druid-broker.default.svc.cluster.local:8082"
DRUID_COORDINATOR = "druid-coordinator.default.svc.cluster.local:8081"
DRUID_URL = "druid/v2"
DRUID_DATASOURCES_LIST_ENDPOINT = "/druid/coordinator/v1/datasources"


class KafkaConnection:
    def __init__(self, client_id="sonmt"):
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
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

    def produce_df_to_kafka(self, topic: str, df: pd.DataFrame):
        existed_list_topics = self.get_existed_list_topics()
        if topic not in existed_list_topics:
            self.create_new_topic(topic)

        cols = df.columns
        row_data = {}

        for ind in df.index:
            for col in cols:
                if type(df[col][ind]) is np.int64:
                    row_data[col] = int(df[col][ind])
                else:
                    row_data[col] = df[col][ind]

            self.producer.send(topic, row_data)


class DruidConnection:
    def __init__(self):
        self.druid_host = DRUID_BROKER + "/" + DRUID_URL
        self.druid_coordinator = DRUID_COORDINATOR
        self.query = PyDruid(DRUID_BROKER, DRUID_URL)

    # def create_output_datasource(self, output):
    #     df = self.query.scan(
    #         datasource=output,
    #         intervals="0000/3000"
    #     )
    #     if len(df) == 0:
    #         # TODO: Create new datasource
    #         pass

    def load_ticker_data(self, start_date, end_date, ticker_table="ticker_data"):
        self.query.topn(
            datasource=ticker_table,
            granularity="day",
            intervals=start_date + "/" + end_date,
            filter=Dimension("ticker") != "null",
            dimension="ticker",
            metric="high",
            aggregations={"high": longmax("high"),
                          "low": longmin("low"),
                          "open": stringfirst("open"),
                          "close": stringlast("close"),
                          "volume": longsum("volume")},
            threshold=2000
        )
        df = self.query.export_pandas()
        return df

    def load_indicator_data(self, indicator_table, start_date, end_date, metric):
        if start_date is None:
            start_date = "2022-01-01T00:00"
        self.query.topn(
            datasource=indicator_table,
            granularity="day",
            intervals=start_date + "/" + end_date,
            dimension="ticker",
            metric=metric,
            aggregations={metric: longmax(metric)},
            threshold=5000
        )
        df = self.query.export_pandas()
        return df


class Miner:
    def __init__(self, input_tables: Dict[str, int],
                 indicator_name: str,
                 recursive_range: int):
        self.druid_conn = DruidConnection()
        self.kafka_conn = KafkaConnection()
        self.input_tables = input_tables
        self.indicator_name = indicator_name
        self.output_table = self.indicator_name + "_data"
        self.recursive_range = recursive_range

    def execute(self):
        print("Start loading data...")
        dict_input_dfs, start_date, end_date = self.load_data_from_druid()
        print(f"Finish loading from {start_date} to {end_date}")
        for k in dict_input_dfs.keys():
            if len(dict_input_dfs[k]) == 0:
                print("Empty Input")
                return
        print("Start transforming data...")
        output = self.transform(dict_input_dfs, start_date, end_date)
        self.save_data(output)

    def get_start_time(self):
        datasources_list = requests.get(
            self.druid_conn.druid_coordinator + DRUID_DATASOURCES_LIST_ENDPOINT).json()
        if self.output_table not in datasources_list:
            return "2022-01-01T00:00"
        else:
            today = datetime.today().date()
            iso_today = today.isoformat() + "T00:00"
            output_exist_data = self.druid_conn.load_indicator_data(self.output_table,
                                                                    "2022-01-01T00:00", iso_today, self.indicator_name)

            date_list = output_exist_data["timestamp"].values.tolist()
            last_date = max(date_list)

            start_date = datetime.strptime(last_date.split("T")[0], '%Y-%m-%d') + timedelta(days=1)
            start_date = datetime.isoformat(start_date)

            date_list = sorted(list(set(date_list)))

            return start_date, last_date, date_list

    def load_data_from_druid(self):
        # get interval's start date
        start_date, last_date, date_list = self.get_start_time()

        # get interval's end date (today)
        # today = datetime.today().date()
        # tomorrow = today + timedelta(days=1)
        # end_date = tomorrow.isoformat() + "T00:00:00"
        end_date = "2023-02-03T00:00:00"

        # get list input dfs
        dict_input_dfs = {}

        # Load ticker_data
        if "ticker_data" in self.input_tables.keys():
            ticker_offset = self.input_tables["ticker_data"]
            if ticker_offset > 0:
                start_date_ticker = date_list[-1 - ticker_offset]
            else:
                start_date_ticker = start_date
            print(f"start_date_ticker: {start_date_ticker}")
            ticker_data = self.druid_conn.load_ticker_data(start_date_ticker, end_date, ticker_table="ticker_data")
            dict_input_dfs["ticker_data"] = ticker_data
            self.input_tables.pop("ticker_data")

        # Load recursive data
        if self.recursive_range > 0:
            start_recursive_date = datetime.fromisoformat(start_date) - timedelta(days=self.recursive_range)
            start_recursive_date = start_recursive_date.isoformat()

            recursive_df = self.druid_conn.load_indicator_data(self.output_table, start_recursive_date, end_date,
                                                               self.indicator_name)

            if recursive_df is None or len(recursive_df) == 0:
                dict_input_dfs[self.output_table] = None
            else:
                dict_input_dfs[self.output_table] = recursive_df

            print("finish load recursive data")
            print(dict_input_dfs[self.output_table])

        # Load indicator_data from list
        for indicator_tab in self.input_tables.keys():
            indicator_offset = self.input_tables[indicator_tab]
            if indicator_offset > 0:
                start_date_indicator = date_list[-1 - indicator_offset]
            else:
                start_date_indicator = start_date
            indicator_name = indicator_tab.split("-")[0]
            indicator_df = self.druid_conn.load_indicator_data(indicator_tab, start_date_indicator, end_date,
                                                               indicator_name)
            dict_input_dfs[indicator_tab] = indicator_df

        return dict_input_dfs, start_date, end_date

    @abstractmethod
    def get_inputs(self, current_date: str, dict_input_dfs: Dict[str, pd.DataFrame], output_list: list):
        pass

    @abstractmethod
    def formula(self, current_date: str, dict_input_dfs: Dict[str, pd.DataFrame], output_list: list):
        pass

    def transform(self, dict_inputs_df, start_date, end_date):
        """
        Compute indicator from input data
        :param dict_inputs_df: dict of input dataframes with keys = input_table + "_data"
        :param start_date: start point of time interval
        :param end_date: end point of time interval
        :return: rs: dict of output dataframe, with keys = tickers
        """
        # Normalize time interval
        if start_date.endswith(":00.000Z"):
            start_date = start_date.replace(":00.000Z", "")
        if end_date.endswith(":00.000Z"):
            end_date = end_date.replace(":00.000Z", "")

        start_date = datetime.fromisoformat(start_date)
        end_date = datetime.fromisoformat(end_date)
        total_time_delta = (end_date - start_date).days

        print(f"...with dict_inputs_df = {dict_inputs_df}")

        # Get list ticker
        k = list(dict_inputs_df.keys())[0]
        first_input_df = dict_inputs_df[k]
        # print(f"first_input_df: {first_input_df}")
        list_ticker = sorted(list(set(first_input_df["ticker"].values.tolist())))

        # TRANSFORMATION STARTS
        rs = {}

        # Transform for each ticker
        for ticker in list_ticker:
            ticker_dict_inputs_df = {}
            output_list = []

            # Get input data for each ticker
            for item in dict_inputs_df.items():
                key = item[0]
                df = item[1]
                # Get recursive list
                if key == self.output_table:
                    recursive_ticker_df = df[df["ticker"] == ticker]
                    output_list = recursive_ticker_df[self.indicator_name].values.tolist()
                    continue
                ticker_df = df[df["ticker"] == ticker]
                ticker_dict_inputs_df[key] = ticker_df
            ticker_indicator_data = []
            for days_delta in range(total_time_delta):
                one_date = start_date + timedelta(days=days_delta)

                # Skip weekends
                if one_date.weekday() > 4:
                    continue
                one_date = datetime.isoformat(one_date)

                # Apply formula for each day
                output_list, ind_value = self.formula(one_date, ticker_dict_inputs_df, output_list)
                ticker_indicator_data.append([one_date, ticker, ind_value])

            ticker_indicator_df = pd.DataFrame(ticker_indicator_data,
                                               columns=["time", "ticker", self.indicator_name])

            ticker_indicator_df = ticker_indicator_df[np.isnan(ticker_indicator_df[self.indicator_name]) == 0]
            rs[ticker] = ticker_indicator_df
        return rs

    def save_data(self, output):
        output_list = []
        for ticker in output.keys():
            ticker_output_df = output[ticker]
            ticker_output_df.reset_index(drop=True)
            output_list.append(ticker_output_df)

        output_df = pd.concat(output_list, ignore_index=True)

        print("Final output", output_df)
        output_df.to_csv(f"/Users/sonmt/HUST/2022-1/DATN-CN/data/{self.indicator_name}_airflow.csv", index=False)
        # self.kafka_conn.produce_df_to_kafka(self.output_table, output_df)
        print("finish producing to kafka topic")


class OBVMiner(Miner):
    def __init__(self, input_tables, indicator_name, recursive_range):
        super().__init__(input_tables, indicator_name, recursive_range)

    def get_inputs(self, current_date: str, dict_input_dfs: Dict[str, pd.DataFrame], output_list: list):
        """
        :param output_list: list containing temp values of calculated indicator
        :param current_date: the date of current time in ISO Format; example: "2023-12-31T00:00"
        :param dict_input_dfs: dict of input dataframes with keys as input_table + "_data"
        :return: input variables for the formula
        """
        if not current_date.endswith(".000Z"):
            current_date += ".000Z"
        ticker_data = dict_input_dfs["ticker_data"]
        ticker_data = ticker_data.reset_index(drop=True)

        row_index_list = ticker_data.index[ticker_data['timestamp'] == current_date].tolist()
        if len(row_index_list) == 0:
            return None

        current_row_index = row_index_list[0]
        cur_close = float(ticker_data["close"][current_row_index])
        cur_volume = float(ticker_data["close"][current_row_index])
        prev_close = float(ticker_data["close"][current_row_index - 1]) if current_row_index > 0 else 0

        prev_1_obv = output_list[-1] if len(output_list) > 0 else None

        return cur_close, cur_volume, prev_close, prev_1_obv

    def formula(self, current_date: str, dict_input_dfs: Dict[str, pd.DataFrame], output_list: list):
        inputs = self.get_inputs(current_date, dict_input_dfs, output_list)
        if inputs is None:
            cur_obv = None
        else:
            cur_close, cur_volume, prev_close, prev_1_obv = inputs
            if prev_1_obv is None:
                cur_obv = 0
            else:
                cur_obv = prev_1_obv + np.sign(cur_close - prev_close) * cur_volume
        output_list.append(cur_obv)
        return output_list, cur_obv


class RSMiner(Miner):
    def __init__(self, input_tables: Dict[str, int], indicator_name: str, recursive_range: int):
        super().__init__(input_tables, indicator_name, recursive_range)

    def get_inputs(self, current_date: str, dict_input_dfs: Dict[str, pd.DataFrame], output_list: list):
        """
        :param output_list: list containing temp values of calculated indicator
        :param current_date: the date of current time in ISO Format; example: "2023-12-31T00:00"
        :param dict_input_dfs: dict of input dataframes with keys as input_table + "_data"
        :return: input variables for the formula
        """

        if not current_date.endswith(".000Z"):
            current_date += ".000Z"
        ticker_data = dict_input_dfs["ticker_data"]
        ticker_data = ticker_data.reset_index(drop=True)

        row_index_list = ticker_data.index[ticker_data['timestamp'] == current_date].tolist()
        if len(row_index_list) == 0:
            print("invalid timestamp")
            return None

        current_row_index = row_index_list[0]
        if current_row_index < 13:
            print(f"current_row_index is {current_row_index}, less than 13")
            return None

        close_list = (ticker_data.loc[current_row_index - 13:current_row_index, ["close"]])["close"].to_list()
        close_list = [int(close) for close in close_list]
        gain_list = []
        loss_list = []

        for i in range(1, len(close_list) - 1):
            diff = close_list[i] - close_list[i - 1]
            if diff >= 0:
                gain_list.append(diff)
            else:
                loss_list.append(diff)

        # time = ticker_data.loc[current_row_index, ["timestamp"]]
        #
        # if len(gain_list) == 0:
        #     print(f"gain_list of {time}: {len(gain_list)}")

        avg_gain = sum(gain_list) / len(gain_list) if len(gain_list) != 0 else 0
        avg_loss = sum(loss_list) / len(loss_list) if len(loss_list) != 0 else 0
        return avg_gain, avg_loss

    def formula(self, current_date: str, dict_input_dfs: Dict[str, pd.DataFrame], output_list: list):
        inputs = self.get_inputs(current_date, dict_input_dfs, output_list)
        if inputs is None:
            rs = 0
        else:
            avg_gain, avg_loss = inputs
            rs = avg_gain / np.abs(avg_loss) if avg_loss != 0 else 0

        output_list.append(rs)
        return output_list, rs


 # SECTION

indicator_dag = DAG(
    dag_id='indicator_dag',
    default_args={
        'retries': 1
    },
    description="DAG for stock indicators computation",
    schedule=timedelta(days=1),
    start_date=datetime.today(),
    tags=['indicator'])


@task(task_id="obv")
def task1():
    # obv_miner = OBVMiner(input_tables={"ticker_data": 0}, indicator_name="obv", recursive_range=1)
    # obv_miner.execute()
    print("OBV Miner executing...")
    return "OBV_MINER"


obv_task = PythonOperator(task_id="obv", python_callable=task1, dag=indicator_dag)


@task(task_id="rs")
def compute_rs():
    # rs_miner = RSMiner(input_tables={"ticker_data": 20}, indicator_name="rs", recursive_range=0)
    # rs_miner.execute()
    print("RS Miner executing...")
    return "RS_MINER"


rs_task = PythonOperator(task_id="rs", python_callable=compute_rs, dag=indicator_dag)

 # SECTION
obv_task >> rs_task