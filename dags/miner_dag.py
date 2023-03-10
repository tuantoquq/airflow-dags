from airflow import DAG
from airflow.operators.python import PythonOperator

from abc import abstractmethod
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from typing import Dict
import ticker_crawler
import pytz
from miner import Miner

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


class RSIMiner(Miner):
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
        rs_data = dict_input_dfs["rs_data"]
        rs_data = rs_data.reset_index(drop=True)

        row_index_list = rs_data.index[rs_data['timestamp'] == current_date].tolist()
        if len(row_index_list) == 0:
            print("invalid timestamp")
            return None

        current_row_index = row_index_list[0]
        rs = rs_data.loc[current_row_index]["rs_data"]

        return rs

    def formula(self, current_date: str, dict_input_dfs: Dict[str, pd.DataFrame], output_list: list):
        rs = self.get_inputs(current_date, dict_input_dfs, output_list)
        if rs is None:
            rsi = 0
        else:
            rsi = 100 / (100 + rs)

        output_list.append(rs)
        return output_list, rsi


# SECTION
tz = pytz.timezone('Asia/Ho_Chi_Minh')


def localize_utc_tz(d):
    return tz.fromutc(d)


indicator_dag = DAG(
    dag_id='indicator_dag',
    default_args={
        'retries': 1
    },
    description="DAG for stock indicators computation",
    schedule_interval='15 15 * * 1-5',
    start_date=datetime.today().replace(tzinfo=tz),
    tags=['indicator'],
    user_defined_filters={
        'local_tz': localize_utc_tz,
    }
)


crawler_task = PythonOperator(task_id="crawler", python_callable=ticker_crawler.main, dag=indicator_dag)

def compute_rs():
    print("RS Miner executing...")
    # rs_miner = RSMiner(input_tables={"ticker_data": 20}, indicator_name="rs", recursive_range=0)
    # rs_miner.execute()
    return "RS Miner finish"


rs_task = PythonOperator(task_id="rs", python_callable=compute_rs, dag=indicator_dag)


def compute_rsi():
    print("RSI Miner executing")
    # rsi_miner = RSIMiner(input_tables={"rs_data": 0}, indicator_name="rsi", recursive_range=0)
    # rsi_miner.execute()
    return "RSI Miner finish"


rsi_task = PythonOperator(task_id="rsi", python_callable=compute_rsi, dag=indicator_dag)


# SECTION
crawler_task >> (rs_task)
rs_task >> rsi_task
