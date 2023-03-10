from abc import abstractmethod
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from typing import Dict
import requests

from dao.druid_connection import DruidConnection
from dao.kafka_connection import KafkaConnection
from dao import const


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
        start = datetime.now()
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
        print(f"Finish execution in {datetime.now() - start}")

    def get_start_time(self):
        datasources_list = requests.get(
            self.druid_conn.druid_coordinator + const.DRUID_DATASOURCES_LIST_ENDPOINT).json()
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
        today = datetime.today().date()
        tomorrow = today + timedelta(days=1)
        end_date = tomorrow.isoformat() + "T00:00:00"
        # end_date = "2023-02-03T00:00:00"

        # get list input dfs
        dict_input_dfs = {}

        # Load ticker_data
        if "ticker_data" in self.input_tables.keys():
            ticker_offset = self.input_tables["ticker_data"]
            if ticker_offset > 0:
                print(date_list)
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

            recursive_df = self.druid_conn.load_indicator_data(self.output_table, start_recursive_date, end_date, self.indicator_name)

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
            indicator_df = self.druid_conn.load_indicator_data(indicator_tab, start_date_indicator, end_date, indicator_name)
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
        output_df.to_csv(f"/Users/sonmt/HUST/2022-1/DATN-CN/data/{self.indicator_name}_miner_0802_0203.csv", index=False)
        self.kafka_conn.produce_df_to_kafka(self.output_table, output_df)
        print("finish producing to kafka topic")
