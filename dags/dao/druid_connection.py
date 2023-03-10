import yaml
from pydruid.client import *
from pydruid.utils.aggregators import *
from pydruid.utils.filters import Dimension


def load_configs(path="config.yml"):
    with open(path, "r") as stream:
        try:
            config = yaml.safe_load(stream)
            return config
        except yaml.YAMLError as exc:
            print(exc)


class DruidConnection:
    def __init__(self):
        config = load_configs()
        self.druid_host = config["druid_broker"] + "/" + config["druid_url"]
        self.druid_coordinator = config["druid_coordinator"]
        self.query = PyDruid(config["druid_broker"], config["druid_url"])

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

