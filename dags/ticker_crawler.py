from vnstock import *
from dao.kafka_connection import KafkaConnection
from tqdm import tqdm


def crawler():
    # list ticker
    codes = listing_companies()['ticker'].tolist()
    print(len(codes))  # 1631 ticker

    list_df = []
    # get historical stock data from start_date to end_date
    today = datetime.now()
    yesterday = "2023-03-01"
    today_str = today.strftime("%Y-%m-%d")
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    start_date = yesterday_str
    end_date = today_str

    print(f"Crawling data from {start_date} to {end_date}")
    for code in tqdm(codes, desc="Stock Tickers"):
        try:
            df = stock_historical_data(symbol=code, start_date=start_date, end_date=end_date)
        except:
            continue
        df['ticker'] = code
        cols = df.columns.tolist()
        cols = cols[-1:] + cols[-2:-1] + cols[:-2]
        df = df[cols]
        list_df.append(df)
    df_all = pd.concat(list_df, ignore_index=True)

    df_all.columns = ["ticker", "time", "open", "high", "low", "close", "volume"]
    timestamp_type = {"time": str}
    df_all = df_all.astype(timestamp_type).reset_index(drop=True)
    return df_all


def main():
    print("Crawling...")
    # start = datetime.now()
    # ticker_data = crawler()
    # end = datetime.now()
    # print(f"Crawling time: {datetime.fromtimestamp(end.timestamp() - start.timestamp())}")
    # print(ticker_data)
    # kafka_conn = KafkaConnection()
    # kafka_conn.produce_df_to_kafka("ticker_data", ticker_data)
    # ticker_data.to_csv("/Users/sonmt/HUST/2022-1/DATN-CN/data/ticker_0802_0203.csv", index=False)


if __name__ == '__main__':
    main()