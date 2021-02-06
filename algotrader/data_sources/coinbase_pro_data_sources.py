from .data_source import DataSource
from cbpro_candles.candle_db import CoinbaseProCandleDatabase
from cbpro import PublicClient
import pandas as pd

cols = ['granularity', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count']


class CoinBaseProApi(DataSource):

    def __init__(self):
        self.data_source = PublicClient()

    def get_dataframe(self, product_id_list, granularity, max_count=300):

        dataframes = []

        for product_id in product_id_list:
            dataframes.append(self.data_source.get_product_historic_rates(product_id, granularity))

        candle_dfs = pd.concat(
            dataframes,
            axis=1,
            keys=[product_id for product_id in product_id_list]
        )

        print(candle_dfs)

        return candle_dfs


class CoinbaseProDatabase(DataSource):

    def __init__(self, db_file):
        super().__init__()
        self.data_source = CoinbaseProCandleDatabase(db_file)

    def get_dataframe(self, product_id_list, granularity, max_count=300):
        return self.data_source.get_dataframe(product_id_list, granularity)
