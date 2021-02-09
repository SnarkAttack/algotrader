import sqlite3
from decimal import Decimal
import pandas as pd
import dateutil.parser
from datetime import datetime, timezone
import cryptowatch as cw
from google.protobuf.json_format import MessageToJson
import json
from .data_source import DataSource

coinbase_pro_asset_ids = {
    65: "btcusd",
    68: "ethusd",
    70: "ltcusd",
    464: "bchusd",
    4996: "etcusd",
    5281: "zrxusd",
    3359: "xrpusd",
    52519: "xlmusd",
    59629: "eosusd",
    59633: "repusd",
    60555: "linkusd",
    60891: "xtzusd",
    60910: "algousd",
    61105: "dashusd",
    61666: "atomusd",
    61869: "oxtusd",
    61930: "kncusd",
    62910: "daiusd",
    62911: "zecusd",
    62912: "batusd",
    62980: "omgusd",
    63075: "mkrusd",
    63158: "compusd",
    63520: "bandusd",
    63562: "nmrusd",
    63903: "umausd",
    63904: "yfiusd",
    63905: "lrcusd",
    64346: "uniusd",
    85463: "renusd",
    85464: "balusd",
    92864: "wbtcusd",
    136704: "nuusd",
    136931: "filusd",
    137192: "bntusd",
    137195: "snxusd",
    137202: "aaveusd",
    137349: "grtusd",
    137670: "celousd",
}

config = json.load(open('config.ini', 'r'))

api_key = config['cryptowatch']['security']['api_key']


def adapt_decimal(d):
    return str(d)

def convert_decimal(s):
    return Decimal(s.decode("utf-8"))

def adapt_iso_datetime(dt):
    return str(dt)

def convert_iso_datetime(s):
    return dateutil.parser.isoparse(s)

class CryptoWatchWebsocket(object):

    def __init__(self, db_file='databases/cryptowatch_coinbase_pro_candles.db'):
        # Set your API Key
        cw.api_key = api_key
        # Subscribe to resources (https://docs.cryptowat.ch/websocket-api/data-subscriptions#resources)
        #cw.stream.subscriptions = ["exchanges:2:ohlc"]
        cw.stream.subscriptions = [f"markets:{asset_id}:ohlc" for asset_id in coinbase_pro_asset_ids.keys()]
        cw.stream.on_intervals_update = self.handle_intervals_update
        self.database = CryptoWatchDatabase(db_file)
        self.printed = False

    # What to do on each trade update
    def handle_intervals_update(self, interval_update):
        json_msg = json.loads(MessageToJson(interval_update))['marketUpdate']
        #print(json_msg['marketUpdate'].keys())
        exchange_id = json_msg['market']['exchangeId']
        market_id = json_msg['market']['marketId']
        for interval in json_msg['intervalsUpdate']['intervals']:
            if not self.printed:
                print(interval)
            self.database.add_candle(
                exchange_id,
                market_id,
                interval['periodName'],
                datetime.fromtimestamp(int(interval['closetime']), tz=timezone.utc),
                Decimal(interval['ohlc']['openStr']),
                Decimal(interval['ohlc']['highStr']),
                Decimal(interval['ohlc']['lowStr']),
                Decimal(interval['ohlc']['closeStr']),
                Decimal(interval['volumeBaseStr']),
                Decimal(interval['volumeQuoteStr'])
                )
        self.printed = True


    def connect(self):
        # Start receiving
        cw.stream.connect()

    def disconnect(self):
        # Call disconnect to close the stream connection
        cw.stream.disconnect()

class CryptoWatchDatabase(object):

    def __init__(self,
                 db_file='databases/cryptowatch_coinbase_pro_candles.db',
                 pull_history=False):
        self.db_file = db_file
        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter("decimal", convert_decimal)
        sqlite3.register_adapter(datetime, adapt_iso_datetime)
        sqlite3.register_converter("iso_datetime", convert_iso_datetime)
        if not self.candle_table_exists():
            self.create_candle_table()
            self.populate_historical_data()
        elif pull_history:
            self.populate_historical_data()

    def conn(self):
        conn = sqlite3.connect(self.db_file,
                                detect_types=sqlite3.PARSE_DECLTYPES |
                                sqlite3.PARSE_COLNAMES)
        return conn

    def candle_table_exists(self):

        table_exists = False

        conn = self.conn()
        c = conn.cursor()

        c.execute('''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='candles';''')

        if c.fetchone()[0] == 1:
            table_exists = True

        conn.commit()
        conn.close()

        return table_exists

    def create_candle_table(self):

        conn = self.conn()
        c = conn.cursor()

        c.execute('''CREATE TABLE "candles" (
                    "exchange_id"    integer,
                    "market_id"	integer,
                    "period"	text,
                    "timestamp"	iso_datetime,
                    "open"	decimal,
                    "high"	decimal,
                    "low"	decimal,
                    "close"	decimal,
                    "volume"	decimal,
                    "quote_volume" decimal
                )''')

        c.execute('''CREATE UNIQUE INDEX ix_market_prod_gran_ts ON candles(exchange_id, market_id, period, timestamp)''')

        conn.commit()

        conn.close()

    def populate_historical_data_period(self, market_id, period, data):

        exchange_id = 2

        for candle in data:
            self.add_candle(exchange_id,
                            market_id,
                            period,
                            datetime.fromtimestamp(int(candle[0]), tz=timezone.utc),
                            Decimal(candle[1]),
                            Decimal(candle[2]),
                            Decimal(candle[3]),
                            Decimal(candle[4]),
                            Decimal(candle[5]),
                            Decimal(candle[6])
                            )

    def populate_historical_data(self):

        exchange = "coinbase-pro"
        exchange_id = 2

        for market_id, pair in coinbase_pro_asset_ids.items():
            print(f"{exchange}:{pair}")
            response = cw.markets.get(f"{exchange}:{pair}", ohlc=True)
            self.populate_historical_data_period(market_id, "60", response.of_1m)
            self.populate_historical_data_period(market_id, "180", response.of_3m)
            self.populate_historical_data_period(market_id, "300", response.of_5m)
            self.populate_historical_data_period(market_id, "900", response.of_15m)
            self.populate_historical_data_period(market_id, "1800", response.of_30m)
            self.populate_historical_data_period(market_id, "3600", response.of_1h)
            self.populate_historical_data_period(market_id, "7200", response.of_2h)
            self.populate_historical_data_period(market_id, "14400", response.of_4h)
            self.populate_historical_data_period(market_id, "21600", response.of_6h)
            self.populate_historical_data_period(market_id, "43200", response.of_12h)
            self.populate_historical_data_period(market_id, "86400", response.of_1d)
            self.populate_historical_data_period(market_id, "259200", response.of_3d)
            self.populate_historical_data_period(market_id, "604800", response.of_1w)
            self.populate_historical_data_period(market_id, "604800_Monday", response.of_1w_monday)
            break

    def add_candle(self,
                   exchange_id,
                   market_id,
                   period,
                   timestamp,
                   open,
                   high,
                   low,
                   close,
                   volume,
                   quote_volume):

        conn = self.conn()
        c = conn.cursor()

        c.execute('''REPLACE INTO candles VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
                                                                            exchange_id,
                                                                            market_id,
                                                                            period,
                                                                            timestamp,
                                                                            open,
                                                                            high,
                                                                            low,
                                                                            close,
                                                                            volume,
                                                                            quote_volume))

        conn.commit()
        conn.close()

    def get_dataframe(self, exchange_id, market_id_list, period, max_count=300):

        conn = self.conn()
        c = conn.cursor()

        dataframes = []

        cols = ['period', 'timestamp', 'open', 'high', 'low', 'close', 'volume']

        for market_id in market_id_list:

            c.execute('''SELECT %s FROM candles where exchange_id = ? AND market_id = ? AND period = ? ORDER BY timestamp''' % (', '.join(cols),), (exchange_id, product_id, granularity))

            candle_data = c.fetchall()

            if max_count is not None and len(candle_data) > max_count:
                candle_data = candle_data[-max_count:]

            dataframes.append(pd.DataFrame(candle_data, columns=cols))

        candle_dfs = pd.concat(
            dataframes,
            axis=1,
            keys=[product_id for product_id in product_id_list]
        )

        conn.commit()
        conn.close()

        return candle_dfs

class CryptoWatchCoinbaseProDataSource(DataSource):

    def __init__(self):
        self.data_source = CryptoWatchDatabase()

    def get_dataframe(self, product_id_list, granularity, max_count=300):
        return self.data_source.get_dataframe(product_id_list, granularity)
