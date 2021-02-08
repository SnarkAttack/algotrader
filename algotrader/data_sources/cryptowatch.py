import sqlite3
from decimal import Decimal
import pandas as pd
import dateutil.parser
import datetime
import cryptowatch as cw
from google.protobuf.json_format import MessageToJson
import json

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

    def __init__(self):
        # Set your API Key
        cw.api_key = api_key
        # Subscribe to resources (https://docs.cryptowat.ch/websocket-api/data-subscriptions#resources)
        cw.stream.subscriptions = ["exchanges:2:ohlc"]
        cw.stream.on_intervals_update = self.handle_intervals_update
        self.printed = False

    # What to do on each trade update
    def handle_intervals_update(self, interval_update):
        """
            trade_update follows Cryptowatch protocol buffer format:
            https://github.com/cryptowatch/proto/blob/master/public/markets/market.proto
        """
        # market_msg = ">>> Market#{} Exchange#{} Pair#{}: {} New Trades".format(
        #     trade_update.marketUpdate.market.marketId,
        #     trade_update.marketUpdate.market.exchangeId,
        #     trade_update.marketUpdate.market.currencyPairId,
        #     len(trade_update.marketUpdate.tradesUpdate.trades),
        # )
        # print(market_msg)
        # for trade in trade_update.marketUpdate.tradesUpdate.trades:
        #     trade_msg = "\tID:{} TIMESTAMP:{} TIMESTAMPNANO:{} PRICE:{} AMOUNT:{}".format(
        #         trade.externalId,
        #         trade.timestamp,
        #         trade.timestampNano,
        #         trade.priceStr,
        #         trade.amountStr,
        #     )
        #     print(trade_msg)
        json_msg = MessageToJson(interval_update)
        if not self.printed:
            print(interval_update)
            self.printed=True

    def connect(self):
        # Start receiving
        cw.stream.connect()

    def disconnect(self):
        # Call disconnect to close the stream connection
        cw.stream.disconnect()

class Candle(object):
    def __init__(self, 
                 market_id,
                 product_id, 
                 granularity, 
                 timestamp, 
                 open=None,
                 high=None,
                 low=None,
                 close=None,
                 volume=Decimal(0),
                 quote_volume=Decimal(0),
                 trade_count=0,
                 complete=True):
        self.market_id = market_id
        self.product_id = product_id
        self.granularity = granularity
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.quote_volume = quote_volume
        self.trade_count = trade_count
        self.complete = complete

    def update_candle(self, price, volume):
        dec_price = Decimal(price)
        dec_volume = Decimal(volume)
        if self.open is None:
            self.open = dec_price
        if self.high is None or self.high < dec_price:
            self.high = dec_price
        if self.low is None or self.low > dec_price:
            self.low = dec_price
        self.close = dec_price
        self.volume += dec_volume
        self.trade_count += 1

class CryptoWatchDatabase(object):

    def __init__(self, db_file):
        self.db_file = db_file
        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter("decimal", convert_decimal)
        sqlite3.register_adapter(datetime.datetime, adapt_iso_datetime)
        sqlite3.register_converter("iso_datetime", convert_iso_datetime)
        if not self.candle_table_exists():
            self.create_candle_table()

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
                    "market"    text,
                    "product_id"	text,
                    "granularity"	integer,
                    "timestamp"	iso_datetime,
                    "open"	decimal,
                    "high"	decimal,
                    "low"	decimal,
                    "close"	decimal,
                    "volume"	decimal,
                    "quote_volume" decimal,
                    "trade_count"	INTEGER
                )''')

        c.execute('''CREATE UNIQUE INDEX ix_market_prod_gran_ts ON candles(market, product_id, granularity, timestamp)''')

        conn.commit()

        conn.close()

    def add_candle(self, candle):

        conn = self.conn()
        c = conn.cursor()

        c.execute('''INSERT INTO candles VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (candle.market, candle.product_id,
                                                                             candle.granularity,
                                                                             candle.timestamp,
                                                                             candle.open,
                                                                             candle.high,
                                                                             candle.low,
                                                                             candle.close,
                                                                             candle.volume,
                                                                             candle.quote_volume,
                                                                             candle.trade_count))

        conn.commit()
        conn.close()

    def get_dataframe(self, product_id_list, granularity, max_count=300):

        conn = self.conn()
        c = conn.cursor()

        dataframes = []

        cols = ['granularity', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count']

        for product_id in product_id_list:

            c.execute('''SELECT %s FROM candles where product_id = ? AND granularity = ? ORDER BY timestamp''' % (', '.join(cols),), (product_id, granularity))

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



    def get_candles(self, product_id, granularity, max_count=300):

        conn = self.conn()
        c = conn.cursor()

        c.execute('''SELECT * FROM candles where product_id = ? AND granularity = ? ORDER BY timestamp''', (product_id, granularity))

        candle_data = c.fetchall()   

        if max_count is not None and len(candle_data) > max_count:
            candle_data = candle_data[-max_count:]

        candles = [Candle(*list(c)) for c in candle_data]
        
        conn.commit()
        conn.close()

        return candles
