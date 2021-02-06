from datetime import datetime, timezone


class Trade(object):

    def __init__(self, product_id, ts, price, quantity, value):
        self.product_id = product_id
        self.ts = ts
        self.price = price
        self.quantity = quantity
        self.value = value


class Environment(object):

    def __init__(self,
                 data_source,
                 product_id_list,
                 granularity,
                 balance):
        self.data_source = data_source
        self.product_id_list = product_id_list
        self.granularity = granularity
        self.balance = balance
        self.fees = 0
        self.buy_count = 0
        self.sell_count = 0
        self.positive_trade_count = 0
        self.negative_trade_count = 0
        self.trades = []
        self.start_time = datetime.now(tz=tiemzone.utc)