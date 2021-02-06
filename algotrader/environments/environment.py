from datetime import datetime, timezone
import uuid


class Transaction(object):

    def __init__(self, uuid=None):
        if uuid is None:
            uuid = uuid.uuid4()
        self.uuid = uuid


class Position(Transaction):

    def __init__(self, product_id, quantity, time_acq, price_acq, uuid=None):
        super().__init__(uuid)
        self.product_id = product_id
        self.quantity = quantity
        self.time_acq = time_acq
        self.price_acq = price_acq


class BuyMarketOrder(Transaction):

    def __init__(self, product_id, size=None, funds=None, uuid=None):
        super().__init__(uuid)
        if size is None and funds is None:
            raise ValueError("One of size or funds must not be null")
        elif size is not None and funds is not None:
            raise ValueError("Only one of size and funds should be non-null")
        self.size = size
        self.funds = funds


class SellMarketOrder(Transaction):

    def __init__(self, product_id, size=None, funds=None, uuid=None):
        super().__init__(uuid)
        if size is None and funds is None:
            raise ValueError("One of size or funds must not be null")
        elif size is not None and funds is not None:
            raise ValueError("Only one of size and funds should be non-null")
        self.size = size
        self.funds = funds


class BuyLimitOrder(Transaction):

    def __init__(self, product_id, price, size, cancel_after=None, uuid=None):
        super().__init__(uuid)
        self.price = price
        self.size = size
        self.cancel_after = cancel_after


class SellLimitOrder(Transaction):

    def __init__(self, product_id, price, size, cancel_after=None, uuid=None):
        super().__init__(uuid)
        self.price = price
        self.size = size
        self.cancel_after = cancel_after


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
        self.positions = []
        self.start_time = datetime.now(tz=tiemzone.utc)

    def find_positions(self, func):
        return [position for position in self.positions if func(position)]

    def process_buy_order(self, buy_order):
        self.buy_count += 1

    def process_sell_order(self, trade):
        self.sell_count += 1

    def get_current_trades(self):
        pass