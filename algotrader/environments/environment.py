from datetime import datetime, timezone
import uuid


class Transaction(object):

    def __init__(self, uid=None):
        if uid is None:
            uid = uuid.uuid4()
        self.uuid = uid


class Position(Transaction):

    def __init__(self, product_id, quantity, ts, price, uuid=None):
        super().__init__(uuid)
        self.product_id = product_id
        self.quantity = quantity
        self.ts = ts
        self.price = price

    @property
    def value(self):
        return self.quantity*self.price


class BuyMarketOrder(Transaction):

    def __init__(self, product_id, ts, quantity=None, funds=None, uuid=None):
        super().__init__(uuid)
        if quantity is None and funds is None:
            raise ValueError("One of size or funds must not be null")
        elif quantity is not None and funds is not None:
            raise ValueError("Only one of size and funds should be non-null")
        self.product_id = product_id
        self.ts = ts
        self.quantity = quantity
        self.funds = funds


class SellMarketOrder(Transaction):

    def __init__(self, product_id, ts, quantity=None, funds=None, uuid=None):
        super().__init__(uuid)
        if quantity is None and funds is None:
            raise ValueError("One of size or funds must not be null")
        elif quantity is not None and funds is not None:
            raise ValueError("Only one of size and funds should be non-null")
        self.product_id = product_id
        self.ts = ts
        self.quantity = quantity
        self.funds = funds


class BuyLimitOrder(Transaction):

    def __init__(self, product_id, ts, price, quantity, cancel_after=None, uuid=None):
        super().__init__(uuid)
        self.product_id = product_id
        self.ts = ts
        self.price = price
        self.quantity = quantity
        self.cancel_after = cancel_after


class SellLimitOrder(Transaction):

    def __init__(self, product_id, ts, price, quantity, cancel_after=None, uuid=None):
        super().__init__(uuid)
        self.product_id = product_id
        self.ts = ts
        self.price = price
        self.quantity = quantity
        self.cancel_after = cancel_after


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
        self.pending_buys = []
        self.pending_sells = []
        self.positions = []
        self.start_time = datetime.now(tz=timezone.utc)

    def find_positions(self, func):
        return [position for position in self.positions if func(position)]

    def update_pending_orders(self):
        pass

    def process_buy_order(self, sell_order):
        self.pending_buys.append(buy_order)

    def process_sell_order(self, trade):
        self.pending_sells.append(sell_order)

    def sell_position(self, pos, price):
        self.balance += (price*pos.quantity)
