from cbpro_candles.candle_db import CoinbaseProCandleDatabase
from ..environments.environment import (
    Environment,
    BuyLimitOrder,
    BuyMarketOrder,
    SellLimitOrder,
    SellMarketOrder,
    Position,
)


class BacktestReport(object):

    def __init__(self,
                 product_id_list,
                 granularity,
                 start_balance,
                 end_cash_balance,
                 end_asset_balance,
                 fees,
                 total_balance,
                 profit,
                 buy_count,
                 sell_count,
                 positive_trade_count,
                 negative_trade_count,
                 outstanding_positions):
        self.product_id_list = product_id_list
        self.granularity = granularity
        self.start_balance = start_balance
        self.end_cash_balance = end_cash_balance
        self.end_asset_balance = end_asset_balance
        self.fees = fees
        self.total_balance = total_balance
        self.profit = profit
        self.buy_count = buy_count
        self.sell_count = sell_count
        self.positive_trade_count = positive_trade_count
        self.negative_trade_count = negative_trade_count
        self.outstanding_positions = outstanding_positions

    @classmethod
    def from_bt_env(cls, bt_env):
        total_balance = bt_env.balance + bt_env.get_asset_balance() - bt_env.fees
        profit = total_balance-bt_env.start_balance

        return BacktestReport(bt_env.product_id_list,
                              bt_env.granularity,
                              bt_env.start_balance,
                              bt_env.balance,
                              bt_env.get_asset_balance(),
                              bt_env.fees,
                              total_balance,
                              profit,
                              bt_env.buy_count,
                              bt_env.sell_count,
                              bt_env.positive_trade_count,
                              bt_env.negative_trade_count,
                              bt_env.positions)

    def get_current_holdings(self):
        return ''.join([f"\n\t\t{position.product_id} - {position.quantity}" for position in self.outstanding_positions])

    def get_products_str(self):
        return ', '.join([product_id for product_id in self.product_id_list])

    def __str__(self):
        portfolio_gain_percent = round(100*self.profit/self.start_balance, 2)
        if self.positive_trade_count == 0 and self.negative_trade_count == 0:
            positive_trade_percent = '-'
        else:
            positive_trade_percent = round(100*self.positive_trade_count/(self.positive_trade_count+self.negative_trade_count), 2)
        return (f"\nResults for backtest for {self.get_products_str()} at {self.granularity} "
                f"second intervals:\n\tCurrent holdings: "
                f"{self.get_current_holdings()}\n\tStart balance: "
                f"{self.start_balance}\n\tEnd cash balance: "
                f"{self.end_cash_balance}\n\tEnd asset balance: "
                f"{self.end_asset_balance}\n\tFees: "
                f"{self.fees}\n\tEnd total balance "
                f"{self.total_balance}\n\tProfit: "
                f"{self.profit} ({portfolio_gain_percent}%)\n\t+/- trades: "
                f"{self.positive_trade_count}/{self.negative_trade_count} "
                f"({positive_trade_percent}%)\n"
                )


class BacktesterEnv(Environment):

    def __init__(self,
                 data_source,
                 product_id_list,
                 granularity,
                 balance=10000):
        super().__init__(data_source,
                         product_id_list,
                         granularity,
                         balance)
        self.start_balance = balance
        self.end_balance = balance

    def get_dataframe(self):
        return self.data_source.get_dataframe(self.product_id_list, self.granularity)

    def get_last_price(self, product_id):
        df = self.get_dataframe()
        last_row_idx = len(df)-1
        return df[product_id].close[last_row_idx]

    def get_price(self, product_id, ts):
        product_df = self.get_dataframe()[product_id]
        price = product_df.loc[product_df['timestamp'] == ts].iloc[0].close
        return price

    def update_pending_orders(self, df):
        still_pending_buys = []
        still_pending_sells = []
        last_row_idx = len(df)-1

        for order in self.pending_buys:
            product_id = order.product_id
            if type(order) == BuyLimitOrder:
                if df[product_id].close[last_row_idx] >= order.price:
                    p = Position(product_id,
                             order.size,
                             df[product_id].timestamp[last_row_idx],
                             order.price)
                else:
                    still_pending_buys.append(order)
        self.pending_buys = still_pending_buys

        for order in self.pending_sells:
            product_id = order.product_id
            if type(order) == SellLimitOrder:
                if df[product_id].close[last_row_idx] <= order.price:
                    positions = self.find_positions(lambda x: x.product_id == product_id)
                    for position in positions:
                        self.sell_position(position, order.price)
                else:
                    still_pending_sells.append(order)

    def process_buy_order(self, buy_order):
        if type(buy_order) == BuyLimitOrder:
            self.pending_buys.append(buy_order)
        elif type(buy_order) == BuyMarketOrder:
            quantity = buy_order.quantity
            price = self.get_price(buy_order.product_id, buy_order.ts)
            if buy_order.quantity is None:
                quantity = buy_order.funds/price
            position = Position(buy_order.product_id,
                                quantity,
                                buy_order.ts,
                                price)
            self.balance -= (quantity*price)
            self.positions.append(position)
            self.buy_count += 1
            print(f"Bought {position.quantity} shares of {position.product_id} at "
                  f"{position.ts} for {position.price} a share (for a total of "
                  f"{self.balance})")

    def process_sell_order(self, sell_order):
        if type(sell_order) == SellLimitOrder:
            self.pending_sells.append(sell_order)
        elif type(sell_order) == SellMarketOrder:
            for position in self.find_positions(lambda x: x.product_id == sell_order.product_id):
                sell_price = self.get_price(sell_order.product_id, sell_order.ts)
                sell_value = position.quantity*sell_price
                profit = sell_value-position.value
                if profit >= 0:
                    self.positive_trade_count += 1
                else:
                    self.negative_trade_count += 1
                self.sell_position(position, sell_price)
                self.sell_count += 1
                print(f"Sold {position.quantity} shares of "
                      f"{position.product_id} at {position.ts} "
                      f"for {sell_price} a share (for a total "
                      f"of {sell_value}, profit of "
                      f"{profit}")
                self.positions.remove(position)

    def add_sell_value(self, value):
        if value >= 0:
            self.add_positive_sell()
        else:
            self.add_negative_sell()

    def get_asset_balance(self):
        asset_balance = 0
        df = self.get_dataframe()
        for position in self.positions:
            current_price = self.get_dataframe()[position.product_id].close[len(df)-1]
            asset_balance += current_price*position.quantity
        return round(asset_balance, 2)

    def generate_backtest_report(self, graph_path=None):
        return BacktestReport.from_bt_env(self)
