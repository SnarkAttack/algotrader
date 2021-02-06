from cbpro_candles.candle_db import CoinbaseProCandleDatabase
from ..environments.environment import Trade


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
                 outstanding_trades):
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
        self.outstanding_trades = outstanding_trades

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
                              bt_env.trades)

    def get_current_holdings(self):
        return ''.join([f"\n\t\t{trade.product_id} - {trade.quantity}" for trade in self.outstanding_trades])

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


class BacktesterEnv(object):

    def __init__(self,
                 data_source,
                 product_id_list,
                 granularity,
                 start_balance=10000):
        self.data_source = data_source
        self.product_id_list = product_id_list
        self.granularity = granularity
        self.start_balance = start_balance
        self.balance = start_balance
        self.end_balance = start_balance
        self.fees = 0
        self.buy_count = 0
        self.sell_count = 0
        self.positive_trade_count = 0
        self.negative_trade_count = 0
        self.trades = []

    def get_dataframe(self):
        return self.data_source.get_dataframe(self.product_id_list, self.granularity)

    def add_buy(self):
        self.buy_count += 1

    def add_sell(self):
        self.sell_count += 1

    def add_positive_sell(self):
        self.add_sell()
        self.positive_trade_count += 1

    def add_negative_sell(self):
        self.add_sell()
        self.negative_trade_count += 1

    def add_sell_value(self, value):
        if value >= 0:
            self.add_positive_sell()
        else:
            self.add_negative_sell()

    def get_asset_balance(self):
        asset_balance = 0
        df = self.get_dataframe()
        for trade in self.trades:
            current_price = self.get_dataframe()[trade.product_id].close[len(df)-1]
            asset_balance += current_price*trade.quantity
        return round(asset_balance, 2)

    def generate_backtest_report(self, graph_path=None):
        return BacktestReport.from_bt_env(self)
