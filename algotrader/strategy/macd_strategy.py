from talib import MACD
from .strategy import Strategy
from ..environments.environment import Trade

MACD_HIGH = 0
MACD_LOW = 1


class MacdStrategy(Strategy):

    def __init__(self,
                 fast_period=12,
                 slow_period=26,
                 signal_period=9):
        super().__init__()
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.signals = {}
        self.trade = None

    def execute(self, env, df):

        last_row_idx = len(df)-1

        for product_id in env.product_id_list:

            product_df = df[product_id]

            macd, macd_signal, macd_hist = MACD(product_df.close,
                                                fastperiod=self.fast_period,
                                                slowperiod=self.slow_period,
                                                signalperiod=self.signal_period)

            macd_state = MACD_HIGH if macd_hist[last_row_idx] > 0 else MACD_LOW

            if self.signals.get(product_id) is None:
                self.signals[product_id] = macd_state
            elif self.signals[product_id] != macd_state:
                if macd_state == MACD_HIGH:
                    # buy
                    if len(env.trades) == 0:
                        quantity = env.balance/product_df.close[last_row_idx]
                        ts = product_df.timestamp[last_row_idx]
                        price = product_df.close[last_row_idx]
                        trade = Trade(
                            product_id,
                            ts,
                            price,
                            quantity,
                            env.balance)
                        print(f"Bought {quantity} shares of {product_id} at "
                              f"{ts} for {price} a share (for a total of "
                              f"{env.balance})")
                        env.trades.append(trade)
                        env.balance = 0
                        env.add_buy()
                    else:
                        print("already holding trade")
                elif macd_state == MACD_LOW:
                    # sell
                    if len(env.trades) > 0:
                        trade = env.trades[0]
                        sell_price = product_df.close[last_row_idx]
                        sell_value = trade.quantity*sell_price
                        sell_ts = product_df.timestamp[last_row_idx]
                        profit = sell_value-trade.value
                        print(f"Sold {trade.quantity} shares of "
                            f"{trade.product_id} at {sell_ts} "
                            f"for {sell_price} a share (for a total "
                            f"of {sell_value}, profit of "
                            f"{profit}")
                        env.balance = sell_value
                        env.trades = []
                        env.add_sell_value(profit)
                    else:
                        print("did not have active trade")
                self.signals[product_id] = macd_state
