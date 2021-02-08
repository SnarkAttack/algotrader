from talib import MACD
from .strategy import Strategy
from ..environments.environment import (
    Environment,
    BuyMarketOrder,
    SellMarketOrder,
    Position,
)

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
                    if len(env.positions) == 0:
                        quantity = env.balance/product_df.close[last_row_idx]
                        ts = product_df.timestamp[last_row_idx]
                        price = product_df.close[last_row_idx]
                        buy_order = BuyMarketOrder(product_id, ts, funds=env.balance)
                        env.process_buy_order(buy_order)
                    else:
                        print("already holding trade")
                elif macd_state == MACD_LOW:
                    # sell
                    if len(env.positions):
                        for position in env.find_positions(lambda x: x.product_id == product_id):
                            ts = product_df.timestamp[last_row_idx]
                            sell_order = SellMarketOrder(
                                product_id,
                                ts,
                                quantity=position.quantity,
                            )
                            env.process_sell_order(sell_order)
                    else:
                        print("did not have active trade")
                self.signals[product_id] = macd_state
