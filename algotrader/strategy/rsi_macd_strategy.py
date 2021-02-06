from talib import MACD, RSI
from .strategy import Strategy

NO_POSITION = 0
RSI_OVERSOLD = 1
BUY_INDICATED = 2
BUY = 3
POSITION_HELD = 4
RSI_OVERBOUGHT = 5
SELL_INDICATED = 6
SELL = 7

OVERSOLD_THRESHOLD = 30
OVERBOUGHT_THRESHOLD = 70


class Trade(object):

    def __init__(self, product_id, ts, price, quantity, value):
        self.product_id = product_id
        self.ts = ts
        self.price = price
        self.quantity = quantity
        self.value = value


class RsiMacdStrategy(Strategy):

    def __init__(self,
                 fast_period=12,
                 slow_period=26,
                 signal_period=9):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.states = {}

    def next_state(self, product_id, rsi, macd):

        curr_state = self.states[product_id]
        next_state = curr_state

        if curr_state == NO_POSITION:
            if rsi <= OVERSOLD_THRESHOLD:
                next_state = RSI_OVERSOLD
        elif curr_state == RSI_OVERSOLD:
            if rsi > OVERSOLD_THRESHOLD:
                next_state = BUY_INDICATED
        elif curr_state == BUY_INDICATED:
            if macd > 0:
                next_state = BUY
        elif curr_state == POSITION_HELD:
            if rsi >= OVERBOUGHT_THRESHOLD:
                next_state = RSI_OVERBOUGHT
        elif curr_state == RSI_OVERBOUGHT:
            if rsi < OVERBOUGHT_THRESHOLD:
                next_state = SELL_INDICATED
        elif curr_state == SELL_INDICATED:
            if macd < 0:
                next_state == SELL
        else:
            print(f"Unexpected state {curr_state}")

        return next_state

    def execute(self, env, df):
        '''
        Execute a single iteration of algorithmic trading
        '''
        last_row_idx = len(df)-1

        for product_id in env.product_id_list:

            product_df = df[product_id]

            rsi = RSI(product_df.close)

            macd, macd_signal, macd_hist = MACD(product_df.close,
                                                fastperiod=self.fast_period,
                                                slowperiod=self.slow_period,
                                                signalperiod=self.signal_period)

            if self.states.get(product_id) is None:
                self.states[product_id] = NO_POSITION

            next_state = self.next_state(product_id, rsi[last_row_idx], macd[last_row_idx])

            if self.states[product_id] != next_state:
                if next_state == BUY:
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
                    self.states[product_id] = POSITION_HELD
                elif next_state == SELL:
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
                    self.states[product_id] = NO_POSITION
                else:
                    self.states[product_id] = next_state
