from talib import RSI, BBANDS
from .strategy import Strategy
from ..environments.environment import (
    Environment,
    BuyMarketOrder,
    SellMarketOrder,
    Position,
)
from datetime import datetime, timezone

RSI_UPPER = 70
RSI_LOWER = 30


class ScoreStrategy(Strategy):

    def __init__(self,
                 ratio=5):
        super().__init__()
        self.ratio = 5

    '''
    All get_score functions are expected to return positive values for buy signals
    and negative values for sell signals, with 0 being allowed if the signal is inconclusive
    '''

    def get_rsi_score(self, product_df):
        rsi = RSI(product_df.close).iloc[-1]
        if rsi >= RSI_UPPER:
            return -1
        elif rsi <= RSI_LOWER:
            return 1
        return 0

    def get_funds_amount(self, env):
        curr_val = env.get_current_value()
        return min(curr_val/self.ratio, env.balance)

    def get_bbands_score(self, product_df):
        upper_df, middle_df, lower_df = BBANDS(product_df.close, 20, 2, 2)
        close = product_df.close.iloc[-1]
        upper = upper_df.iloc[-1]
        middle = middle_df.iloc[-1]
        lower = lower_df.iloc[-1]
        #print(f"Upper: {upper}, Middle: {middle}, Lower: {lower}")
        if close < lower:
            return 1
        elif close > upper:
            return -1
        else:
            return 0

    def execute(self, env, df):
        '''
        Execute a single iteration of algorithmic trading
        '''
        last_row_idx = len(df)-1

        for product_id in env.product_id_list:

            product_score = 0

            product_df = df[product_id]

            ts = product_df.loc[last_row_idx].timestamp

            rsi_score = self.get_rsi_score(product_df)
            bbands_score = self.get_bbands_score(product_df)

            if rsi_score+bbands_score >= 2:
                if len(env.find_positions(lambda x: x.product_id == product_id)) == 0:
                        quantity = env.balance/product_df.close[last_row_idx]
                        ts = product_df.timestamp[last_row_idx]
                        price = product_df.close[last_row_idx]
                        funding_amount = self.get_funds_amount(env)
                        buy_order = BuyMarketOrder(product_id, ts, funds=funding_amount)
                        env.process_buy_order(buy_order)
            elif rsi_score+bbands_score <= -2:
                if len(env.find_positions(lambda x: x.product_id == product_id)) > 0:
                    for position in env.find_positions(lambda x: x.product_id == product_id):
                        ts = product_df.timestamp[last_row_idx]
                        sell_order = SellMarketOrder(
                            product_id,
                            ts,
                            quantity=position.quantity,
                        )
                        env.process_sell_order(sell_order)

