

class Backtester(object):

    def __init__(self):
        pass

    def backtest(self, bt_env, strategy):
        df = bt_env.get_dataframe()
        for i in range(1, len(df)):
            df_step = df.head(i)
            strategy.execute(bt_env, df_step)
        print(bt_env.generate_backtest_report())
