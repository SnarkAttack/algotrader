#!python3

from algotrader.backtest.backtest import Backtester
from algotrader.environments.backtest_environment import BacktesterEnv
from algotrader.strategy.macd_strategy import MacdStrategy
from algotrader.strategy.rsi_macd_strategy import RsiMacdStrategy
from algotrader.data_sources.coinbase_pro_data_sources import CoinbaseProDatabase
import argparse

db_file = "../cbpro_candles/candles_test.db"

PRODUCTS = sorted([
    "BTC-USD",
    "ETH-USD",
    "LTC-USD",
    "BCH-USD",
    "EOS-USD",
    "DASH-USD",
    "OXT-USD",
    "MKR-USD",
    "XLM-USD",
    "ATOM-USD",
    "XTZ-USD",
    "ETC-USD",
    "OMG-USD",
    "ZEC-USD",
    "LINK-USD",
    "REP-USD",
    "ZRX-USD",
    "ALGO-USD",
    "DAI-USD",
    "KNC-USD",
    "COMP-USD",
    "BAND-USD",
    "NMR-USD",
    "CGLD-USD",
    "UMA-USD",
    "LRC-USD",
    "YFI-USD",
    "UNI-USD",
    "REN-USD",
    "BAL-USD",
    "WBTC-USD",
    "NU-USD",
    "FIL-USD",
    "AAVE-USD",
    "GRT-USD",
    "BNT-USD",
    "SNX-USD"
])

GRANULARITIES = [60, 300, 900, 3600, 21600, 86400]

def main():

    parser = argparse.ArgumentParser()

    list_of_strategies = ['macd', 'rsi_macd']

    parser.add_argument('-s', '--strategy',
                        help='Type of strategy to use',
                        choices=list_of_strategies,
                        default='macd')
    parser.add_argument('-p', '--products',
                        help='Comma separated list of products to test',
                        choices=PRODUCTS,
                        default='ETH-USD')
    parser.add_argument('-g', '--granularity',
                        help='Granularity (in seconds) to backtest on',
                        type=int,
                        choices=GRANULARITIES,
                        default=3600)
    parser.add_argument('-b', '--start_balance',
                        type=int,
                        help='Starting balance to use',
                        default=10000)
    parser.add_argument('-c', '--candles_count',
                        type=int,
                        help='Number of candles to use in calculations',
                        default=300)

    args = parser.parse_args()

    products = args.products.split(',')

    backtester = Backtester()

    data_source = CoinbaseProDatabase(db_file)

    back_env = BacktesterEnv(data_source,
                             products,
                             args.granularity,
                             balance=args.start_balance)

    if args.strategy == 'macd':
        strategy = MacdStrategy()
    elif args.strategy == 'rsi_macd':
        strategy = RsiMacdStrategy()

    backtester.backtest(back_env, strategy)

if __name__ == "__main__":
    main()
