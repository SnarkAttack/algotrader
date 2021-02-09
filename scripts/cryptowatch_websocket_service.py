from algotrader.data_sources.cryptowatch import CryptoWatchWebsocket
import argparse

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('--pull-history', dest='pull_history', action='store_true')
    parser.set_defaults(pull_history=False)

    cws = CryptoWatchWebsocket()
    print("Websocket connecting")
    cws.connect(pull_history=args.pull_history)


if __name__ == "__main__":
    main()
