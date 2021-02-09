from algotrader.data_sources.cryptowatch import CryptoWatchWebsocket

def main():
    cws = CryptoWatchWebsocket()
    print("Websocket connecting")
    cws.connect()


if __name__ == "__main__":
    main()
