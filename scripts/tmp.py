from coinbase_pro_bot.coinbase_pro_bot import CoinbaseProBot

if __name__ == "__main__":
    cpb = CoinbaseProBot()
    cpb.load_portfolio('3600coinbasebot.key')
    cpb.start_interactive()
    