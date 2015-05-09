from api_btc_e import api_btc_e
from api_hitbtc import api_hitbtc
from api_bitfinex import api_bitfinex
from api_bitstamp import api_bitstamp
from tickerreader import tickerReader



class tradeBot:
    """Provides a high level API for interacting with various exchanges. Currently supporting btc-e,
    bitfinex, bitstamp, hitbtc"""
    def __init__(self,):
        self.btce_initialized=False
        self.bitfinex_initialized=False
        self.bitstamp_initialize=False
        self.hitbtc_initialize=False

        self.__bot_btce=None
        self.__bot_bitfinex=None
        self.__bot_bitstamp=None
        self.__bot_hitbtc=None
        
        self.__tickerRead=tickerReader()


    def initBtce(self,api_key,api_secret,wait_for_nonce=True):
        self.__bot_btce=api_btc_e(api_key=api_key,api_secret=api_secret,wait_for_nonce=wait_for_nonce)

    def initHitbtc(self,api_key,api_secret,wait_for_nonce=True):
        self.__bot_hitbtc=api_hitbtc(api_key=api_key,api_secret=api_secret,wait_for_nonce=wait_for_nonce)
        
    def initBitfinex(self,api_key,api_secret,wait_for_nonce=True):
        self.__bot_bitfinex=api_bitfinex(api_key=api_key,api_secret=api_secret,wait_for_nonce=wait_for_nonce)
        
    def initBitstamp(self,api_key,api_secret,clientid,wait_for_nonce=False):
        self.__bot_bitstamp=api_bitstamp(api_key=api_key,api_secret=api_secret,clientid=clientid,wait_for_nonce=wait_for_nonce)


#remains to be tested
    def checkIfActiveOrders(self,exchange,pair):
        """Checks to see if there are any open orders for an exchange"""
        if exchange=="btc-e":
            info=self.__bot_btce.getInfo()
            return info['return']['open_orders']>0
        return True

#remains to be tested
    def cancelAllOrders(self,exchange):
        cancelled_orders=[]
        if exchange=="btc-e":
            if self.checkIfActiveOrders("btc-e","btc_usd"):
                orders=self.__bot_btce.ActiveOrders("btc_usd")
                orders=orders['return']

                for order in orders:
                    self.__bot_btce.CancelOrder(order)
                    cancelled_orders.append(order)
        return cancelled_orders


    def getTradingFees(self,exchange,pair='btc_usd'):
        """Gets the trading fee for a given exchange and pair. Right now pair is implemented through default arguments"""
        if exchange=="btc-e":
            return self.__bot_btce.getTradingFees()
        if exchange=="bitfinex":
            return self.__bot_bitfinex.getTradingFees()
        if exchange=="bitstamp":
            return self.__bot_bitstamp.getTradingFees()
        if exchange=="hitbtc":
            return self.__bot_hitbtc.getTradingFees()
        
    def getWithdrawalFees(self,exchange,pair='btc_usd'):
        """Gets the trading fee for a given exchange and pair. Right now pair is implemented through default arguments"""
        if exchange=="btc-e":
            return self.__bot_btce.getWithdrawalFees()
        if exchange=="bitfinex":
            return self.__bot_bitfinex.getWithdrawalFees()
        if exchange=="bitstamp":
            return self.__bot_bitstamp.getWithdrawalFees()
        if exchange=="hitbtc":
            return self.__bot_hitbtc.getWithdrawalFees()        
            
    def getUSDtoBankWithdrawalFees(self,exchange):
        return True
            
    def getBalance(self,exchange,coin):
        """Gets the current balance for an exchange"""
        if exchange=="btc-e":
            return self.__bot_btce.getBalance(coin=coin)
        if exchange=="bitfinex":
            return self.__bot_bitfinex.getBalance(coin=coin)
        if exchange=="hitbtc":
            return self.__bot_hitbtc.getBalance(coin=coin)
        if exchange=="bitstamp":
            return self.__bot_bitstamp.getBalance(coin=coin)
            
    

    def simulateOrder(self,bid_exchange,ask_exchange,amount):
        """This function gets the current bid and ask prices from two exchanges,
        along witht the trading fees and returns the amount of USD that would have been earned
        or lost if this order was fulfilled at this instant"""
        
        #get bid from bid exchange
        bid=self.getTicker(exchange=bid_exchange,bid_or_ask="bid")
        bidfee=self.getTradingFees(exchange=bid_exchange)
        
        #get ask from ask exchange
        ask=self.getTicker(exchange=ask_exchange,bid_or_ask="ask")
        askfee=self.getTradingFees(exchange=ask_exchange)  
        ask_withdrawal_fee=self.getWithdrawalFees(exchange=ask_exchange)
        
        #calculate the money earned    
        bitcoin_purchased=amount-amount*askfee
        bitcoin_sold=bitcoin_purchased-bitcoin_purchased*bidfee-ask_withdrawal_fee
        money_earned=bitcoin_sold*bid-amount*ask
        
        #total=((amount-amount*askfee)-(amount-amount*askfee)*bidfee)*bid-amount*ask
        return money_earned
        
    def findOptimal(self,bid_exchange,ask_exchange,amount):
        
        #get bid from bid exchange
        bid=self.getTicker(exchange=bid_exchange,bid_or_ask="bid")
        bidfee=self.getTradingFees(exchange=bid_exchange)
        
        #get ask from ask exchange
        ask=self.getTicker(exchange=ask_exchange,bid_or_ask="ask")
        askfee=self.getTradingFees(exchange=ask_exchange)       
        ask_withdrawal_fee=self.getWithdrawalFees(exchange=ask_exchange)
        
        a_coef=((amount-amount*askfee)-(amount-amount*askfee)*bidfee)-ask_withdrawal_fee
        b_coef=amount
        coef=b_coef/a_coef
        
        
        opt_bid=coef*ask+0.01
        opt_ask=(bid-0.01)/coef
        
        return {'desired_min_bid':opt_bid,'desired_max_ask':opt_ask,'current_bid':bid,'current_ask':ask}
        
    def openOrder(self,exchange,buy_or_sell,price):
        """Opens an order"""
    
    def getTicker(self,exchange,bid_or_ask):
        """gets buy and sell for an exchange"""
        if exchange=="btc-e":
            return self.__tickerRead.btce(bid_or_ask)
        if exchange=="bitfinex":
            return self.__tickerRead.bitfinex(bid_or_ask)
        if exchange=="hitbtc":
            return self.__tickerRead.hitbtc(bid_or_ask)
        if exchange=="bitstamp":
            return self.__tickerRead.bitstamp(bid_or_ask)
            
    def Trade(self,exchange,bid_or_ask,rate,amount):
        if exchange=="btc-e":
            return self.__bot_btce.Trade(ttype=bid_or_ask,trate=rate,tamount=amount)
        if exchange=="bitstamp":
            return self.__bot_bitstamp.Trade(ttype=bid_or_ask,trate=rate,tamount=amount)
        if exchange=="bitfinex":
            return self.__bot_bitfinex.Trade(ttype=bid_or_ask,trate=rate,tamount=amount)



