import requests
import json
import hashlib
import hmac
import time
import base64
from apitemplate import apiTemplate

class api_bitfinex(apiTemplate):
     

    def __init__(self,api_key,api_secret,wait_for_nonce=False):
        self.__api_key = api_key
        self.__api_secret = api_secret
        self.__wait_for_nonce = wait_for_nonce
        

    def __nonce(self):
        if self.__wait_for_nonce: time.sleep(1)
        self.__nonce_v = str(int(time.time()*1000000))

    def __signature(self, message):
        return hmac.new(self.__api_secret,message, digestmod=hashlib.sha384).hexdigest()
    
    def __api_call(self,method,params,options={}):
      self.__nonce()
      params['request'] = method
      params['nonce'] = str(self.__nonce_v)       
      params['options']=options
     
      
      params = json.dumps(params,encoding="utf-8")

      params=base64.b64encode(params)
      
      headers = {
                      "X-BFX-APIKEY" : self.__api_key,
                      "X-BFX-PAYLOAD": params,
                      "X-BFX-SIGNATURE" : self.__signature(params)}
                      
      
      r = requests.post("https://api.bitfinex.com"+method, data={}, headers=headers)
      response = r.content
      data = json.loads(response)
      return data
  
    def getBalance(self,coin):
        if coin=='usd':
            return float(self.__api_call(method='/v1/balances',params={})[0]['available'])
        if coin=="btc":
            return float(self.__api_call(method='/v1/balances',params={})[1]['available'])

      
    def getTradingFees(self):
        """The bitfinex fees are split in taker and maker. We return taker which is the worst case scenario."""
        #we multiply by 0.01 because the fees are returned in percentage %
        return float(self.__api_call(method='/v1/account_infos',params={})[0]['fees'][0]['taker_fees'])*0.01
        
    def getWithdrawalFees(self):
        return 0.0
      
    def Trade(self,ttype, trate, tamount, tpair='btcusd'):
        if ttype=='buy':
            return self.__api_call(method='/order/new',params={'symbol':tpair,'amount':tamount,'price':trate,
            'exchange':bitfinex,'side':'buy','type':'exchange market'})
        elif ttype=='sell':
            return self.__api_call(method='/order/new',params={'symbol':tpair,'amount':tamount,'price':trate,
            'exchange':bitfinex,'side':'sell','type':'exchange market'})
            
    
    def getUSDtoBankWithdrawalFees(self,amount):
        """Last time checked: September 2, 2014. Best choice was wire transfer"""
        if amount<20:
            return 20.0
        else:
            return amount*0.001
              
  