import requests
import json
import hashlib
import hmac
import time
import base64
from apitemplate import apiTemplate

class api_bitstamp(apiTemplate):
     

    def __init__(self,api_key,api_secret,clientid,wait_for_nonce=False):
        self.__api_key = api_key
        self.__api_secret = api_secret
        self.__wait_for_nonce = wait_for_nonce
        self.__clientid=str(clientid)
        

    def __nonce(self):
        if self.__wait_for_nonce: time.sleep(1)
        self.__nonce_v = str(int(time.time()*100000))

    def __signature(self, message):
        return hmac.new(self.__api_secret,msg=message + self.__clientid + self.__api_key, digestmod=hashlib.sha256).hexdigest().upper()
    
    def __api_call(self,method,params):
      self.__nonce()
      nonce=self.__nonce_v   
      params['signature'] = self.__signature(nonce)
      params['nonce'] = nonce      
      params['key']=self.__api_key                
      
      r = requests.post("https://www.bitstamp.net/api/"+method, data=params)
      response = r.content
      data = json.loads(response)
      return data
      
    def Trade(self, ttype, trate, tamount):
        if ttype=='buy':
            return self.__api_call(method='buy/',params={'amount':tamount,'price':trate})
        elif ttype=='sell':
            return self.__api_call(method='sell/',params={'amount':tamount,'price':trate})
  
    def getBalance(self,coin):
        if coin=='btc':
            return float(self.__api_call(method='balance/',params={})['btc_available'])
        if coin=='usd':
            return float(self.__api_call(method='balance/',params={})['usd_available'])
      
    def getTradingFees(self):
        #we multiply by 0.01 because the fees are returned in percentage %
      return float(self.__api_call(method='balance/',params={})['fee'])*0.01
      
    def getWithdrawalFees(self):
        return 0.0
      

    def getUSDtoBankWithdrawalFees(self):
        """Last time checked: September 2, 2014. Once converted to euro the fee is 0.9 euros, so $1.2 approx"""
        return 1.2
              
  
  