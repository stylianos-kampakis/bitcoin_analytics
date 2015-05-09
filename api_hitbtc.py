# -*- coding: utf-8 -*-
"""
Created on Mon Aug 11 19:37:02 2014

@author: stelios
"""

import requests
import json
import hashlib
import hmac
import time
from apitemplate import apiTemplate

class api_hitbtc(apiTemplate):     

    def __init__(self,api_key,api_secret,wait_for_nonce=False):
        self.__api_key = api_key
        self.__api_secret = api_secret
        self.__wait_for_nonce = wait_for_nonce
        

    def __nonce(self):
        if self.__wait_for_nonce: time.sleep(1)
        self.__nonce_v = str(int(time.time()*100))

    
    def __signature(self, message):
        return hmac.new(self.__api_secret,msg=message, digestmod=hashlib.sha512).hexdigest().lower()
    
    def __api_call(self,method,params):
    #get the nonce
      self.__nonce()
      nonce=self.__nonce_v   
      
      params['nonce'] = nonce      
      params['apikey']=self.__api_key 
      #for the signature inn hitbtc we need to get the uri and the post_data
      url="https://api.hitbtc.com/"+method
      #url_signature=method
      
      uri = requests.get(url, params=params).url
      uri=uri.replace("https://api.hitbtc.com","")
      
      header={ 'X-Signature':self.__signature(uri)}        
      
      r = requests.get(url, params=params,headers=header)
      response = r.content
      data = json.loads(response)
      return data
  
    def getBalance(self,coin):
        if coin=='usd':
            return self.__api_call(method='api/1/payment/balance',params={})['balance'][0]['balance']
        if coin=='btc':
            return self.__api_call(method='api/1/payment/balance',params={})['balance'][2]['balance']
      
    def getTradingFees(self):
        #retrieved from the webpage, fees cannot be retrieved from the API
        return 0.001

    def getWithdrawalFees(self):
        return 0.0
        
    def getUSDtoBankWithdrawalFees(self):
        """Last time checked: September 2, 2014. Flat fee of 9 dollars"""
        return 9.0
        