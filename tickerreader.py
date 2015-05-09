import json
import urllib2


class tickerReader:
    
    def bitstamp(self,bid_or_ask):    
        #bitstamp.com
        req = urllib2.Request('https://www.bitstamp.net/api/ticker/', headers={ 'User-Agent': 'Mozilla/5.0' })
        html = urllib2.urlopen(req).read()
        res=json.loads(html)
        if bid_or_ask=='bid':
            return float(res['bid'])
        elif bid_or_ask=='ask':
            return float(res['ask'])


    def bitfinex(self,bid_or_ask):    
        #bitfinex.com
        req = urllib2.Request('https://api.bitfinex.com/v1/pubticker/btcusd', headers={ 'User-Agent': 'Mozilla/5.0' })
        html = urllib2.urlopen(req).read()
        res=json.loads(html)
        if bid_or_ask=='bid':
            return float(res['bid'])
        elif bid_or_ask=='ask':
            return float(res['ask'])
        
    
    def hitbtc(self,bid_or_ask):
        #hitbtc.com
        req = urllib2.Request('http://api.hitbtc.com/api/1/public/BTCUSD/ticker', headers={ 'User-Agent': 'Mozilla/5.0' })
        html = urllib2.urlopen(req).read()
        res=json.loads(html)
        if bid_or_ask=='bid':
            return float(res['bid'])
        elif bid_or_ask=='ask':
            return float(res['ask'])

    
    def btce(self,bid_or_ask):
        #btc-e.com
        req = urllib2.Request('https://btc-e.com/api/2/btc_usd/ticker', headers={ 'User-Agent': 'Mozilla/5.0' })
        html = urllib2.urlopen(req).read()
        res=json.loads(html)
        res=res['ticker']
        if bid_or_ask=='bid':
            return float(res['buy'])
        elif bid_or_ask=='ask':
            return float(res['sell'])
        