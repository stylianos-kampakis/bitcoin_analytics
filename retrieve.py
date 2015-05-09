import urllib2
import json

import MySQLdb

db = MySQLdb.connect(host="", # your host, usually localhost
                     user="", # your username
                      passwd="", # your password
                      db="") # name of the data base

cur = db.cursor()

#define the store function for sql database
def saver(bid,ask,exchange):
    dummy="""INSERT INTO data VALUES ('%s',%f,%f,CURDATE(),CURTIME()) """ %(exchange,float(bid),float(ask))
    print(dummy)
    cur.execute(dummy)

try:
    #bitstamp.com
    req = urllib2.Request('https://api.bitfinex.com/v1/pubticker/btcusd', headers={ 'User-Agent': 'Mozilla/5.0' })
    html = urllib2.urlopen(req).read()
    res=json.loads(html)
    bid=res['bid']
    ask=res['ask']
    saver(bid,ask,"bitstamp.com")
except:
    print("Error for bitstamp.com")



#kraken.com
try:
    req = urllib2.Request('https://api.kraken.com/0/public/Ticker?pair=XBTUSD', headers={ 'User-Agent': 'Mozilla/5.0' })
    html = urllib2.urlopen(req).read()
    res=json.loads(html)
    res=res['result']
    res=res['XXBTZUSD']
    ask=res['a'][0]
    bid=res['b'][0]
    saver(bid,ask,"kraken.com")
except:
    print("Error for kraken.com")


#bitfinex.com
try:
    req = urllib2.Request('https://api.bitfinex.com/v1/pubticker/btcusd', headers={ 'User-Agent': 'Mozilla/5.0' })
    html = urllib2.urlopen(req).read()
    res=json.loads(html)
    bid=res['bid']
    ask=res['ask']
    saver(bid,ask,"bitfinex.com")
except:
    print("Error for bitfinex.com")

#hitbtc.com
try:
    req = urllib2.Request('http://api.hitbtc.com/api/1/public/BTCUSD/ticker', headers={ 'User-Agent': 'Mozilla/5.0' })
    html = urllib2.urlopen(req).read()
    res=json.loads(html)
    bid=res['bid']
    ask=res['ask']
    saver(bid,ask,"hitbtc.com")
except:
    print("Error for hitbtc.com")


#okcoin.cn
try:
    req = urllib2.Request('https://www.okcoin.cn/api/ticker.do?symbol=btc_cny', headers={ 'User-Agent': 'Mozilla/5.0' })
    html = urllib2.urlopen(req).read()
    res=json.loads(html)
    res=res['ticker']
    bid=res['buy']
    ask=res['sell']
    saver(bid,ask,"okcoin.com")
except:
    print("Error for okcoin.com")

#btc-e.com
try:
    req = urllib2.Request('https://btc-e.com/api/2/btc_usd/ticker', headers={ 'User-Agent': 'Mozilla/5.0' })
    html = urllib2.urlopen(req).read()
    res=json.loads(html)
    res=res['ticker']
    bid=res['buy']
    ask=res['sell']
    saver(bid,ask,"btc-e.com")
except:
    print("Error for btc-e.com")



#btcchina.com
try:
    req = urllib2.Request('https://data.btcchina.com/data/ticker', headers={ 'User-Agent': 'Mozilla/5.0' })
    html = urllib2.urlopen(req).read()
    res=json.loads(html)
    res=res['ticker']
    bid=res['buy']
    ask=res['sell']
    saver(bid,ask,"btcchina.com")
except:
    print("Error for btcchina.com")

#litetree.com
#requires post request
##import urllib
##url = 'https://www.litetree.com/api/1.1/ticker'
##values = {'pair' : 'BTC/USD'}
##
##data = urllib.urlencode(values)
##req = urllib2.Request(url, data, headers={ 'User-Agent': 'Mozilla/5.0' })
##response = urllib2.urlopen(req)
##html = response.read()
##res=json.loads(html)
##res=res['data']
##bid=res['bid']
##ask=res['ask']
##saver(bid,ask,"litetree.com")

#vaultofsatoshi.com
#needs API authentication. need to be verified first.

#atlasats.com
#needs account
