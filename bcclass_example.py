from bcclass import bcData
import pandas as pd
from numpy import ravel as ravel

pd.set_option('display.height', 500)
pd.set_option('display.width', 5000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns',10)



bcclassExample=bcData()
bcclassExample.dbname="somename"
bcclassExample.dbusername=="someuser"
bcclassExample.dbserver="someserver"
bcclassExample.dbpassword="password"

#Standard use case. Set exchanges
print("<<EXAMPLE>>:Standard use case. Set exchanges")
bcclassExample.Exchanges_setAllExchanges()
bcclassExample.exchanges=["btc-e.com","bitstamp.com","hitbtc.com","bitfinex.com"]
bcclassExample.exchanges


#Test spread queries and plotting.
print("<<EXAMPLE>>:Test standard queries and plotting")
nspread=bcclassExample.MySQL_NCases_getNSpread(number=60*6)
bcclassExample.Descriptives_Plotting_plotSingleVariable()
nspread.describe()
#check if we get the same results
bcclassExample.Descriptives_describeCurrentFrame()
bcclassExample.Descriptives_crosstabSpread()
ctar=bcclassExample.Descriptives_crosstabArray()

print("<<EXAMPLE>>: Smoothed spread")
nspread=bcclassExample.MySQL_NCases_getNSpread(60)
bcclassExample.Analysis_smoothFrame(smoothing=10)
bcclassExample.Descriptives_Plotting_plotSingleVariable(use_smoothed_frame=True)


#check the error handling of the query mechanism
print("<<EXAMPLE>>:check the error handling of the query mechanism")
bcclassExample._bcData__dbQuery("oasdadsasd")


#Get the last N cases, smooth them and then plot them
print("<<EXAMPLE>>:Get the last N cases, smooth them and then plot them")
dfnbids=bcclassExample.MySQL_NCases_getLastNRecords(120)
smoothed=bcclassExample.Analysis_smoothFrame(smoothing=10)
bcclassExample.Descriptives_Plotting_plotGroupedFrame(use_smoothed_frame=True)
bcclassExample.Descriptives_describeGroupedFrame()
#check if we get the same results with both describe functions
dfnbids.describe()

#Run more select queries, and also test the missing minutes function
print("<<EXAMPLE>>:Run more select queries, and also test the missing minutes function")
maxnbids=bcclassExample.MySQL_NCases_getNMaxMinuteBids(60)
mins=bcclassExample.Descriptives_getMissingMinutes()
minnasks=bcclassExample.MySQL_NCases_getNMinMinuteAsks(60)

dfnbids=bcclassExample.MySQL_NCases_getLastNRecords(60)

#Checking compatibility with sci-kit learn
print("<<EXAMPLE>>:Checking compatibility with sci-kit learn")
rec3=bcclassExample.Analysis_getInputAndTargets(target_vars=['bid'],exclude_target_exchanges=bcclassExample.Analysis_targetExchange('btc-e.com'),lags=2)
rec4=bcclassExample.Analysis_getInputAndTargets(target_vars=['ask'],exclude_target_exchanges=bcclassExample.Analysis_targetExchange('bitstamp.com'),lags=7)
rec5=bcclassExample.Analysis_getInputAndTargets(target_vars=['volume'],lags=7,exclude_target_exchanges=bcclassExample.Analysis_targetExchange('bitstamp.com'))
rec6=bcclassExample.Analysis_getInputAndTargets(target_vars=['bid'],lags=7,exclude_target_exchanges=bcclassExample.Analysis_targetExchange('bitstamp.com'))
rec7=bcclassExample.Analysis_getInputAndTargets(target_vars=['volume'],lags=1,exclude_target_exchanges=bcclassExample.Analysis_targetExchange('bitstamp.com'))
rec8=bcclassExample.Analysis_getInputAndTargets(target_vars=['volume'],lags=1,exclude_target_exchanges=bcclassExample.Analysis_targetExchange('bitstamp.com'),combine_groups=False)
##
from sklearn import svm
clf = svm.SVC(gamma=0.001, C=100.)

clf.fit(rec3['input'], numpy.ravel(rec3['target']))
clf.fit(rec4['input'], numpy.ravel(rec4['target']))
clf.fit(rec5['input'], numpy.ravel(rec5['target']))
clf.fit(rec6['input'], numpy.ravel(rec6['target']))
clf.fit(rec7['input'], numpy.ravel(rec7['target']))
#clf.fit(rec8['input'], numpy.ravel(rec8['target']))
##
###Run a mysql query again to make sure that no internal class objects have changed or been messed up
dfnbids=bcclassExample.MySQL_NCases_getLastNRecords(6)
bcclassExample.Descriptives_Plotting_plotGroupedFrame()

#EXAMPLEs for foreign dataframes. This is the case where a function is called on some other frame, and not the current_frame
print("<<EXAMPLE>>: Foreign frames")
#get N last spread, smooth and plot
bcclassExample=bcData()
bcclassExample.Exchanges_setUSDExchanges()
nspread2=bcclassExample.MySQL_NCases_getNSpread(60)
bcclassExample.Descriptives_Plotting_plotSingleVariable()

bcclassExample.Descriptives_Plotting_plotSingleVariable(dfs=[nspread2])

bcclassExample.Descriptives_Plotting_plotSingleVariable(dfs=[nspread2],use_smoothed_frame=True)


smoothedspread=bcclassExample.Analysis_smoothFrame(nspread2,smoothing=5)
bcclassExample.Descriptives_Plotting_plotSingleVariable(dfs=[nspread2,smoothedspread],use_smoothed_frame=True)

#get last N records and then plot
bcclassExample=bcData()
bcclassExample.Exchanges_setUSDExchanges()
nrecs=bcclassExample.MySQL_NCases_getLastNRecords(60)
bcclassExample.Descriptives_Plotting_plotGroupedFrame(df=[nrecs])

smoothed=bcclassExample.Analysis_smoothFrame(nrecs,smoothing=5)
bcclassExample.Descriptives_Plotting_plotGroupedFrame(df=[nrecs,smoothed])


print("<<EXAMPLE>>: Testing smoothed")


smoothed=bcclassExample.Analysis_smoothFrame(nrecs,smoothing=5)
smoothed=bcclassExample.Pandas_wrapper(smoothed,"dropna")
rec=bcclassExample.Analysis_getInputAndTargets(df=smoothed,exclude_target_exchanges=bcclassExample.Analysis_targetExchange('bitfinex.com'),target_vars=['bid'])

from sklearn import svm
clf = svm.SVC(gamma=0.001, C=100.)

clf.fit(rec['input'], numpy.ravel(rec['target']))


bcclassExample.Exchanges_setUSDExchanges()
print("<<EXAMPLE>>: Testing bidaspspread conversion to dataset for analysis with sci-kit learn")
nspread2=bcclassExample.MySQL_NCases_getNSpread(60)


#smoothed=smoothed.dropna()
rec=bcclassExample.Analysis_getInputAndTargets(nspread2,input_vars=['bidaskspread'],target_vars=['bidaskspread'],lags=7,exclude_target_exchanges=bcclassExample.Analysis_targetExchange('bitfinex.com'))
#bcclassExample.Analysis_getInputAndTargets()

from sklearn.tree import DecisionTreeRegressor
# Fit regression model
clf_1 = DecisionTreeRegressor(max_depth=2)
clf_1.fit(rec['input'], numpy.ravel(rec['target']))
y_1 = clf_1.predict(rec['input'])

print("<<EXAMPLE>>:Get last n records, smooth and try to predict multiple variables at once")
nspread2=bcclassExample.MySQL_NCases_getLastNRecords(60)
smoothed=bcclassExample.Analysis_smoothFrame(smoothing=5)
smoothed=bcclassExample.Pandas_wrapper(smoothed,"dropna")
bcclassExample.Pandas_wrapper(smoothed,"describe")
rec=bcclassExample.Analysis_getInputAndTargets(smoothed,lags=7,exclude_target_exchanges=bcclassExample.Analysis_targetExchange('bitfinex.com'))


from sklearn.linear_model import MultiTaskLasso, Lasso
multi_task_lasso = MultiTaskLasso(alpha=1.).fit(rec['input'], rec['target'])

#testing smoothed
#try some crazy value
#smoothed=bcclassExample.Analysis_smoothFrame(nrecs,smoothing=5000000)


nspread2=bcclassExample.MySQL_NCases_getNSpread(60)
smoothed=bcclassExample.Analysis_smoothFrame(smoothing=5)
bcclassExample.Descriptives_Plotting_plotSingleVariable()

print("<<EXAMPLES>>: maxbid and minask plotting")

maxbid=bcclassExample.MySQL_NCases_getNMaxMinuteBids(60)
smoothed=bcclassExample.Analysis_smoothFrame(smoothing=5)
bcclassExample.Descriptives_Plotting_plotSingleVariable()
minask=bcclassExample.MySQL_NCases_getNMinMinuteAsks(60)
smoothed=bcclassExample.Analysis_smoothFrame(smoothing=5)
bcclassExample.Descriptives_Plotting_plotSingleVariable()

recs=bcclassExample.MySQL_NCases_getLastNRecords(60)
bcclassExample.Descriptives_Plotting_plotGroupedFrame()

bcclassExample=bcData()
bcclassExample.Exchanges_setUSDExchanges()
ssp=bcclassExample.MySQL_NCases_getSelectedNSpread(60)
#bcclassExample.Analysis_smoothFrame(smoothing=5)
#bcclassExample.Descriptives_Plotting_plotGroupedFrame()

#bcclassExample.Descriptives_Plotting_plotGroupedFrame(use_smoothed_frame=True)

print("<<EXAMPLE>>: Testing the conversion of SelectedNSpread to data for analysis with sci-kit learn")
rec=bcclassExample.Analysis_getInputAndTargets(exclude_target_exchanges=bcclassExample.Analysis_targetExchange('bitfinex.com'),combine_groups=False)

###time out on the sql server side
###dfbid=bcclassExample.getAllMaxMinuteBidsUSD()
###dfask=bcclassExample.getAllMaxMinuteAskUSD()
##


##
##
###times out on the sql server side
###dfspread=bcclassExample.getAllSpreadUSD()
##
###groups=dfnbids.groupby('exchange')
##
##
