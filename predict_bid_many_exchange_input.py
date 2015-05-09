# -*- coding: utf-8 -*-
"""
Created on Fri Sep 05 15:09:27 2014

@author: stelios
"""

from bcclass import bcData
import pandas as pd
from numpy import ravel as ravel
from sklearn import cross_validation
import sklearn
from sklearn import linear_model


import numpy
def ccc(x,y):
    #in order to fight the problem of 0 variance
    x=x
    correlation=numpy.corrcoef(x,y)[0][1]
    std_x=numpy.std(x)
    std_y=numpy.std(y)
    
    var_x=numpy.var(x)
    var_y=numpy.var(y)
    
    mean_x=numpy.mean(x)
    mean_y=numpy.mean(y)
    
    return (2*correlation*std_x*std_y)/(var_x+var_y+(mean_x-mean_y)**2)



def rollingError(model,ins,outs,left_outs):
    length_input=len(ins)    
    res=numpy.ones(left_outs)
    ins=ins[::-1]
    outs=outs[::-1]
    model.fit(ins[0:length_input-left_outs],numpy.ravel(outs[0:length_input-left_outs]))
        
    #res=model.predict(ins[length_input-left_outs:length_input])
    dummy=ins[length_input-left_outs]    
    for i in range(0,left_outs):
        res[i]=model.predict(dummy)
        dummy=dummy.tolist()
        dummy.pop(0)
        dummy.append(res[i])
        dummy=numpy.asarray(dummy)
    
    print(res)
    print(outs[length_input-left_outs:length_input])  
    print(outs[length_input-left_outs:length_input]-res)
    return ccc(res,outs[length_input-left_outs:length_input]),res,outs[length_input-left_outs:length_input],res-outs[length_input-left_outs:length_input]
    

pd.set_option('display.height', 500)
pd.set_option('display.width', 5000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns',10)

paok=bcData()
paok.Exchanges_setAllExchanges()
paok.exchanges=["btc-e.com","bitstamp.com","hitbtc.com","bitfinex.com"]


print("<<UNIT TEST>>: Testing bidaspspread conversion to dataset for analysis with sci-kit learn")
data=paok.MySQL_NCases_getLastNRecords(number=60*24)

#smoothed=smoothed.dropna()
rec=paok.Analysis_getInputAndTargets(data,input_vars=['bid'],target_vars=['bid'],lags=7,exclude_target_exchanges=paok.Analysis_targetExchange('bitstamp.com'))
smoothed=paok.Analysis_smoothFrame(smoothing=5)
paok.Descriptives_Plotting_plotGroupedFrame()
smoothed=paok.Pandas_wrapper(smoothed,"dropna")
rec=paok.Analysis_getInputAndTargets(smoothed,lags=24,input_vars=['bid'])
#paok.Analysis_getInputAndTargets()

inp=rec['input']
out=ravel(rec['target'])
from sklearn import svm



clf=svm.SVR(C=0.5, cache_size=200, coef0=0.0, degree=3,
epsilon=0.1, gamma=20.1, kernel='rbf', max_iter=-1, probability=False,
random_state=None, shrinking=True, tol=0.001, verbose=False)

clf = linear_model.BayesianRidge(alpha_1=5, alpha_2=0.000001, compute_score=False, copy_X=True,
       fit_intercept=True, lambda_1=0.1, lambda_2=1, n_iter=300,
       normalize=False, tol=0.001, verbose=False)
       
       
prediction_span=12*12
ccc_er,abs_er,res,outs=rollingError(clf,inp,out,prediction_span)

for i in range(0,60*24-prediction_span):
    res=numpy.insert(res,0,numpy.nan)


plt.plot(res)
print(ccc_er)
print(abs_er)
