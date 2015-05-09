from bcclass import bcData
import pandas as pd
from numpy import ravel as ravel
from sklearn import cross_validation
import sklearn
from sklearn import linear_model
from analysis import ccc
from analysis import rollingError
import analysis

import numpy


pd.set_option('display.height', 500)
pd.set_option('display.width', 5000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns',10)

paok=bcData()
paok.Exchanges_setAllExchanges()
paok.exchanges=["btc-e.com","bitstamp.com","hitbtc.com","bitfinex.com"]


prediction_span=60*2
lags=12

print("<<EXAMPLE>>: Testing bidaspspread conversion to dataset for analysis with sci-kit learn")
data=paok.MySQL_NCases_getNMaxMinuteBids(exchanges=['bitstamp.com'],number=60*24)

paok.Analysis_getInputAndTargets(data,input_vars=['maxbid'],target_vars=['maxbid'],lags=7,exclude_target_exchanges=paok.Analysis_targetExchange('bitstamp.com'))
smoothed=paok.Analysis_smoothFrame(smoothing=2)
paok.Descriptives_Plotting_plotSingleVariable(use_smoothed_frame=True)
#sined=numpy.sin(smoothed['maxbid'])
#for i in range(0,len(data)-len(smoothed)):
#    sined=numpy.insert(sined,0,numpy.nan)
#sined=sined*numpy.mean(smoothed['maxbid'])/numpy.var(smoothed['maxbid'])+numpy.mean(smoothed['maxbid'])
#
#smoothed['sined']=sined
#plt.plot(sined)
#plt.show()

smoothed=paok.Pandas_wrapper(smoothed,"dropna")
rec=paok.Analysis_getInputAndTargets(smoothed,input_vars=['maxbid'],lags=lags)

inp=rec['input']
out=ravel(rec['target'])

from sklearn.tree import DecisionTreeRegressor
clf1 = linear_model.BayesianRidge(alpha_1=0.00000001, alpha_2=0.00000001, compute_score=False, copy_X=True,
       fit_intercept=True, lambda_1=0.00000001, lambda_2=0.00000001, n_iter=1000,
       normalize=False, tol=0.001, verbose=False)

       
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
clf2 = Pipeline([('poly', PolynomialFeatures(degree=2)),('linear', LinearRegression(fit_intercept=False))])       



from sklearn import svm
clf3=svm.SVR(C=1.0, cache_size=200, coef0=0.0, degree=2,
epsilon=0.001, gamma=1.0, kernel='rbf', max_iter=-1, probability=False,
random_state=None, shrinking=True, tol=0.001, verbose=False)
       
print("rolling error")
ccc_er,abs_er,res,outs=rollingError(clf3,inp,out,prediction_span)


print("getting to cv rolling error")
ccc_erCV,abs_erCV=analysis.CV_rollingError(model=clf3,dataset_in=inp,dataset_targets=out,lags=lags,number_of_inputs=100,prediction_span=prediction_span)

for i in range(0,len(data)-prediction_span):
    res=numpy.insert(res,0,numpy.nan)


plt.plot(res)
print(ccc_erCV)
print(abs_erCV)

import pandas
pandas.tools.plotting.autocorrelation_plot(inp)
