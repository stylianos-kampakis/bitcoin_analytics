from bcclass import bcData
import pandas as pd
from numpy import ravel as ravel
from sklearn import cross_validation
import sklearn
from sklearn import linear_model
import numpy
from random import randrange


import numpy
def ccc(x,y):
    """Calculates the concordance correlation coefficient"""
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



def rollingError(model,ins,outs,prediction_span,debug=True):
    """Predicts the next 'prediction_span' data points and then calculates the error."""
    length_input=len(ins)    
    res=numpy.ones(prediction_span)
    ins=ins[::-1]
    outs=outs[::-1]
    model.fit(ins[0:length_input-prediction_span],numpy.ravel(outs[0:length_input-prediction_span]))
        
    #make the predictions
    dummy=ins[length_input-prediction_span]    
    for i in range(0,prediction_span):
        res[i]=model.predict(dummy)
        dummy=dummy.tolist()
        dummy.pop(0)
        dummy.append(res[i])
        dummy=numpy.asarray(dummy)
        
    if debug:
        print(res)
        print(outs[length_input-prediction_span:length_input])  
        print(outs[length_input-prediction_span:length_input]-res)
    
    #calculate errors
    ccc_er=ccc(res,outs[length_input-prediction_span:length_input])
    abs_er=numpy.mean(abs(res-outs[length_input-prediction_span:length_input]))
        
    
    return ccc_er,abs_er,res,outs[length_input-prediction_span:length_input]
    
    
    
def CV_rollingError(model,dataset_in,dataset_targets,lags=10,number_of_inputs=10,prediction_span=10,nfolds=10,debug=True):

    ccc_error=numpy.ones(nfolds)  
    abs_error=numpy.ones(nfolds)
    
    for k in range(0,nfolds):
        if debug:
            print("fold number:" + str(k))
        #create a random fold by finding a cut-off point in the dataset
        split_point=randrange(lags+number_of_inputs,len(dataset_in)-prediction_span)
        start_point=split_point-number_of_inputs
        
        ins=dataset_in[start_point:split_point+prediction_span]
        outs=dataset_targets[start_point:split_point+prediction_span]
        
        ccc_error[k],abs_error[k],x,y=rollingError(model=model,ins=ins,outs=outs,prediction_span=prediction_span,debug=debug)
        
        
        
    return ccc_error,abs_error
    
