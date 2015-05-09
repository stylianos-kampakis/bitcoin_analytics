# bitcoin_analytics
Some modules in python to help with the analysis of bitcoin data.

*queries.txt
SQL query for creating the tables required to store the bitcoin data.

*retrieve and tickerreader
these are modules used to read data and store it in the database. In the original version of this system, Google app engine
was used in order to store the data

*bcclass and bcclass_notepad
the main class of this module, used to retrieve and visualize data

*analysis
an early version implementation of some some simple metrics for forecasting

*predict_bid and predict_bid_many_exchange_input
some examples for predicting future bitcoin prices using scikit-learn

*graphs.r
some graphs in R, using ggplot2, for visualizing the bitcoin spread between different exchanges

*tradebot
early version of a bot used for algorithmic trading

*api_something
classes used for accessing the APIs of various exchanges

