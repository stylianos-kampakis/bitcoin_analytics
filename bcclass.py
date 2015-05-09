from sqlalchemy import create_engine
from pandas import DataFrame
from pandas import crosstab
from pandas.stats.moments import rolling_mean
import pandas.io.sql as psql
import matplotlib.pyplot as plt
#should get kicked out in final versionf
from numpy import floor
from numpy import array as numpyarray

import random


class bcData:

    def __init__(self):

        self.exchanges=[]
        
        self.dbusername="user"
        self.dbpassword="password"
        self.dbserver="someserver"
        self.dbname="dbname"

        self.current_frame=DataFrame()
        self.smoothed_frame=DataFrame()

        self.last_query=""

        self.__spread="""SELECT maxbids.exchange as maxbidexchange,minasks.exchange as  minaskexchange,date(maxbids.datenow) as datenow,max(maxbids.maxbid) as maxbid,
        min(minasks.minask) as minask,
        (maxbid-minask) as bidaskspread,time(maxbids.hourminute) as hourminute   FROM
        (SELECT exchange,datenow,bid as maxbid,DATE_FORMAT(timenow,'%%k:%%i') as hourminute FROM  data) maxbids
        INNER JOIN
        (SELECT exchange,datenow,ask as minask,DATE_FORMAT(timenow,'%%k:%%i') as hourminute FROM  data) minasks
        ON maxbids.datenow=minasks.datenow AND maxbids.hourminute=minasks.hourminute  AND maxbids.exchange<>minasks.exchange
        GROUP BY datenow DESC, hourminute  DESC"""


        self.__spreadN=[""" INSERT INTO tempdata1<UNION QUERY>;""",
        """ INSERT INTO tempdata2 SELECT * from tempdata1;""",
        """  SELECT maxbids.exchange as maxbidexchange,minasks.exchange as  minaskexchange,date(maxbids.datenow) as datenow,max(maxbids.maxbid) as maxbid,
        min(minasks.minask) as minask,
        (max(maxbids.maxbid)-min(minasks.minask)) as bidaskspread,time(maxbids.hourminute) as hourminute   FROM
        (SELECT exchange,datenow,bid as maxbid,DATE_FORMAT(timenow,'%%k:%%i') as hourminute FROM  tempdata1) maxbids
        INNER JOIN
        (SELECT exchange,datenow,ask as minask,DATE_FORMAT(timenow,'%%k:%%i') as hourminute FROM  tempdata2) minasks
        ON maxbids.datenow=minasks.datenow AND maxbids.hourminute=minasks.hourminute  AND maxbids.exchange<>minasks.exchange
        GROUP BY datenow DESC, hourminute  DESC LIMIT <NUMBER>"""]

        self.__spreadNSelected=[""" INSERT INTO tempdata1<UNION QUERY>;""",
        """ INSERT INTO tempdata2 SELECT * from tempdata1;""",
        """ SELECT td1.exchange as bidexchange,td2.exchange as askexchange,td1.bid,td2.ask,(td1.bid-td2.ask) as bidaskspread,
        td1.volume as bidvolume, td2.volume as askvolume,
        concat(td1.exchange,'-',td2.exchange) as exchange,
        date(td1.datenow) as datenow,time(td1.hourminute) as hourminute   FROM
        (SELECT bid, volume,exchange,datenow,DATE_FORMAT(timenow,'%%k:%%i') as hourminute FROM  tempdata1) td1
        INNER JOIN        (SELECT ask,volume,exchange,datenow,DATE_FORMAT(timenow,'%%k:%%i') as hourminute FROM  tempdata2) td2
        ON td1.datenow=td2.datenow AND td1.hourminute=td2.hourminute  AND td1.exchange<>td2.exchange
        AND td1.exchange IN <BIDEXCHANGES>
        AND td2.exchange IN <ASKEXCHANGES>
        ORDER BY td1.exchange,td2.exchange,datenow DESC, hourminute DESC"""]

        self.__minminuteask="""SELECT d.exchange,d.datenow,r.minask,r.hourminute FROM
        (SELECT datenow,DATE_FORMAT(timenow,'%%k:%%i') as hourminute ,MIN(ask) as minask FROM data GROUP BY datenow,DATE_FORMAT(timenow,'%%k:%%i')) r
        INNER JOIN data d ON d.datenow=r.datenow AND d.ask=r.minask AND
        DATE_FORMAT(d.timenow,'%%k:%%i')=r.hourminute"""

        self.__maxminutebid="""SELECT d.exchange,d.datenow,r.maxbid,r.hourminute FROM
        (SELECT datenow,DATE_FORMAT(timenow,'%%k:%%i') as hourminute ,MAX(bid) as maxbid FROM data GROUP BY datenow,DATE_FORMAT(timenow,'%%k:%%i')) r
        INNER JOIN data d ON d.datenow=r.datenow AND d.bid=r.maxbid AND
        DATE_FORMAT(d.timenow,'%%k:%%i')=r.hourminute"""

#the temporary table is part of the following queries: max n minute bid, max n minute ask, max n spread.
        self.__droptemptable="""DROP TEMPORARY TABLE IF EXISTS tempdata ;"""

        self.__temptable="""CREATE TEMPORARY TABLE tempdata  (

        exchange char(50) NOT NULL,
        bid float,
        ask float,
        volume float,
        datenow date NOT NULL,
        timenow time NOT NULL

        );"""

#the insert into temp table is part of the following queries: max n minute bid, max n minute ask, max n spread.
        self.__insertIntoTempTable="""INSERT INTO tempdata <TABLE>;"""

        self.__temptableForMaxNBidorMinAsk="""CREATE temporary TABLE tempdata2(exchange char(50) NOT NULL, datenow date NOT NULL,<maxbidORminask> float, hourminute time NOT NULL );"""

        self.__selectFinalClauseForMaxNBidandMinNAsk="""SELECT * FROM tempdata2 ORDER BY time(hourminute) DESC LIMIT <NUMBER>"""

        self.__maxminNminuteAction="""
        SELECT exchange,datenow,<MAXMIN>(<ACTION>) as <MAXMIN><ACTION>,DATE_FORMAT(timenow,'%%k:%%i') as hourminute FROM  tempdata
        GROUP BY datenow DESC,DATE_FORMAT(timenow,'%%k:%%i') DESC"""

        self.__getLastNRecords="""(select * from data where exchange = '<EXCHANGE>' order by DATE(datenow) DESC,TIME(timenow) DESC limit <NUMBER>)"""


    def __connectDb(self,server,user,passwd,db):
        try:
            engine=create_engine("""mysql://%s:%s@%s/%s"""%(user,passwd,server,db))
            return engine 
        except Exception, e:
            print(str(e))
            print("Error when trying to connect to the database")
            raise


    def __dbQuery(self,query):
        """Connects to the DB and asks a query. Overwrites the current frame and smoothed frame if query is successful."""

        engine=self.__connectDb(server=self.dbserver,user=self.dbusername,
                             passwd=self.dbpassword,db=self.dbname)
        try:
            #if the query returns none, then don't change the current dataframe.
            df=DataFrame(psql.read_sql_query(query, engine))
            self.current_frame=df
            self.smoothed_frame=DataFrame()
        except Exception,e:
            print(str(e))
            print("Error. Either the query is not correct, or the server timed-out.")
            return False
        
        #the query was executed successfuly
        return True



    def __commitQuery(self,queries):
        """Helper function. Queries that are composed of many subqueries separated with ';'
        cannot be executed by the current MySQLdb version. We have to run db.commit() for each query, before sending the next one.
        This helper function is used for max n minute bids, min n minute asks and n spread.
        """
        try:
            engine=self.__connectDb(server=self.dbserver,user=self.dbusername,
                             passwd=self.dbpassword,db=self.dbname)
            connection = engine.connect()

            for i in range(0,len(queries)):
                res=connection.execute(queries[i])
                
            #copy column names from the query
            columns=connection.execute(queries[i])._metadata.keys

            connection.close()
            rows=res.fetchall()

            if len(rows)==0:
                raise Exception("Nothing was returned. Exiting without altering the current dataframe. Perhaps a time-out occured at the database.")
                return False

            temp = []
            #need to convert to list, otherwise constructor gets confused
            for row in rows:
                temp.append(list(row))

            
            

            df=DataFrame(temp,columns=columns)

            self.current_frame=df
            self.smoothed_frame=DataFrame()
            #the query was executed successfuly
            return True
        except Exception,e:
            print str(e)
            print("An error occured. Please make sure the query is right")
            return False

    def __unionQuery(self,limit,exchanges=[]):
            """Helper function. Returns the last N (where N=limit) records for each exchange.
            It is used by: MySQL_NCases_getNMaxMinuteBids(), MySQL_NCases_getNMinMinuteAsks(), MySQL_NCases_getNMinMinuteAsks(), MySQL_NCases_getLastNRecords()"""
            query=""
            
            if exchanges==[]:
                exchanges=self.exchanges

            for ex in exchanges:
                dummy=self.__getLastNRecords
                dummy=dummy.replace("<EXCHANGE>",ex)
                dummy=dummy.replace("<NUMBER>",str(limit))
                query=query+dummy+"UNION ALL"
            #trim the last UNION ALL
            query=query[:-9]
            return query


    def MySQL_getAllMaxMinuteBids(self,debug=True):
        """Gets the max bid for all exchanges and all records in the database."""
        if debug==True:
            print(self.__maxminutebid)

        if self.__dbQuery(self.__maxminutebid):
            self.last_query="MySQL_getAllMaxMinuteBids"
            df=self.current_frame.copy()
            self.current_frame.grouped=False
            df.grouped=False
            return df

    def MySQL_NCases_getNMaxMinuteBids(self,number=720,exchanges=[],debug=True):
        """Gets the max bid from the last N records. Default is last 12 hours. Arguments:
            number: the number of records to retrieve. The records are in reverse chronological order."""

        if exchanges==[]:
            exchanges=self.exchanges

        if len(exchanges)==0:
            raise Exception("Error: No exchanges were set! Please set some exchanges first before using this function")

        #construct the query
        droptable=self.__droptemptable
        temptablequery=self.__temptable

        inserttemptable=self.__insertIntoTempTable
        inserttemptable=inserttemptable.replace("<TABLE>",self.__unionQuery(number,exchanges=exchanges))

        getmaxbids=self.__maxminNminuteAction.replace("<MAXMIN>","max").replace("<ACTION>","bid")
        #we have to add a semicolon since we are making consecutive queries
        getmaxbids=getmaxbids+";"
        getmaxbids="INSERT INTO tempdata2 " + getmaxbids

        temptable2=self.__temptableForMaxNBidorMinAsk.replace("<maxbidORminask>","maxbid")

        selectstatement=self.__selectFinalClauseForMaxNBidandMinNAsk.replace("<NUMBER>",str(number))


        queries=[droptable,temptablequery,inserttemptable,temptable2,getmaxbids,selectstatement]
        if debug==True:
            dummy=""
            for i in range(0,len(queries)):
                dummy=dummy+queries[i].replace("\n","")
            print(dummy)
        if self.__commitQuery(queries):
            self.last_query="MySQL_NCases_getNMaxMinuteBids"
            df=self.current_frame.copy()
            self.current_frame.grouped=False
            df.grouped=False
            return df

    def MySQL_NCases_getNMinMinuteAsks(self,number=720,exchanges=[],debug=True):
        """Gets the min ask from the last N records. Default is last 12 hours. Arguments:
            number: the number of records to retrieve. The records are in reverse chronological order."""

        if exchanges==[]:
            exchanges=self.exchanges

        if len(exchanges)==0:
            raise Exception("Error: No exchanges were set! Please set some exchanges first before using this function")

        #Construct the query
        droptable=self.__droptemptable
        temptablequery=self.__temptable

        inserttemptable=self.__insertIntoTempTable
        inserttemptable=inserttemptable.replace("<TABLE>",self.__unionQuery(number,exchanges=exchanges))

        getmaxbids=self.__maxminNminuteAction.replace("<MAXMIN>","min").replace("<ACTION>","ask")
        #we have to add a semicolon since we are making consecutive queries
        getmaxbids=getmaxbids+";"
        getmaxbids="INSERT INTO tempdata2 " + getmaxbids

        temptable2=self.__temptableForMaxNBidorMinAsk.replace("<maxbidORminask>","minask")

        selectstatement=self.__selectFinalClauseForMaxNBidandMinNAsk.replace("<NUMBER>",str(number))

        queries=[droptable,temptablequery,inserttemptable,temptable2,getmaxbids,selectstatement]

        if debug==True:
            dummy=""
            for i in range(0,len(queries)):
                dummy=dummy+queries[i].replace("\n","")
            print(dummy)
        #send the query to the database
        if self.__commitQuery(queries):
            self.last_query="MySQL_NCases_getNMinMinuteAsks"
            df=self.current_frame.copy()
            self.current_frame.grouped=False
            df.grouped=False
            return df



    def MySQL_getAllMinMinuteAsk(self,debug=True):
        """Gets the min ask for all exchanges and from all records in the database"""
        if debug==True:
            print(self.__minminuteask)
        if self.__dbQuery(self.__minminuteask):
            self.last_query="MySQL_getAllMinMinuteAsk"
            df=self.current_frame.copy()
            self.current_frame.grouped=False
            df.grouped=False
            return df

    def MySQL_getAllSpread(self,debug=True):
        """Gets the spread from all records and all exchanges in the database"""
        if debug==True:
            print(self.__spread)
        if self.__dbQuery(self.__spread):
            self.last_query="MySQL_getAllSpread"
            df=self.current_frame.copy()
            self.current_frame.grouped=False
            df.grouped=False
            return df

    def MySQL_NCases_getNSpread(self,number=720,exchanges=[],debug=True):
        """Gets the spread from the last N rows. Arguments:
            number: the number of records to retrieve. The records are in reverse chronological order."""
        if exchanges==[]:
            exchanges=self.exchanges

        if len(exchanges)==0:
            raise Exception("Error: No exchanges were set! Please set some exchanges first before using this function")

        #construct the query
        queries=[]
        [queries.append(x) for x in self.__spreadN]

        queries[0]=queries[0].replace("<UNION QUERY>",self.__unionQuery(number,exchanges=exchanges))
        queries[2]=queries[2].replace("<NUMBER>",str(number))


        droptable=self.__droptemptable.replace("tempdata","tempdata2")
        tempquery=self.__temptable.replace("tempdata","tempdata2")
        queries.insert(0,tempquery)
        queries.insert(0,droptable)

        droptable=self.__droptemptable.replace("tempdata","tempdata1")
        tempquery=self.__temptable.replace("tempdata","tempdata1")
        queries.insert(0,tempquery)
        queries.insert(0,droptable)

        if debug==True:
            dummy=""
            for i in range(0,len(queries)):
                dummy=dummy+queries[i].replace("\n","")
            print(dummy)
        #send query to the database
        if self.__commitQuery(queries):
            self.last_query="MySQL_NCases_getNSpread"
            df=self.current_frame.copy()
            self.current_frame.grouped=False
            df.grouped=False
            return df




    def MySQL_NCases_getSelectedNSpread(self,number=720,bidexchanges=[],askexchanges=[],debug=True):
            """Gets the spread from the last N rows for the selected exchanges. It can be used to get
            fewer pairs for the spread than would be obtained by using MySQL_NCases_getNSpread which returns
            all pairs. 
            Arguments:
                number: the number of records to retrieve. The records are in reverse chronological order.
                bidexchanges:the exchanges that will be used for retrieving the bids
                askexchanges:the exchanges that will be used for retrieving the asks"""

            bidexchanges=tuple(bidexchanges)
            askexchanges=tuple(askexchanges)

            if bidexchanges==():
                bidexchanges=tuple(self.exchanges)
            if askexchanges==():
                askexchanges=tuple(self.exchanges)

            if len(bidexchanges)==0 or len(askexchanges)==0:
                raise Exception("Error: No exchanges were set! Please set some exchanges first before using this function")

            #construct the query
            queries=[]
            [queries.append(x) for x in self.__spreadNSelected]

            queries[0]=queries[0].replace("<UNION QUERY>",self.__unionQuery(number))
            queries[2]=queries[2].replace("<BIDEXCHANGES>",str(bidexchanges))
            queries[2]=queries[2].replace("<ASKEXCHANGES>",str(askexchanges))


            droptable=self.__droptemptable.replace("tempdata","tempdata2")
            tempquery=self.__temptable.replace("tempdata","tempdata2")
            queries.insert(0,tempquery)
            queries.insert(0,droptable)

            droptable=self.__droptemptable.replace("tempdata","tempdata1")
            tempquery=self.__temptable.replace("tempdata","tempdata1")
            queries.insert(0,tempquery)
            queries.insert(0,droptable)

            if debug==True:
                dummy=""
                for i in range(0,len(queries)):
                    dummy=dummy+queries[i].replace("\n","")
                print(dummy)
            #send query to the database
            if self.__commitQuery(queries):
                self.last_query="MySQL_NCases_getSelectedNSpread"
                df=self.current_frame.copy()
                self.current_frame.grouped=True
                df.grouped=False
                return df

    def MySQL_NCases_getLastNRecords(self,number=720,exchanges=[],debug=True):
        """Gets the last N records (bids, asks and volume) for each exchange from the DB. Arguments:
            number: the number of records to retrieve. The records are in reverse chronological order."""
        if exchanges==[]:
            exchanges=self.exchanges

        if len(exchanges)==0:
            raise Exception("Error: No exchanges were set! Please set some exchanges first before using this function")


        query=self.__unionQuery(number,exchanges=exchanges)
        if debug==True:
            print(query)
        if self.__dbQuery(query):
            self.last_query="MySQL_NCases_getLastNRecords"
            df=self.current_frame.copy()
            self.current_frame.grouped=True
            df.grouped=True
            return df


    def Exchanges_setAllExchanges(self,debug=True):
        """Sets the following exchanges ["btc-e.com","kraken.com","bitstamp.com","hitbtc.com","bitfinex.com","okcoin.com","btcchina.com"] """
        self.exchanges=["btc-e.com","kraken.com","bitstamp.com","hitbtc.com","bitfinex.com","okcoin.com","btcchina.com"]
        if debug==True:
            print(self.exchanges)

    def Exchanges_setUSDExchanges(self,debug=True):
        """Sets the following exchanges ["btc-e.com","kraken.com","bitstamp.com","hitbtc.com","bitfinex.com"] """
        self.exchanges=["btc-e.com","kraken.com","bitstamp.com","hitbtc.com","bitfinex.com"]
        if debug==True:
            print(self.exchanges)

    def Descriptives_describeCurrentFrame(self,df=None):
        """Summary statistics on the current current_frame."""
        if df is None:
            df=self.current_frame
        try:
            print df.describe()
        except:
            raise Exception("Error describeCurrentFrame(). Please make sure you have executed a query first before running describeCurrentFrame().")

    def Descriptives_describeGroupedFrame(self,df=None):
        """Provides summary statistics per exchange. This method needs the getLastNBids() to be run first."""
        if df is None:
            df=self.current_frame.copy()
        try:
            groups=df.groupby('exchange')
            print groups.apply(lambda x : x.describe())
        except:
            raise Exception("Error describeGroupedFrame(). Please make sure you have executed a query. Also some queries (such as spread queries) do not support group describe.")

    def __PlotGrouped(self,df,var,smoothed,markers):
        """Helper function used by Descriptives_Plotting_plotGroupedFrame()."""
        groups=df.groupby('exchange')
        i=0
        for group in groups:
            for j in range(0,len(var)):
                plt.plot(group[1][var[j]],markers[i],label=group[0].replace(".com","")+"-"+var[j]+smoothed)
                i=i+1
                plt.hold(True)


    def Descriptives_Plotting_plotGroupedFrame(self,df=None,var=[],use_smoothed_frame=True,use_current_frame=True):
        """Plots the bid and ask for each exchange. If df is a list, then it plots each dataframe. Requires the getLastNRecords or getSelectedSpreadN to be run first.
        It assumes that all the dataframes are of the same kind (they have been from the same kind query, e.g. are all from getLastNRecords for example)."""

        #close all plots before doing anything, otherwise sometimes plotting can get messed up
        #plt.close()
        plt.figure(floor(random.random()*1000000))

        #if no dataframes have been provided, then simply use the current frame or the smoothed frame, as it is set by the variables.
        #otherwise use the dataframe(s) provided
        markers=["^","8",":","--",".","1","d","x","4","H","*","+","o","v","D",">","|","_"]
        #just make sure we have enough markers
        markers=markers*3
        try:
            if df is None:
                 if use_smoothed_frame and len(self.smoothed_frame)>0:
                    if self.smoothed_frame.grouped==False:
                        raise Exception("Error: Smoothed frame is not a grouped frame.")
                    df=self.smoothed_frame.copy()


                    if var==[]:
                        #if the dataframe contains 'volume', then the df must have come from a getLastNRecords
                        if df.columns.tolist().__contains__('volume'):
                            var=['bid','ask']
                        else:
                            #the other choice is that the df has come from
                            var=['bidaskspread']


                    #the function assumes that the rows are in reverse chronological order (most recent come first).
                    #in order to plot the data, we need to reverse the order.
                    try:
                        #timenow is used by getNRecords
                        df=df.sort(columns=['datenow','timenow'],ascending=True)
                    except:
                        #hourminute is used by selectedNSpread
                        df=df.sort(columns=['datenow','hourminute'],ascending=True)

                    df=df.reset_index(drop=True)
                    self.__PlotGrouped(df,var,"-smoothed",markers[0:int(len(markers)/2)])
                 if use_current_frame:
                    if self.current_frame.grouped==False:
                        raise Exception("Error: Current frame is not a grouped frame.")
                    df=self.current_frame.copy()

                    if var==[]:
                        #if the dataframe contains 'volume', then the df must have come from a getLastNRecords
                        if df.columns.tolist().__contains__('volume'):
                            var=['bid','ask']
                        else:
                            #the other choice is that the df has come from
                            var=['bidaskspread']


                    #the function assumes that the rows are in reverse chronological order (most recent come first).
                    #in order to plot the data, we need to reverse the order.
                    try:
                        #timenow is used by getNRecords
                        df=df.sort(columns=['datenow','timenow'],ascending=True)
                    except:
                        #hourminute is used by selectedNSpread
                        df=df.sort(columns=['datenow','hourminute'],ascending=True)

                    df=df.reset_index(drop=True)
                    self.__PlotGrouped(df,var,"",markers[int(len(markers)/2)+1:len(markers)])
                 #the legend is presented for the native frames, but is not included for foreign frames.
                 leg=plt.legend(loc='best', fancybox=True)
                 leg.get_frame().set_alpha(0.5)
                 plt.show()
            else:
                #final choice the df must be a list. So, copy this to a variable called dfs, and then use variable 'df' as a dummy
                dfs=df

                if var==[]:
                        #if the first dataframe of the list contains 'volume', then the df must have come from a getLastNRecords
                        if dfs[0].columns.tolist().__contains__('volume'):
                            var=['bid','ask']
                        else:
                            #the other choice is that the df has come from
                            var=['bidaskspread']

                for i in range(0,len(dfs)):
                    df=dfs[i]
                    if df.grouped==False:
                        raise Exception("Error: Non-grouped dataframe detected.")


                    #the function assumes that the rows are in reverse chronological order (most recent come first).
                    #in order to plot the data, we need to reverse the order.
                    try:
                        #timenow is used by getNRecords
                        df=df.sort(columns=['datenow','timenow'],ascending=True)
                    except:
                        #hourminute is used by selectedNSpread
                        df=df.sort(columns=['datenow','hourminute'],ascending=True)

                    df=df.reset_index(drop=True)
                    self.__PlotGrouped(df,var,"",markers)
            #if the dataframes are not native, ignore the legends
                plt.show()
        except:
            raise Exception("Error plotAskandBidsGroupedFrame(). Please make sure you have executed a query. Also some queries (such as spread queries) do not support group describe.")

    def Descriptives_crosstabSpread(self,df=None,allow_duplicates=True):
        """Crosstabulates the exchanges in the spread (min ask exchange vs max bid exchange for each row)."""
        if df is None:
            df=self.current_frame.copy()
        try:
            #this functionality regarding duplicates is obsolete
#            if not allow_duplicates:
#                #if duplicates are not allowed, then we need to sort by time and the spread
#                #and make sure that we take only the last duplicate, which includes the biggest spread
#                df=df.sort(columns=['hourminute','bidaskspread'],ascending=True)
#                df=df.reset_index(drop=True)
#                df=df.drop_duplicates(['hourminute'],take_last=True)
#
#                print(crosstab(df['minaskexchange'],df['maxbidexchange']))
#            else:
#                print(crosstab(df['minaskexchange'],df['maxbidexchange']))
            print(crosstab(df['minaskexchange'],df['maxbidexchange']))
        except:
            print "Error Descriptives_crosstabSpread. Make sure you have executed a spread query before executing this function."


    def Descriptives_crosstabArray(self,df=None):
        """Returns a dictionary with all the pairs between exchanges in the frame returned from spread query."""
        if df is None:
            df=self.current_frame
        
        maxbidsexchanges=df['maxbidexchange'].value_counts()
        minaskexchanges=df['minaskexchange'].value_counts()
        
        maxbidindex=maxbidsexchanges.index
        minaskindex=minaskexchanges.index
        
        freqs=[]
        for i in range(0,len(maxbidindex)):
            for j in range(0,len(minaskindex)):
                dummy=df[(df['maxbidexchange']==maxbidindex[i]) & (df['minaskexchange']==minaskindex[j])]
                rows=len(dummy)
                temp={'maxbidexchange':maxbidindex[i],'minaskexchange':minaskindex[j],'freq':rows}
                freqs.append(temp)
        return freqs


    def Descriptives_Plotting_plotSingleVariable(self,dfs=[],var='bidaskspread',allow_duplicates=True,use_current_frame=True,use_smoothed_frame=True):
        """Plots a single variable from a non-grouped frame, along with its smoothing.
        This function can work either with the native frames or with externally provided dataframes.
        The arguments use_current_frame and use_smoothed_frame are ignored if dfs is not empty.

        Arguments:
            dfs: A list of spread dataframes, extracted by the MySQL_NCases_getNSpread() or similar (non-grouped) function
            allow_duplicates: If set to False, then duplicate entries are deleted. Duplicate entries can come as a result of the query returning instaces for the same minute.
            Default is False, since if the duplicates are plotted, the plotting function will inject them as additional datapoints in time.
            use_current_frame, use_smoothed_frame: If set to true, then the current frames are used. These arguments are ignored if a list of dfs is provided."""
        #plt.close()
        plt.figure(floor(random.random()*1000000))
        if dfs==[]:
            if use_current_frame:
                try:
                    df=self.current_frame.copy()
                except:
                    print("Error: use_current_frame was set to True, but no current frame is present")
                try:
                    #we have to reverse the order and reset the index in order to plot from older to more recent
                    #We also order by the bidaskspread. The reason for that is that in case that there are some duplicate values for the same minute
                    #we want to remove the row with the smallest spread (given that duplicates are not allowed). The same aplies for maxbid and minask.
                    try:
                        df=df.sort(columns=['datenow','hourminute',var],ascending=True)
                    except:
                       
                        try:
                            print("Default variable not recognized. Tryng variable 'maxbid'")
                            df=df.sort(columns=['datenow','hourminute','maxbid'],ascending=True)
                            var='maxbid'
                        except:
                            try:
                                print("Default variable not recognized. Tryng variable 'minask'")
                                df=df.sort(columns=['datenow','hourminute','minask'],ascending=True)
                                var='minask'
                            except:
                                raise Exception('Please provide a valid variable')


                    df=df.reset_index(drop=True)
                    if not allow_duplicates:
                    #The spreads are sorted in ascending order. So if we get two spreads at the same time, we want the biggest one, which based on the
                    #sorting is going to be the last.
                        df=df.drop_duplicates(['datenow','hourminute'],take_last=True)

                    plt.plot(df[var],label=var)

                except:
                    print """Error Descriptives_Plotting_plotSpread(). Make sure you have executed a spread query before executing this function or
                    you have provided an appropriate dataframe. The dataframe format for this function."""

            if use_smoothed_frame and len(self.smoothed_frame)>0:
                #we have to reverse the order and reset the index in order to plot from older to more recent
                    try:
                        df=self.smoothed_frame.copy()
                    except:
                        print("Warning: include_smoothed was set to True, but no smoothed frame was provided")
                        return
                #if duplicates are not allowed, then we need to sort by time and the spread
                #and make sure that we take only the last duplicate, which includes the biggest spread
                    df=df.sort(columns=['datenow','hourminute',var],ascending=True)
                    df=df.reset_index(drop=True)
                    if not allow_duplicates:
                        df=df.drop_duplicates(['datenow','hourminute'],take_last=True)
                    plt.plot(df[var],label=var+'-smoothed')
        else:
            for i in range(0,len(dfs)):
                df=dfs[i]
                #if duplicates are not allowed, then we need to sort by time and the spread
                #and make sure that we take only the last duplicate, which includes the biggest spread
                df=df.sort(columns=['hourminute',var],ascending=True)
                df=df.reset_index(drop=True)
                if not allow_duplicates:
                    df=df.drop_duplicates(['hourminute'],take_last=True)

                plt.plot(df[var],label=var)

        leg=plt.legend(loc='best', fancybox=True)
        leg.get_frame().set_alpha(0.5)
        plt.show()

    def Descriptives_getMissingMinutes(self,df=[]):
        """The getMaxNBids, getMinNAsks, and getNSpread may not return a single value for each minute.
        This can happen due to problems in the process that stores data in the database. Ther missing minutes is a way
        to estimate the magnitude of this problem for the results of a query. This can be converted to a metric by calculating the mean or the sum."""
        if df is None:
            df=self.current_frame.copy()

        dummy=[]
        for i in range(0,len(df)):
            #it assumes that the type of 'hourminute' is timedelta64
            dummy.append(df['hourminute'][i])

        lagged=dummy[1:len(dummy)]
        results=[]
        for i in range(0,len(lagged)):
            dif=dummy[i]-lagged[i]
            #we need to divide to convert to minute
            results.append(int(dif)/(6*(10**10))-1)
        return results


    def __createLaggedSeries(self,df,vars,exclude_exchanges=[],lags=3,input=True):
        """ Helper function that lags a current_frame and returns it. It is used by Analysis_getInputAndTargets.
        It is used for creating both input and target dataframes."""
        records=[]

        if df.grouped==True:

            #get the values from each group
            for group in df.groupby('exchange'):
                if not exclude_exchanges.__contains__(group[0]):
                    records.append(list(group[1][vars].values))
            #convert to list (from numpy list)
            for i in range(0,len(records)):
                records[i]=[x.tolist() for x in records[i]]
        else:
            records.append(df[vars[0]])
            records[0]=[[x] for x in records[0]]

            #lag the time series and create a new list. If this is a target list, and not an input
            #then we do not need to add lags at every element of the list
        if input:
            for i in range(0,len(records)):
                #we start from 1 and not 0, since the most recent record is a target, not an input
                for j in range(1,len(records[i])-lags+1):
                    #we have to subtract one otherwise we will end up with one more lag than the user intended! So, lag=3, for example, means
                    #that each input cell will have 3 elements
                    lagged=records[i][j:j+lags-1]
                    #flatten the list that is being appended
                    lagged=[x for y in lagged for x in y]
                    #append the list
                    [records[i][j-1].append(y) for y in lagged]

        #remove the last <LAG> instances of the list from each group
        for i in range(0,len(records)):
            records[i]=records[i][0:len(records[i])-lags]


        return records


    def __combineGroups(self,records):
        """Helper function used by Analysis_getInputAndTargets(). It will break down if the groups are of unequal length"""
        try:
            for i in range(1,len(records)):
                    for j in range(0,len(records[0])):
                        to_append=records[i][j]
                        [records[0][j].append(y) for y in to_append]
            return records[0]
        except:
            raise Exception("Error when trying to combine the different groups, into one list. Please make sure that the groups are of equal size.")

    def Analysis_smoothFrame(self,df=None,smoothed_vars=None,smoothing=2):
        if df is None:
            grouped=self.current_frame.grouped
            df=self.current_frame.copy()
            df.grouped=grouped
        #detect whether the dataframe is grouped or not and call the appropriate function
        if df.grouped==True:
            return self.__Analysis_smoothGroupedFrame(df,smoothed_vars,smoothing)
        else:
            return self.__Analysis_smoothNonGroupedFrame(df,smoothed_vars,smoothing)

    def __Analysis_smoothNonGroupedFrame(self,df=None,smoothed_vars=None,smoothing=2):
        """Creates a new frame by applying the a rolling mean function on a non-grouped frame."""

        if df is None:
            df=self.current_frame.copy()
            grouped=self.current_frame.grouped
        else:
            grouped=df.grouped

        #guess the vars
        if smoothed_vars is None:
            cols=df.columns.tolist()
            if cols.__contains__('bidaskspread'):
                smoothed_vars=['bidaskspread']
            elif cols.__contains__('maxbid'):
                smoothed_vars=['maxbid']
            elif cols.__contains__('minask'):
                smoothed_vars=['minask']

        if smoothing>len(df):
            raise Exception("Error: the smoothing parameter is larger than the length of the dataframe. Please try with a smaller smoothing parameter.")

        #sort as appropriate before applying the rolling mean so that latter instances are towards the end
        df=df.sort(columns=['datenow','hourminute'],ascending=True)
        df=df.reset_index(drop=True)

        dummy=rolling_mean(df[smoothed_vars],smoothing)

        df.loc[:,smoothed_vars]=dummy
        #reverse the sorting to show the lastest instances first
        df=df.sort(columns=['datenow','hourminute'],ascending=False)
        self.smoothed_frame=df.copy()
        self.smoothed_frame.grouped=grouped
        df.grouped=grouped
        return df


    def __Analysis_smoothGroupedFrame(self,df=None,smoothed_vars=None,smoothing=2):
        """Creates a new grouped frame, by applying rolling mean to the bid, ask and the volume.
        If any variables are excluded from smoothing, then they are returned as they are."""

        if df is None:
            df=self.current_frame.copy()
            grouped=self.current_frame.grouped
        else:
            grouped=df.grouped

        if smoothed_vars is None:
            #if the df contains the volume column, then this is an indication that this dataframe has come
            #as a result of a getNRecords query
            if df.columns.tolist().__contains__('volume'):
                smoothed_vars=['bid','ask','volume']
            else:
                #otherwise, it must be from a SelectedNSpread query
                smoothed_vars=['bidaskspread']

        if smoothing>len(df):
            raise Exception("Error: the smoothing parameter is larger than the length of the dataframe. Please try with a smaller smoothing parameter.")

        #we need to sort so that the the older instances come first, before applying the rolling mean function
        try:
            #for getNRecords query
            df=df.sort(columns=['datenow','timenow'],ascending=True)
            df=df.reset_index(drop=True)
        except:
            #for selectedNSpread query
            df=df.sort(columns=['datenow','hourminute'],ascending=True)
            df=df.reset_index(drop=True)


        try:
            for group in df.groupby('exchange'):
                dummy=rolling_mean(group[1][smoothed_vars],smoothing)
                df.loc[df['exchange']==group[0],smoothed_vars]=dummy
            #reverse the sorting so that the more recent instances are at the head (reverse chronological order)
            try:
                df=df.sort(columns=['datenow','timenow'],ascending=False)
            except:
                df=df.sort(columns=['datenow','hourminute'],ascending=False)
            #copy and set the .grouped property
            df=df.reset_index(drop=True)
            self.smoothed_frame=df.copy()
            self.smoothed_frame.grouped=grouped
            df.grouped=grouped
            return df
        except Exception,e:
            print(str(e))
            raise Exception("Error at __Analysis_smoothGroupedFrame. Make sure you have executed a grouped query first, such as MySQL_NCases_getLastNRecords")

    def Analysis_getInputAndTargets(self,df=None,combine_groups=True,input_vars=[],exclude_input_exchanges=[],
    target_vars=[],exclude_target_exchanges=[],lags=3):
        """ VERY IMPORTANT: The function assumes that the entries for each exchange are in descending chronological order.
        More recent inputs come first.
        Arguments:
            """
        if df is None:
            grouped=self.current_frame.grouped
            df=self.current_frame.copy()
            df.grouped=grouped


#try to guess the type of dataframe and the variables the user is trying to predict
        if input_vars==[]:
            #if the df contains the volume column, then this is an indication that this dataframe has come
            #as a result of a getNRecords query
            if df.columns.tolist().__contains__('volume'):
                input_vars=['bid','ask','volume']
            elif df.columns.tolist().__contains__('bidaskspread'):
                input_vars=['bidaskspread']
            elif df.columns.tolist().__contains__('maxbid'):
                input_vars=['maxbid']
            elif df.columns.tolist().__contains__('minask'):
                input_vars=['minask']
            else:
                raise Exception("Please specify input_vars")

        if target_vars==[]:
            #if the df contains the volume column, then this is an indication that this dataframe has come
            #as a result of a getNRecords query
            if df.columns.tolist().__contains__('volume'):
                target_vars=['bid','ask','volume']
            elif df.columns.tolist().__contains__('bidaskspread'):
                target_vars=['bidaskspread']
            elif df.columns.tolist().__contains__('maxbid'):
                target_vars=['maxbid']
            elif df.columns.tolist().__contains__('minask'):
                target_vars=['minask']
            else:
                raise Exception("Please specify target_vars")


        input_records=self.__createLaggedSeries(df=df,vars=input_vars,exclude_exchanges=exclude_input_exchanges,lags=lags,input=True)
        target_records=self.__createLaggedSeries(df=df,vars=target_vars,exclude_exchanges=exclude_target_exchanges,lags=lags,input=False)

        #if combine_groups=True, then we return a single list, where each element contains
        #all the lags from all the exchanges. Otherwise we return a list of lists, where each
        #sublist contains only a single exchange (along with its lags)
        if combine_groups and df.grouped==True:
            input_records=self.__combineGroups(input_records)
            input_records=input_records[1:len(input_records)]

            target_records=self.__combineGroups(target_records)
            target_records=target_records[0:len(target_records)-1]

            return {'input': numpyarray(input_records),'target': numpyarray(target_records)}
        elif df.grouped==True:
            final_dict={}

            exchange_input_names=[]
            exchange_target_names=[]

            #get the names from each group
            for group in df.groupby('exchange'):
                if not exclude_input_exchanges.__contains__(group[0]):
                    exchange_input_names.append(group[0])
                if not exclude_target_exchanges.__contains__(group[0]):
                    exchange_target_names.append(group[0])
            #add to the dict the inputs
            for i in range(0,len(exchange_input_names)):
                input=input_records[i][1:len(input_records[i])]
                final_dict['input-'+exchange_input_names[i]]=numpyarray(input)
            #add to the dict the targets
            for i in range(0,len(exchange_target_names)):
                target=target_records[i][0:len(target_records[i])-1]
                final_dict['target-'+exchange_target_names[i]]=numpyarray(target)

            return final_dict
        else:
            return {'input': numpyarray(input_records[0]),'target':numpyarray(target_records[0])}


    def Analysis_targetExchange(self,exchange):
        """Helper function for the Analysis_getInputsAndTargets(). Arguments:
            exchange: This is the name of the exchange that you want to be the target for the Analysis_getInputsAndTargets()."""
        dummy=[]
        #add all exchanges except for the one we want to include
        [dummy.append(x) for x in self.exchanges if x!=exchange]
        if len(self.exchanges)==len(dummy):
            print("Warning: no exchanges were removed from the list. Please make sure you've spelled the name(s) correctly.")
        return dummy

    def Pandas_wrapper(self,df,function):
        try:
            grouped=df.grouped
            func = getattr(df, function)
            res=func()
            res.grouped=grouped
            return res
        except AttributeError:
            raise Exception("function not found")