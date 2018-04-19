# Max van Delft, Patrick t Jong
# April 2018
# run by 'python customer_dataV2.py'. execution takes about 20 minutes

# Costumer data before 2015:
# 1. Last transaction date                      V
# 2. First transaction date                     V
# 3. Average time between transactions          V
# 4. Lifespan                                   V
# 5. Number of transactions                     V
# 6. Spending behaviour (total spending price)  V
# 7. Countrycode                                V
# 8. Maximal transaction price                  V
# 9. Average transaction price                  V

import datetime
import numpy as np
import pandas as pd
from collections import defaultdict
from sklearn import preprocessing

print ("Started")
#Global variables
df = None
df1 = None
df2 = None
dfCustomer = None
df1ByAccountName = None

# Read data from sales.csv
def readdata():
    global df
    df = pd.read_csv('sales.csv', names=['saleId','saleDateTime','accountName','coins','currency','priceInCurrency','priceInEUR','methodId','ip','ipCountry'])
    df['saleDateTime'] = pd.to_datetime(df['saleDateTime'])
    return;


# Split dataset in data before and after 1 jan 2014
# We collect feature data for customers that joined before this time
def splitdata():
    global df, df1, df2
    df1 = df[df['saleDateTime'] <  pd.Timestamp('20140101')]
    df2 = df[df['saleDateTime'] >= pd.Timestamp('20140101')]
    return;

# Print dataset to the output CSV file
def printdata():
    global dfCustomer
    print ("printing sheet to csv")
    dfCustomer.to_csv(path_or_buf="customer_data.csv", index=False)
    return;

# Create features
def setfeatures():
    global df1, dfCustomer, df1ByAccountName
    print ("busy setting features")
    dfCustomer = pd.DataFrame()
    dfCustomer['accountName'] = df1['accountName']
    df1ByAccountName  = df1.groupby('accountName')
    # Source: https://stackoverflow.com/questions/22072943/faster-way-to-transform-group-with-mean-value-in-pandas/22073449
    dfCustomer['lastTransactionDate' ] = df1ByAccountName['saleDateTime'].transform('max') #1
    dfCustomer['firstTransactionDate'] = df1ByAccountName['saleDateTime'].transform('min') #2
    dfCustomer['lifeSpan'            ] = dfCustomer['lastTransactionDate' ] - dfCustomer['firstTransactionDate'] #3
    dfCustomer['numberOfTransactions'] = df1ByAccountName['accountName' ].transform('count') #4
    dfCustomer['avgTimeBetweenTransactions'] = dfCustomer.apply(lambda row: row.lifeSpan/row.numberOfTransactions, axis=1) #5
    dfCustomer['totalSpending'       ] = df1ByAccountName['priceInEUR'  ].transform('sum') #6

    dfCustomer['maxTransactionPrice' ] = df1ByAccountName['priceInEUR'  ].transform('max') #8
    dfCustomer['avgTransactionPrice' ] = df1ByAccountName['priceInEUR'  ].transform(np.mean) #9
    dfCustomer['country'             ] = df1['ipCountry'] #10
    # Convert date to number of days:mins:secs (timedelta obtject) until 1 jan 2014
    dfCustomer['firstTransactionDate'] = pd.Timestamp('20140101') - dfCustomer['firstTransactionDate']
    dfCustomer['lastTransactionDate' ] = pd.Timestamp('20140101') - dfCustomer['lastTransactionDate' ]
    # convert timedelta to days
    dfCustomer['firstTransactionDate'] = dfCustomer['firstTransactionDate'].astype('timedelta64[D]') # extract days from timedelta
    dfCustomer['lastTransactionDate' ] = dfCustomer['lastTransactionDate' ].astype('timedelta64[D]')
    dfCustomer['lifeSpan'            ] = dfCustomer['lifeSpan'            ].astype('timedelta64[D]')
    dfCustomer['avgTimeBetweenTransactions'] = dfCustomer.apply(lambda row: row.lifeSpan/row.numberOfTransactions, axis=1)
    # Check for duplicates
    dfCustomer.drop_duplicates(subset=["accountName"],inplace=True)
    dfCustomer = dfCustomer.reset_index(drop=True)
    # Convert countries to country codes, i.e. the rank of a country in terms of the number of transactions. 0 => most transactions
    dfCountries = pd.DataFrame()
    dfCountries["country"] = df1["ipCountry"]
    dfCountries["numberOfTransactions"] = df1.groupby('ipCountry')['ipCountry'].transform('count')
    dfCountries.drop_duplicates(subset=["country"],inplace=True)
    dfCountries = dfCountries.sort_values(by=['numberOfTransactions'], ascending=False)
    dfCountries = dfCountries[pd.notnull(dfCountries["country"])]
    dfCountries = dfCountries.reset_index(drop=True)
    dfCountries["rank"] = range(0,len(dfCountries.index))
    dfCountries = dfCountries.drop('numberOfTransactions', 1)
    dfCustomer["countryCode"] = len(dfCountries.index)
    dic = dict((name,None) for name in dfCountries["country"])
    dic = defaultdict(lambda: len(dfCountries.index)+1, dic)
    for index, row in dfCountries.iterrows():
        dic[row["country"]] = row["rank"]
    for i in range(0,len(dfCustomer.index)) :
        dfCustomer.loc[i,'countryCode'] = dic[dfCustomer.iloc[i]["country"]]
    # Clean up old country column
    del dfCustomer['country']


    # used for identification of accounts having purchased in the last month, 2nd last month, etc
    dfLastMonth    = df1[df1['saleDateTime'] >= pd.Timestamp('20131201')]
    dfLastMonth   ['totalSpending'       ] = dfLastMonth   .groupby('accountName')['priceInEUR' ].transform('sum')
    dfLastMonth   ['numberOfTransactions'] = dfLastMonth   .groupby('accountName')['accountName'].transform('count')
    df2ndLastMonth = df1[df1['saleDateTime'] >= pd.Timestamp('20131101')]
    df2ndLastMonth = df2ndLastMonth[df2ndLastMonth['saleDateTime'] < pd.Timestamp('20131201')]
    df2ndLastMonth['totalSpending'       ] = df2ndLastMonth.groupby('accountName')['priceInEUR' ].transform('sum')
    df2ndLastMonth['numberOfTransactions'] = df2ndLastMonth.groupby('accountName')['accountName'].transform('count')
    df3rdLastMonth = df1[df1['saleDateTime'] >= pd.Timestamp('20131001')]
    df3rdLastMonth = df3rdLastMonth[df3rdLastMonth['saleDateTime'] < pd.Timestamp('20131101')]
    df3rdLastMonth['totalSpending'       ] = df3rdLastMonth.groupby('accountName')['priceInEUR' ].transform('sum')
    df3rdLastMonth['numberOfTransactions'] = df3rdLastMonth.groupby('accountName')['accountName'].transform('count')
    df4thLastMonth = df1[df1['saleDateTime'] >= pd.Timestamp('20130901')]
    df4thLastMonth = df4thLastMonth[df4thLastMonth['saleDateTime'] < pd.Timestamp('20131001')]
    df4thLastMonth['totalSpending'       ] = df4thLastMonth.groupby('accountName')['priceInEUR' ].transform('sum')
    df4thLastMonth['numberOfTransactions'] = df4thLastMonth.groupby('accountName')['accountName'].transform('count')
    df5thLastMonth = df1[df1['saleDateTime'] >= pd.Timestamp('20130801')]
    df5thLastMonth = df5thLastMonth[df5thLastMonth['saleDateTime'] < pd.Timestamp('20130901')]
    df5thLastMonth['totalSpending'       ] = df5thLastMonth.groupby('accountName')['priceInEUR' ].transform('sum')
    df5thLastMonth['numberOfTransactions'] = df5thLastMonth.groupby('accountName')['accountName'].transform('count')
    df6thLastMonth = df1[df1['saleDateTime'] >= pd.Timestamp('20130701')]
    df6thLastMonth = df6thLastMonth[df6thLastMonth['saleDateTime'] < pd.Timestamp('20130801')]
    df6thLastMonth['totalSpending'       ] = df6thLastMonth.groupby('accountName')['priceInEUR' ].transform('sum')
    df6thLastMonth['numberOfTransactions'] = df6thLastMonth.groupby('accountName')['accountName'].transform('count')
    dfLastMonth   .drop_duplicates(subset=["accountName"],inplace=True)
    df2ndLastMonth.drop_duplicates(subset=["accountName"],inplace=True)
    df3rdLastMonth.drop_duplicates(subset=["accountName"],inplace=True)
    df4thLastMonth.drop_duplicates(subset=["accountName"],inplace=True)
    df5thLastMonth.drop_duplicates(subset=["accountName"],inplace=True)
    df6thLastMonth.drop_duplicates(subset=["accountName"],inplace=True)

    dfCustomer["spendingLastMonth"   ] = 0                                                                                                       #9
    dfCustomer["spending2ndLastMonth"] = 0                                                                                                       #11
    dfCustomer["spending3rdLastMonth"] = 0                                                                                                       #13
    dfCustomer["spending4thLastMonth"] = 0                                                                                                       #15
    dfCustomer["spending5thLastMonth"] = 0                                                                                                       #17
    dfCustomer["spending6thLastMonth"] = 0                                                                                                       #19

    dfCustomer["transactionsLastMonth"   ] = 0                                                                                                   #10
    dfCustomer["transactions2ndLastMonth"] = 0                                                                                                   #12
    dfCustomer["transactions3rdLastMonth"] = 0                                                                                                   #14
    dfCustomer["transactions4thLastMonth"] = 0                                                                                                   #16
    dfCustomer["transactions5thLastMonth"] = 0                                                                                                   #18
    dfCustomer["transactions6thLastMonth"] = 0                                                                                                   #20

    d1 = dict((name,0) for name in dfLastMonth   ["accountName"])
    d2 = dict((name,0) for name in df2ndLastMonth["accountName"])
    d3 = dict((name,0) for name in df3rdLastMonth["accountName"])
    d4 = dict((name,0) for name in df4thLastMonth["accountName"])
    d5 = dict((name,0) for name in df5thLastMonth["accountName"])
    d6 = dict((name,0) for name in df6thLastMonth["accountName"])
    d1 = defaultdict(lambda: 0, d1)
    d2 = defaultdict(lambda: 0, d2)
    d3 = defaultdict(lambda: 0, d3)
    d4 = defaultdict(lambda: 0, d4)
    d5 = defaultdict(lambda: 0, d5)
    d6 = defaultdict(lambda: 0, d6)

    for index, row in dfLastMonth.iterrows():
        d1[row["accountName"]] = row["totalSpending"]
    for index, row in df2ndLastMonth.iterrows():
        d2[row["accountName"]] = row["totalSpending"]
    for index, row in df3rdLastMonth.iterrows():
        d3[row["accountName"]] = row["totalSpending"]
    for index, row in df4thLastMonth.iterrows():
        d4[row["accountName"]] = row["totalSpending"]
    for index, row in df5thLastMonth.iterrows():
        d5[row["accountName"]] = row["totalSpending"]
    for index, row in df6thLastMonth.iterrows():
        d6[row["accountName"]] = row["totalSpending"]

    for i in range(0,len(dfCustomer.index)) :
        if d1[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spendingLastMonth']    = d1[dfCustomer.iloc[i]["accountName"]]
        if d2[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spending2ndLastMonth'] = d2[dfCustomer.iloc[i]["accountName"]]
        if d3[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spending3rdLastMonth'] = d3[dfCustomer.iloc[i]["accountName"]]
        if d4[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spending4thLastMonth'] = d4[dfCustomer.iloc[i]["accountName"]]
        if d5[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spending5thLastMonth'] = d5[dfCustomer.iloc[i]["accountName"]]
        if d6[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spending6thLastMonth'] = d6[dfCustomer.iloc[i]["accountName"]]

    for index, row in dfLastMonth.iterrows():
        d1[row["accountName"]] = row["numberOfTransactions"]
    for index, row in df2ndLastMonth.iterrows():
        d2[row["accountName"]] = row["numberOfTransactions"]
    for index, row in df3rdLastMonth.iterrows():
        d3[row["accountName"]] = row["numberOfTransactions"]
    for index, row in df4thLastMonth.iterrows():
        d4[row["accountName"]] = row["numberOfTransactions"]
    for index, row in df5thLastMonth.iterrows():
        d5[row["accountName"]] = row["numberOfTransactions"]
    for index, row in df6thLastMonth.iterrows():
        d6[row["accountName"]] = row["numberOfTransactions"]

    for i in range(0,len(dfCustomer.index)) :
        if d1[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactionsLastMonth']    = d1[dfCustomer.iloc[i]["accountName"]]
        if d2[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactions2ndLastMonth'] = d2[dfCustomer.iloc[i]["accountName"]]
        if d3[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactions3rdLastMonth'] = d3[dfCustomer.iloc[i]["accountName"]]
        if d4[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactions4thLastMonth'] = d4[dfCustomer.iloc[i]["accountName"]]
        if d5[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactions5thLastMonth'] = d5[dfCustomer.iloc[i]["accountName"]]
        if d6[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactions6thLastMonth'] = d6[dfCustomer.iloc[i]["accountName"]]


    # The dataframes below are used for identification of accounts having purchased in the last week, last 2 weeks, etc
    dfLastWeek    = df1[df1['saleDateTime'] >= pd.Timestamp('20131224')]
    dfLastWeek   ['totalSpending'       ] = dfLastWeek   .groupby('accountName')['priceInEUR' ].transform('sum')
    dfLastWeek   ['numberOfTransactions'] = dfLastWeek   .groupby('accountName')['accountName'].transform('count')
    dfLast2Weeks  = df1[df1['saleDateTime'] >= pd.Timestamp('20131217')]
    dfLast2Weeks ['totalSpending'       ] = dfLast2Weeks .groupby('accountName')['priceInEUR' ].transform('sum')
    dfLast2Weeks ['numberOfTransactions'] = dfLast2Weeks .groupby('accountName')['accountName'].transform('count')
    dfLast2Months = df1[df1['saleDateTime'] >= pd.Timestamp('20131101')] # The total sales per customer for the last month can be found in 'dflast'
    dfLast2Months['totalSpending'       ] = dfLast2Months.groupby('accountName')['priceInEUR' ].transform('sum')
    dfLast2Months['numberOfTransactions'] = dfLast2Months.groupby('accountName')['accountName'].transform('count')
    dfLast4Months = df1[df1['saleDateTime'] >= pd.Timestamp('20130901')]
    dfLast4Months['totalSpending'       ] = dfLast4Months.groupby('accountName')['priceInEUR' ].transform('sum')
    dfLast4Months['numberOfTransactions'] = dfLast4Months.groupby('accountName')['accountName'].transform('count')
    dfLast8Months = df1[df1['saleDateTime'] >= pd.Timestamp('20130401')]
    dfLast8Months['totalSpending'       ] = dfLast8Months.groupby('accountName')['priceInEUR' ].transform('sum')
    dfLast8Months['numberOfTransactions'] = dfLast8Months.groupby('accountName')['accountName'].transform('count')
    dfLastWeek   .drop_duplicates(subset=["accountName"],inplace=True)
    dfLast2Weeks .drop_duplicates(subset=["accountName"],inplace=True)
    dfLast2Months.drop_duplicates(subset=["accountName"],inplace=True)
    dfLast4Months.drop_duplicates(subset=["accountName"],inplace=True)
    dfLast8Months.drop_duplicates(subset=["accountName"],inplace=True)

    dfCustomer["spendingLastWeek"   ] = 0                                                                                                         #21
    dfCustomer["spendingLast2Weeks" ] = 0                                                                                                         #23
    dfCustomer["spendingLast2Months"] = 0                                                                                                         #25
    dfCustomer["spendingLast4Months"] = 0                                                                                                         #27
    dfCustomer["spendingLast8Months"] = 0                                                                                                         #29

    dfCustomer["transactionsLastWeek"   ] = 0                                                                                                     #22
    dfCustomer["transactionsLast2Weeks" ] = 0                                                                                                     #24
    dfCustomer["transactionsLast2Months"] = 0                                                                                                     #26
    dfCustomer["transactionsLast4Months"] = 0                                                                                                     #28
    dfCustomer["transactionsLast8Months"] = 0                                                                                                     #30

    d1 = dict((name,0) for name in dfLastWeek   ["accountName"])
    d2 = dict((name,0) for name in dfLast2Weeks ["accountName"])
    d3 = dict((name,0) for name in dfLast2Months["accountName"])
    d4 = dict((name,0) for name in dfLast4Months["accountName"])
    d5 = dict((name,0) for name in dfLast8Months["accountName"])
    d1 = defaultdict(lambda: 0, d1)
    d2 = defaultdict(lambda: 0, d2)
    d3 = defaultdict(lambda: 0, d3)
    d4 = defaultdict(lambda: 0, d4)
    d5 = defaultdict(lambda: 0, d5)

    for index, row in dfLastWeek.iterrows():
        d1[row["accountName"]] = row["totalSpending"]
    for index, row in dfLast2Weeks.iterrows():
        d2[row["accountName"]] = row["totalSpending"]
    for index, row in dfLast2Months.iterrows():
        d3[row["accountName"]] = row["totalSpending"]
    for index, row in dfLast4Months.iterrows():
        d4[row["accountName"]] = row["totalSpending"]
    for index, row in dfLast8Months.iterrows():
        d5[row["accountName"]] = row["totalSpending"]

    for i in range(0,len(dfCustomer.index)) :
        if d1[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spendingLastWeek']    = d1[dfCustomer.iloc[i]["accountName"]]
        if d2[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spendingLast2Weeks']  = d2[dfCustomer.iloc[i]["accountName"]]
        if d3[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spendingLast2Months'] = d3[dfCustomer.iloc[i]["accountName"]]
        if d4[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spendingLast4Months'] = d4[dfCustomer.iloc[i]["accountName"]]
        if d5[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'spendingLast8Months'] = d5[dfCustomer.iloc[i]["accountName"]]

    for index, row in dfLastWeek.iterrows():
        d1[row["accountName"]] = row["numberOfTransactions"]
    for index, row in dfLast2Weeks.iterrows():
        d2[row["accountName"]] = row["numberOfTransactions"]
    for index, row in dfLast2Months.iterrows():
        d3[row["accountName"]] = row["numberOfTransactions"]
    for index, row in dfLast4Months.iterrows():
        d4[row["accountName"]] = row["numberOfTransactions"]
    for index, row in dfLast8Months.iterrows():
        d5[row["accountName"]] = row["numberOfTransactions"]

    for i in range(0,len(dfCustomer.index)) :
        if d1[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactionsLastWeek']    = d1[dfCustomer.iloc[i]["accountName"]]
        if d2[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactionsLast2Weeks']  = d2[dfCustomer.iloc[i]["accountName"]]
        if d3[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactionsLast2Months'] = d3[dfCustomer.iloc[i]["accountName"]]
        if d4[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactionsLast4Months'] = d4[dfCustomer.iloc[i]["accountName"]]
        if d5[dfCustomer.iloc[i]["accountName"]] != 0 :
            dfCustomer.loc[i,'transactionsLast8Months'] = d5[dfCustomer.iloc[i]["accountName"]]

    return;

def settarget():
    global df1, df2, dfCustomer
    dfCustomer['repurchase'] = False
    dfBefore = pd.DataFrame(); dfBefore["accountName"] = df1["accountName"]
    dfAfter  = pd.DataFrame(); dfAfter ["accountName"] = df2["accountName"]
    # dfIntersect will consist of the accountNames that have purchased before 1 jan 2014 as well as after.
    dfIntersect = pd.merge(dfBefore,dfAfter, how = 'inner', on=['accountName','accountName'])
    dfIntersect.drop_duplicates(subset=["accountName"],inplace=True)
    # setting hash table
    d = dict((name,1) for name in dfIntersect["accountName"])
    d = defaultdict(lambda: -1, d)
    for i in range(0,len(dfCustomer.index)):
        if (d[dfCustomer.iloc[i]["accountName"]] == 1):
            dfCustomer.loc[i,'repurchase'] = True
    return;


#Main
readdata()
splitdata()
setfeatures()
settarget()
printdata()

print ("Completed.")