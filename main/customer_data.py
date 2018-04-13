# Costumer data before 2015:
# 1. Number of transactions                     V
# 2. Spending behaviour (total spending price)  V
# 3. First transaction date                     V
# 4. Last transaction date                      V
# 5. Lifespan                                   V
# 6. Maximal transaction price                  V
# 7. Average transaction price                  V
# 8. Total spending last month                  V
# 9. Total spending 2nd last month              V
# 10. Total spending 3rd last month             V
# 11. Total spending 4th last month             V
# 12. Total spending 5th last month             V
# 13. Total spending 6th last month             V
# 14. Total spending last week                  V
# 15. Total spending last 2 weeks               V
# 16. Total spending last 2 months              V
# 17. Total spending last 4 months              V
# 18. Total spending last 8 months              V
# 19. Country                                   V

import datetime
#from datetime import datetime
import numpy as np
import pandas as pd
from collections import defaultdict
#import time

print("Hello World")

df = pd.read_csv('sales.csv', names=['saleId','saleDateTime','accountName','coins','currency','priceInCurrency','priceInEUR','methodId','ip','ipCountry'])
df['saleDateTime'] = pd.to_datetime(df['saleDateTime'])

# split dataset in data before and after 1 jan 2014.
# We collect feature data for cistomers that joined before this time
df1 = df[df['saleDateTime'] <  pd.Timestamp('20140101')]
#df1 = df1[df1['saleDateTime'] >= pd.Timestamp('20130101')] #uncomment to remove customer transactions before 1 jan 2013
df2 = df[df['saleDateTime'] >= pd.Timestamp('20140101')]
#df1 = df[df['saleDateTime'] < datetime.datetime(2014,01,01)]
#df2 = df[df['saleDateTime'] >= datetime.datetime(2014,01,01)]

# The dataframe that will contain all the customer features and the target, which is a boolean whether or not the customer did a repurchase after 1 jan 2014
dfCustomer = pd.DataFrame()
dfCustomer['accountName'         ] = df1['accountName']

df1ByAccountName  = df1.groupby('accountName')
dfCustomer['numberOfTransactions'] = df1ByAccountName['accountName' ].transform('count')                                                      #1
dfCustomer['totalSpending'       ] = df1ByAccountName['priceInEUR'  ].transform('sum')                                                        #2
dfCustomer['firstTransactionDate'] = df1ByAccountName['saleDateTime'].transform('min')                                                        #3
dfCustomer['lastTransactionDate' ] = df1ByAccountName['saleDateTime'].transform('max')                                                        #4
dfCustomer['maxTransactionPrice' ] = df1ByAccountName['priceInEUR'  ].transform('max')                                                        #6
dfCustomer['avgTransactionPrice' ] = df1ByAccountName['priceInEUR'  ].transform(np.mean)                                                      #7 taken from https://stackoverflow.com/questions/22072943/faster-way-to-transform-group-with-mean-value-in-pandas/22073449

dfCustomer['lifeSpan'            ] = dfCustomer['lastTransactionDate' ] - dfCustomer['firstTransactionDate']                                  #5
dfCustomer['firstTransactionDate'] = dfCustomer['firstTransactionDate'] - pd.Timestamp('20100101')
dfCustomer['lastTransactionDate' ] = dfCustomer['lastTransactionDate' ] - pd.Timestamp('20100101')

dfCustomer['firstTransactionDate'] = dfCustomer['firstTransactionDate'].astype('timedelta64[D]') # extract days from timedelta
dfCustomer['lastTransactionDate' ] = dfCustomer['lastTransactionDate' ].astype('timedelta64[D]')
dfCustomer['lifeSpan'            ] = dfCustomer['lifeSpan'            ].astype('timedelta64[D]')
dfCustomer['avgTimeBetweenTransactions'] = dfCustomer.apply(lambda row: row.lifeSpan/row.numberOfTransactions, axis=1)
print(dfCustomer)

dfCustomer['country'             ] = df1['ipCountry']                                                                                         #19

dfCustomer.drop_duplicates(subset=["accountName"],inplace=True)
dfCustomer = dfCustomer.reset_index(drop=True)

# in the code below 'last' is shorthand for 'last month',
# used for identification of accounts having purchased in the last month, 2nd last month, etc
dflast    = df1[df1['saleDateTime'] >= pd.Timestamp('20131201')]
dflast   ['totalSpending'] = dflast   .groupby('accountName')['priceInEUR'].transform('sum')
df2ndlast = df1[df1['saleDateTime'] >= pd.Timestamp('20131101')]
df2ndlast = df2ndlast[df2ndlast['saleDateTime'] < pd.Timestamp('20131201')]
df2ndlast['totalSpending'] = df2ndlast.groupby('accountName')['priceInEUR'].transform('sum')
df3rdlast = df1[df1['saleDateTime'] >= pd.Timestamp('20131001')]
df3rdlast = df3rdlast[df3rdlast['saleDateTime'] < pd.Timestamp('20131101')]
df3rdlast['totalSpending'] = df3rdlast.groupby('accountName')['priceInEUR'].transform('sum')
df4thlast = df1[df1['saleDateTime'] >= pd.Timestamp('20130901')]
df4thlast = df4thlast[df4thlast['saleDateTime'] < pd.Timestamp('20131001')]
df4thlast['totalSpending'] = df4thlast.groupby('accountName')['priceInEUR'].transform('sum')
df5thlast = df1[df1['saleDateTime'] >= pd.Timestamp('20130801')]
df5thlast = df5thlast[df5thlast['saleDateTime'] < pd.Timestamp('20130901')]
df5thlast['totalSpending'] = df5thlast.groupby('accountName')['priceInEUR'].transform('sum')
df6thlast = df1[df1['saleDateTime'] >= pd.Timestamp('20130701')]
df6thlast = df6thlast[df6thlast['saleDateTime'] < pd.Timestamp('20130801')]
df6thlast['totalSpending'] = df6thlast.groupby('accountName')['priceInEUR'].transform('sum')
dflast   .drop_duplicates(subset=["accountName"],inplace=True)
df2ndlast.drop_duplicates(subset=["accountName"],inplace=True)
df3rdlast.drop_duplicates(subset=["accountName"],inplace=True)
df4thlast.drop_duplicates(subset=["accountName"],inplace=True)
df5thlast.drop_duplicates(subset=["accountName"],inplace=True)
df6thlast.drop_duplicates(subset=["accountName"],inplace=True)

dfCustomer["spendingLastMonth"   ] = 0                                                                                                       #8
dfCustomer["spending2ndLastMonth"] = 0                                                                                                       #9
dfCustomer["spending3rdLastMonth"] = 0                                                                                                       #10
dfCustomer["spending4thLastMonth"] = 0                                                                                                       #11
dfCustomer["spending5thLastMonth"] = 0                                                                                                       #12
dfCustomer["spending6thLastMonth"] = 0                                                                                                       #13

d1 = dict((name,0) for name in dflast   ["accountName"])
d2 = dict((name,0) for name in df2ndlast["accountName"])
d3 = dict((name,0) for name in df3rdlast["accountName"])
d4 = dict((name,0) for name in df4thlast["accountName"])
d5 = dict((name,0) for name in df5thlast["accountName"])
d6 = dict((name,0) for name in df6thlast["accountName"])
d1 = defaultdict(lambda: 0, d1)
d2 = defaultdict(lambda: 0, d2)
d3 = defaultdict(lambda: 0, d3)
d4 = defaultdict(lambda: 0, d4)
d5 = defaultdict(lambda: 0, d5)
d6 = defaultdict(lambda: 0, d6)
for index, row in dflast.iterrows():
    d1[row["accountName"]] = row["totalSpending"]
for index, row in df2ndlast.iterrows():
    d2[row["accountName"]] = row["totalSpending"]
for index, row in df3rdlast.iterrows():
    d3[row["accountName"]] = row["totalSpending"]
for index, row in df4thlast.iterrows():
    d4[row["accountName"]] = row["totalSpending"]
for index, row in df5thlast.iterrows():
    d5[row["accountName"]] = row["totalSpending"]
for index, row in df6thlast.iterrows():
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


# The dataframes below are used for identification of accounts having purchased in the last week, last 2 weeks, etc
dfLastWeek    = df1[df1['saleDateTime'] >= pd.Timestamp('20131224')]
dfLastWeek   ['totalSpending'] = dfLastWeek   .groupby('accountName')['priceInEUR'].transform('sum')
dfLast2Weeks  = df1[df1['saleDateTime'] >= pd.Timestamp('20131217')]
dfLast2Weeks['totalSpending']  = dfLast2Weeks .groupby('accountName')['priceInEUR'].transform('sum')
dfLast2Months = df1[df1['saleDateTime'] >= pd.Timestamp('20131101')] # The total sales per customer for the last month can be found in 'dflast'
dfLast2Months['totalSpending'] = dfLast2Months.groupby('accountName')['priceInEUR'].transform('sum')
dfLast4Months = df1[df1['saleDateTime'] >= pd.Timestamp('20130901')]
dfLast4Months['totalSpending'] = dfLast4Months.groupby('accountName')['priceInEUR'].transform('sum')
dfLast8Months = df1[df1['saleDateTime'] >= pd.Timestamp('20130401')]
dfLast8Months['totalSpending'] = dfLast8Months.groupby('accountName')['priceInEUR'].transform('sum')
dfLastWeek   .drop_duplicates(subset=["accountName"],inplace=True)
dfLast2Weeks .drop_duplicates(subset=["accountName"],inplace=True)
dfLast2Months.drop_duplicates(subset=["accountName"],inplace=True)
dfLast4Months.drop_duplicates(subset=["accountName"],inplace=True)
dfLast8Months.drop_duplicates(subset=["accountName"],inplace=True)

dfCustomer["spendingLastWeek"   ] = 0                                                                                                         #14
dfCustomer["spendingLast2Weeks" ] = 0                                                                                                         #15
dfCustomer["spendingLast2Months"] = 0                                                                                                         #16
dfCustomer["spendingLast4Months"] = 0                                                                                                         #17
dfCustomer["spendingLast8Months"] = 0                                                                                                         #18

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

# Convert countries to country codes, i.e. the rank of a country in terms of the number of transactions. 0 => most transactions
dfCountries = pd.DataFrame()
dfCountries["country"]              = df1["ipCountry"]
dfCountries["numberOfTransactions"] = df1.groupby('ipCountry')['ipCountry'].transform('count')
dfCountries.drop_duplicates(subset=["country"],inplace=True)
dfCountries = dfCountries.sort_values(by=['numberOfTransactions'], ascending=False)
dfCountries = dfCountries[pd.notnull(dfCountries["country"])]  # see https://stackoverflow.com/questions/13413590/how-to-drop-rows-of-pandas-dataframe-whose-value-in-certain-columns-is-nan
dfCountries = dfCountries.reset_index(drop=True)
dfCountries["rank"] = range(0,len(dfCountries.index))
dfCountries = dfCountries.drop('numberOfTransactions', 1)

dfCustomer["countryCode"] = len(dfCountries.index)
dic = dict((name,None) for name in dfCountries["country"])
dic = defaultdict(lambda: len(dfCountries.index)+1, dic)
for index, row in dfCountries.iterrows():
    dic[row["country"]] = row["rank"]

for i in range(0,len(dfCustomer.index)) :
    dfCustomer.loc[i,'countryCode'] = dic[dfCustomer.iloc[i]["country"]] # very slow, due to loc
#for index, row in df.iterrows():
#    row["countryCode"] = d[row["country"]]  # doesn't work


#dfCountries.to_csv(path_or_buf="countryRanks.csv", index=False)
#print(dfCountries)


dfCustomer['repurchase'] = False

#print dfCustomer

dfBefore = pd.DataFrame(); dfBefore["accountName"] = df1["accountName"]
dfAfter  = pd.DataFrame(); dfAfter ["accountName"] = df2["accountName"]
#dfIntersect will consist of the accountNames that have purchased before 1 jan 2014 as well as after.
dfIntersect = pd.merge(dfBefore,dfAfter, how = 'inner', on=['accountName','accountName'])
dfIntersect.drop_duplicates(subset=["accountName"],inplace=True)
#print dftmp

# python dictionary, a kind of hash table
#https://stackoverflow.com/questions/9139897/how-to-set-default-value-to-all-keys-of-a-dict-object-in-python
#https://stackoverflow.com/questions/3869487/how-do-i-create-a-dictionary-with-keys-from-a-list-and-values-defaulting-to-says
#https://stackoverflow.com/questions/26660654/how-do-i-print-the-key-value-pairs-of-a-dictionary-in-python/26660785
d = dict((name,1) for name in dfIntersect["accountName"])
d = defaultdict(lambda: -1, d)
#for i in d:
#  print i, d[i]

for i in range(0,len(dfCustomer.index)) :
    if (d[dfCustomer.iloc[i]["accountName"]] == 1) :
        dfCustomer.loc[i,'repurchase'] = True
#for index, row in dfCustomer.iterrows():
#    if (d[row["accountName"]] == 1) :
#        row["repurchase"] = True

print(dfCustomer)
dfCustomer.to_csv(path_or_buf="customer_data_from_2013_to_2014.csv", index=False)