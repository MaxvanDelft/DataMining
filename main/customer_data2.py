# Costumer data before 2015:
# 1. Number of transactions                     V
# 2. Spending behaviour (total spending price)  V
# 3. First transaction date                     V
# 4. Last transaction date                      V
# 5. Lifespan                                   V
# 6. Maximal transaction price                  V
# 7. Average transaction price                  V
# 8. Maximum amount spend on one day            
# 9. Maximal number of transactions on one day  
# 10. Country                                   V

import datetime
#from datetime import datetime
import numpy as np
import pandas as pd
from collections import defaultdict
#import time

print("Hello World")

df = pd.read_csv('sales.csv', names=['saleId','saleDateTime','accountName','coins','currency','priceInCurrency','priceInEUR','methodId','ip','ipCountry'])
df['saleDateTime'] = pd.to_datetime(df['saleDateTime'])
#df1 = df[df['saleDateTime'] < datetime.datetime(2014,01,01)]
#df2 = df[df['saleDateTime'] >= datetime.datetime(2014,01,01)]
df1 = df[df['saleDateTime'] <  pd.Timestamp('20140101')]
df2 = df[df['saleDateTime'] >= pd.Timestamp('20140101')]

dfCustomer = pd.DataFrame()
#dfCustomer = df.groupby('accountName').agg('count')
#dfCustomer = df['accountName'].value_counts()
#print dfCustomer
dfCustomer['accountName'         ] = df1['accountName']

df1ByAccountName  = df1.groupby('accountName')
dfCustomer['numberOfTransactions'] = df1ByAccountName['accountName'].transform('count')                                                       #1
dfCustomer['totalSpending'       ] = df1ByAccountName['priceInEUR'].transform('sum')                                                                 #2
dfCustomer['firstTransactionDate'] = df1ByAccountName['saleDateTime'].transform('min')                                                        #3
dfCustomer['lastTransactionDate' ] = df1ByAccountName['saleDateTime'].transform('max')                                                         #4
dfCustomer['maxTransactionPrice' ] = df1ByAccountName['priceInEUR'].transform('max')                                                           #6
dfCustomer['avgTransactionPrice' ] = df1ByAccountName['priceInEUR'].transform(np.mean)                                                         #7 taken from https://stackoverflow.com/questions/22072943/faster-way-to-transform-group-with-mean-value-in-pandas/22073449

dfCustomer['lifeSpan'            ] = dfCustomer[ 'lastTransactionDate'] - dfCustomer['firstTransactionDate']                                                                                 #5
dfCustomer['firstTransactionDate'] = dfCustomer['firstTransactionDate'] - pd.Timestamp('20100101')
dfCustomer['lastTransactionDate' ] = dfCustomer[ 'lastTransactionDate'] - pd.Timestamp('20100101')

dfCustomer['firstTransactionDate'] = dfCustomer['firstTransactionDate'].astype('timedelta64[D]') # extract days from timedelta
dfCustomer['lastTransactionDate' ] = dfCustomer['lastTransactionDate' ].astype('timedelta64[D]')
dfCustomer['lifeSpan'            ] = dfCustomer['lifeSpan'].astype('timedelta64[D]')

dfCustomer['country'             ] = df1['ipCountry']                                                                                                                #10
dfCustomer['repurchase'          ] = False

dfCustomer.drop_duplicates(subset=["accountName"],inplace=True)
#print dfCustomer

dfBefore = pd.DataFrame(); dfBefore["accountName"] = df1["accountName"]
dfAfter  = pd.DataFrame(); dfAfter ["accountName"] = df2["accountName"]
#dfIntersect will consist of the accountNames that have purchased before 2015 as well as after.
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

dfCustomer = dfCustomer.reset_index(drop=True)
#for i in range(0,len(dfCustomer.index)) :
#    if (d[dfCustomer.iloc[i]["accountName"]] == 1) :
#        dfCustomer.loc[i,'repurchase'] = True
for index, row in dfCustomer.iterrows():
    if (d[row["accountName"]] == 1) :
        row["repurchase"] = True

print(dfCustomer)
dfCustomer.to_csv(path_or_buf="customer_data2.csv", index=False)




dfCountries = pd.DataFrame()
dfCountries["country"]              = df1["ipCountry"]
dfCountries["numberOfTransactions"] = df1.groupby('ipCountry')['ipCountry'].transform('count')
dfCountries.drop_duplicates(subset=["country"],inplace=True)
dfCountries = dfCountries.sort_values(by=['numberOfTransactions'], ascending=False)
dfCountries = dfCountries[pd.notnull(dfCountries["country"])]  # see https://stackoverflow.com/questions/13413590/how-to-drop-rows-of-pandas-dataframe-whose-value-in-certain-columns-is-nan
dfCountries = dfCountries.reset_index(drop=True)
dfCountries["rank"] = range(0,len(dfCountries.index))
dfCountries = dfCountries.drop('numberOfTransactions', 1)

dfCountries.to_csv(path_or_buf="countryRanks.csv", index=False)

#print(dfCountries)
