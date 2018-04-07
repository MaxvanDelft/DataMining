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
import time

print("Hello World")

df = pd.read_csv('sales.csv', names=['saleId','saleDateTime','accountName','coins','currency','priceInCurrency','priceInEUR','methodId','ip','ipCountry'])
df['saleDateTime'] = pd.to_datetime(df['saleDateTime'])
#dfTrainingSet = df[df['saleDateTime'] < datetime.datetime(2014,01,01)]
#dfValidationSet = df[df['saleDateTime'] >= datetime.datetime(2014,01,01)]
dfTrainingSet = df[df['saleDateTime'] < pd.Timestamp('20140101')]
dfValidationSet = df[df['saleDateTime'] >= pd.Timestamp('20140101')]

dfBefore = pd.DataFrame()
dfBefore["accountName"] = dfTrainingSet["accountName"]
dfAfter = pd.DataFrame()
dfAfter["accountName"] = dfValidationSet["accountName"]

df2 = pd.DataFrame()
#df2 = df.groupby('accountName').agg('count')
#df2 = df['accountName'].value_counts()
#print df2
df2['accountName'] = dfTrainingSet['accountName']
df2['numberOfTransactions'] = dfTrainingSet.groupby('accountName')['accountName'].transform('count')                                                       #1
df2['totalSpending'] = dfTrainingSet.groupby('accountName')['priceInEUR'].transform('sum')                                                                 #2

df2['firstTransactionDate'] = dfTrainingSet.groupby('accountName')['saleDateTime'].transform('min')                                                        #3
df2['lastTransactionDate'] = dfTrainingSet.groupby('accountName')['saleDateTime'].transform('max')                                                         #4
df2['lifeSpan'] = df2['lastTransactionDate'] - df2['firstTransactionDate']                                                                                 #5
df2['firstTransactionDate'] = df2['firstTransactionDate'] - pd.Timestamp('20100101')
df2['lastTransactionDate']  = df2['lastTransactionDate']  - pd.Timestamp('20100101')

df2['firstTransactionDate'] = df2['firstTransactionDate'].astype('timedelta64[D]') # extract days from timedelta
df2['lastTransactionDate']  = df2['lastTransactionDate'].astype('timedelta64[D]')
df2['lifeSpan']             = df2['lifeSpan'].astype('timedelta64[D]')

df2['maxTransactionPrice'] = dfTrainingSet.groupby('accountName')['priceInEUR'].transform('max')                                                           #6
df2['avgTransactionPrice'] = dfTrainingSet.groupby('accountName')['priceInEUR'].transform(np.mean)                                                         #7 taken from https://stackoverflow.com/questions/22072943/faster-way-to-transform-group-with-mean-value-in-pandas/22073449
df2['country'] = dfTrainingSet['ipCountry']                                                                                                                #10
df2['repurchase'] = False
df2.drop_duplicates(subset=["accountName"],inplace=True)
#print df2

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

df2 = df2.reset_index(drop=True)
for i in range(0,len(df2.index)) :
  if (d[df2.iloc[i]["accountName"]] == 1) :
    df2.loc[i,'repurchase'] = True

print(df2)
df2.to_csv(path_or_buf="costumer_data2.csv", index=False)



