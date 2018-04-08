import pandas as pd
from collections import defaultdict
from sklearn import tree

df          = pd.read_csv(filepath_or_buffer = "customer_data2.csv")
dfCountries = pd.read_csv(filepath_or_buffer = "countryRanks.csv"  )
df["countryCode"] = len(dfCountries.index)+1

d = dict((name,None) for name in dfCountries["country"])
d = defaultdict(lambda: len(dfCountries.index)+1, d)
for index, row in dfCountries.iterrows():
    d[row["country"]] = row["rank"]
#for i in d:
#    print(i, d[i])

for index, row in df.iterrows():
    row["countryCode"] = d[row["country"]]

names = ["accountName","numberOfTransactions","totalSpending","firstTransactionDate","lastTransactionDate","lifeSpan","maxTransactionPrice","avgTransactionPrice","countryCode","repurchase"
         ]
features = df[names[1:-1]]
target   = df["repurchase"]

for items in features:
    print(items)


#for items in target:
    #print(items)

clf = tree.DecisionTreeClassifier()
clf = clf.fit(features, target)

print(clf.predict([[16,295.0,7.0,455.0,50.0,18.4375,447.0,0]]))