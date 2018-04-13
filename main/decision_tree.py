import pandas as pd
from collections import defaultdict
from sklearn import tree
from sklearn import cross_validation
from sklearn.metrics import confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn import svm
import matplotlib.pyplot as plt

df          = pd.read_csv(filepath_or_buffer = "customer_data.csv")
#dfCountries = pd.read_csv(filepath_or_buffer = "countryRanks.csv" )

names = ["accountName","numberOfTransactions","totalSpending","firstTransactionDate","lastTransactionDate","lifeSpan","maxTransactionPrice","avgTransactionPrice","countryCode","spendingLastMonth","spending2ndLastMonth","spending3rdLastMonth","spending4thLastMonth","spending5thLastMonth","spending6thLastMonth","repurchase"]
#names = ["accountName","spendingLastMonth","spending2ndLastMonth","spending3rdLastMonth","spending4thLastMonth","spending5thLastMonth","spending6thLastMonth","repurchase"]
#names = ["accountName","numberOfTransactions","totalSpending","firstTransactionDate","lastTransactionDate","lifeSpan","maxTransactionPrice","avgTransactionPrice","countryCode","spendingLastWeek","spendingLast2Weeks","spendingLastMonth","spendingLast2Months","spendingLast4Months","spendingLast8Months","repurchase"]
#names = ["accountName","spendingLastWeek","spendingLast2Weeks","spendingLastMonth","spendingLast2Months","spendingLast4Months","spendingLast8Months","repurchase"]
features = df[names[1:-1]]
target   = df["repurchase"]

X = features
y = target

X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.33, random_state=42)

clf = tree.DecisionTreeClassifier(max_depth=10)
clf = clf.fit(X_train, y_train)

#print(clf.predict([[6,25.0,1352.0,1394.0,10.0,4.166666666666667,42.0,0,0.0,0.0,5.0,20.0,0.0,0.0]]))






y_pred = clf.predict(X_test)
cnf_matrix = confusion_matrix(y_test, y_pred)
print(cnf_matrix)


rf_clf = RandomForestClassifier(n_estimators=10, max_depth = 4)
rf_clf = rf_clf.fit(X_train, y_train)
y_pred = rf_clf.predict(X_test)
cnf_matrix = confusion_matrix(y_test, y_pred)
print(cnf_matrix)

scores = cross_validation.cross_val_score(rf_clf, X_test, y_test, scoring='accuracy', cv=10)#.mean()*100
print(scores)

accuracy = cross_validation.cross_val_score(rf_clf, X_test, y_test, scoring='accuracy', cv = 10).mean() * 100
print("Accuracy of Random Forests is: " , accuracy)

with open("tree.dot", 'w') as f:
     f = tree.export_graphviz(rf_clf.estimators_[0], 
                              feature_names=names[1:-1], 
                              out_file=f)


rf_clf = RandomForestClassifier(n_estimators=10, max_depth = 7)
rf_clf = rf_clf.fit(X_train, y_train)
log_clf = LogisticRegression()
svm_clf = svm.SVC()


print("Random Forests: ")
crossValScore = cross_validation.cross_val_score(rf_clf, X_test, y_test, scoring='accuracy', cv = 10)
print(crossValScore)
accuracy = crossValScore.mean() * 100
print("Accuracy of Random Forests is: " , accuracy)
 
 
print("\n\nLog:")
crossValScore = cross_validation.cross_val_score(log_clf, X_test, y_test, scoring='accuracy', cv = 10)
print(crossValScore)
accuracy = crossValScore.mean() * 100
print("Accuracy of Logistic Regression is: " , accuracy)



#names = ["accountName","numberOfTransactions","totalSpending","firstTransactionDate","lastTransactionDate","lifeSpan","maxTransactionPrice","avgTransactionPrice","countryCode","spendingLastMonth","spending2ndLastMonth","spending3rdLastMonth","spending4thLastMonth","spending5thLastMonth","spending6thLastMonth","repurchase"]
#names = ["accountName","spendingLastMonth","spending2ndLastMonth","spending3rdLastMonth","spending4thLastMonth","spending5thLastMonth","spending6thLastMonth","repurchase"]
#names = ["accountName","numberOfTransactions","totalSpending","firstTransactionDate","lastTransactionDate","lifeSpan","maxTransactionPrice","avgTransactionPrice","countryCode","spendingLastWeek","spendingLast2Weeks","spendingLastMonth","spendingLast2Months","spendingLast4Months","spendingLast8Months","repurchase"]
names = ["accountName","spendingLastWeek","spendingLast2Weeks","spendingLastMonth","spendingLast2Months","spendingLast4Months","spendingLast8Months","repurchase"]
features = df[names[1:-1]]
target   = df["repurchase"]

X = features
y = target
X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.2, random_state=42)

print("\n\nSVM:")
crossValScore = cross_validation.cross_val_score(svm_clf, X_test, y_test, scoring='accuracy', cv = 10)
print(crossValScore)
accuracy = crossValScore.mean() * 100
print("Accuracy of SVM is: " , accuracy)


plt.matshow(df[names[1:]].corr())
plt.show()

