from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_validate
from sklearn.model_selection import cross_val_predict
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.metrics import plot_confusion_matrix
import matplotlib.pyplot as plt
import pandas
import numpy as np

def dataExtraction(path):
    df = pandas.read_csv(path)
    df.drop(df.columns[0], axis=1, inplace=True)
    df.reindex(np.random.permutation(df.index))

    X_df = df[df.columns[0:3]]
    y_df = df[df.columns[3:4]]  

    X = X_df.to_numpy()
    y = y_df.to_numpy()

    return X,y

def logisticRegression(X, y):
    clf = LogisticRegression(random_state=0).fit(X, y)

    return clf

def randomForest(X, y):
    clf = RandomForestClassifier(max_depth=5, random_state=1).fit(X, y)

    return clf

def crossFoldValidation(X, y, model):
    cv_results = cross_validate(model, X, y, cv=5)

    return cv_results

def plot(size, distribution, y):
    plt.scatter(size, distribution, c=y)
    plt.show()

    return None

def main():
    path = '../data/data1.csv'
    X, y = dataExtraction(path)
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=5)
    logistic_regression = logisticRegression(X_train, y_train)
    random_forest = randomForest(X_train, y_train)
    plot_confusion_matrix(random_forest, X_test, y_test)
    plt.show()
    # lr_validation = crossFoldValidation(X, y, logistic_regression)
    # rf_validation = crossFoldValidation(X, y, random_forest)
    #plot(X[:,0], X[:,2], y)
    #y_pred = cross_val_predict(clf, x, y, cv=10)

    # print("Logistic Regression CV Scores: \n", lr_validation['test_score'])
    # print("Random Forest CV Scores: \n", rf_validation['test_score'])


if __name__ == '__main__':
    main()