from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_validate
from sklearn.model_selection import cross_val_predict
from sklearn.metrics import confusion_matrix
import pandas
import numpy as np

def dataExtraction(path):
    df = pandas.read_csv(path)
    df.drop(df.columns[0], axis=1, inplace=True)
    #df.reindex(np.random.permutation(df.index))

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
    cv_results = cross_validate(model, X, y, cv=3)

    return cv_results

def main():
    path = '../data/data.csv'
    X, y = dataExtraction(path)
    logistic_regression = logisticRegression(X, y)
    random_forest = randomForest(X, y)
    lr_validation = crossFoldValidation(X, y, logistic_regression)
    rf_validation = crossFoldValidation(X, y, random_forest)
    #y_pred = cross_val_predict(clf, x, y, cv=10)

    print("Logistic Regression CV Scores: \n", lr_validation['test_score'])
    print("Random Forest CV Scores: \n", rf_validation['test_score'])


if __name__ == '__main__':
    main()