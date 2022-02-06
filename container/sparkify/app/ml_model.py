# This Python 3 environment comes with many helpful analytics libraries installed

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col

from pyspark.ml.regression import LinearRegression
from pyspark.mllib.evaluation import RegressionMetrics

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, CrossValidatorModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
import pickle

import matplotlib.pyplot as plt
import seaborn as sns

from scipy import stats
from scipy.stats import norm, skew 
import os
from pyspark.ml.feature import VectorAssembler


def compute_model():
    spark_session = SparkSession.builder.master("local[2]").appName("HousingRegression").getOrCreate()
    spark_context = spark_session.sparkContext
    spark_sql_context = SQLContext(spark_context)

    #File path 
    TRAIN_INPUT = "app/data/train.csv"
    TEST_INPUT = "app/data/test.csv"
    #Read file to pandas dataframe
    pd_train = pd.read_csv(TRAIN_INPUT)
    pd_test = pd.read_csv(TEST_INPUT)
    #Check all missing values and save those columns to a list
    na_cols = pd_train.columns[pd_train.isna().any()].tolist()
    #Check the sum of all null values in columns in df and append to list
    total = pd_train.isnull().sum().sort_values(ascending=False)
    percent = (pd_train.isnull().sum()/pd_train.shape[0]).sort_values(ascending=False)
    #Check and drop missing values
    missing = pd.concat([total, percent], axis=1, keys=['Total', 'Perc_missing'])
    missing.head(15)

    pd_train = pd_train.drop((missing[missing['Perc_missing'] >= 0.15]).index,1)
    pd_train.head()

    pd_train['New'] = pd_train['OverallQual'] * pd_train['GarageArea'] * pd_train['GrLivArea']
    pd_test['New'] = pd_test['OverallQual'] * pd_test['GarageArea'] * pd_test['GrLivArea']

    train_cols = list(pd_train.columns)
    train_cols.remove('SalePrice')

    #Make test ds feature set same as in train ds
    pd_test = pd_test[train_cols]


    # Fill the NA-values with "None"/0, for most of the features 
    # in the particular data, it literally means "None"/0 (e.g. Garage Area, Garage Type, Condition) as the house
    # probably doesn't have the garage.

    for col in ['BsmtQual', 'BsmtCond', 'BsmtExposure', 'BsmtFinType1', 'BsmtFinType2']:
        pd_train[col] = pd_train[col].fillna("None")
        pd_test[col] = pd_test[col].fillna("None")

    for col in ['GarageType', 'GarageFinish', 'GarageQual', 'GarageCond']:
        pd_train[col] = pd_train[col].fillna("None")
        pd_test[col] = pd_test[col].fillna("None")

    for col in ['GarageYrBlt', 'GarageArea', 'GarageCars']:
        pd_train[col] = pd_train[col].fillna(0)
        pd_test[col] = pd_test[col].fillna(0)

    pd_train['MasVnrType'] = pd_train['MasVnrType'].fillna("None")
    pd_test['MasVnrType'] = pd_test['MasVnrType'].fillna("None")

    pd_train['MasVnrArea'] = pd_train['MasVnrArea'].fillna(0)
    pd_test['MasVnrArea'] = pd_test['MasVnrArea'].fillna(0)

    pd_train['Electrical'] = pd_train['Electrical'].fillna(pd_train['Electrical'].mode()[0])
    pd_test['Electrical'] = pd_test['Electrical'].fillna(pd_test['Electrical'].mode()[0])

    print(pd_train.isnull().sum().max()) # check if any missing values are left
    print(pd_test.isnull().sum().max())


    pd_test['BsmtFinSF1'] = pd_test['BsmtFinSF1'].fillna(pd_test['BsmtFinSF1'].mean())
    pd_test['BsmtFinSF2'] = pd_test['BsmtFinSF2'].fillna(pd_test['BsmtFinSF2'].mean())
    pd_test['BsmtUnfSF'] = pd_test['BsmtUnfSF'].fillna(pd_test['BsmtUnfSF'].mean())
    pd_test['TotalBsmtSF'] = pd_test['TotalBsmtSF'].fillna(pd_test['TotalBsmtSF'].mean())
    pd_test['BsmtFullBath'] = pd_test['BsmtFullBath'].fillna(pd_test['BsmtFullBath'].mean())
    pd_test['BsmtHalfBath'] = pd_test['BsmtHalfBath'].fillna(pd_test['BsmtHalfBath'].mean())


    cat_columns = pd_train.select_dtypes(include=['object']).columns
    pd_train[cat_columns] = pd_train[cat_columns].fillna('NoData')
    pd_test[cat_columns] = pd_test[cat_columns].fillna('NoData')


    print("Dropping outliers resulted in %d instances in the new dataset" % len(pd_train))
    pd_train = pd_train.drop(pd_train[(pd_train['GrLivArea']>4500) 
                                    & (pd_train['SalePrice']<300000)].index)
    print("Dropping outliers resulted in %d instances in the new dataset" % len(pd_train))
    pd_train = pd_train.drop(pd_train[(pd_train['GrLivArea']>5500) 
                                    | (pd_train['SalePrice']>500000)].index)
    print("Dropping outliers resulted in %d instances in the new dataset" % len(pd_train))
    pd_train = pd_train.drop(pd_train[pd_train['GarageArea']>1100].index)
    print("Dropping outliers resulted in %d instances in the new dataset" % len(pd_train))


    print("Dropping outliers resulted in %d instances in the new dataset" % len(pd_train))
    pd_train = pd_train.drop(pd_train[(pd_train['GrLivArea']>4500) 
                                    & (pd_train['SalePrice']<300000)].index)
    print("Dropping outliers resulted in %d instances in the new dataset" % len(pd_train))
    pd_train = pd_train.drop(pd_train[(pd_train['GrLivArea']>5500) 
                                    | (pd_train['SalePrice']>500000)].index)
    print("Dropping outliers resulted in %d instances in the new dataset" % len(pd_train))
    pd_train = pd_train.drop(pd_train[pd_train['GarageArea']>1100].index)
    print("Dropping outliers resulted in %d instances in the new dataset" % len(pd_train))

    train_df = spark_session.createDataFrame(pd_train)
    test_df = spark_session.createDataFrame(pd_test)

    train_df = train_df.select([c for c in train_df.columns if c not in na_cols])
    train_cols = train_df.columns
    train_cols.remove('SalePrice')
    test_df = test_df.select(train_cols)


    

    # Reconfigure for py spark dfs
    from pyspark.sql.types import IntegerType

    test_df = test_df.withColumn("BsmtFinSF1", test_df["BsmtFinSF1"].cast(IntegerType()))
    test_df = test_df.withColumn("BsmtFinSF2", test_df["BsmtFinSF2"].cast(IntegerType()))
    test_df = test_df.withColumn("BsmtUnfSF", test_df["BsmtUnfSF"].cast(IntegerType()))
    test_df = test_df.withColumn("TotalBsmtSF", test_df["TotalBsmtSF"].cast(IntegerType()))
    test_df = test_df.withColumn("BsmtFullBath", test_df["BsmtFullBath"].cast(IntegerType()))
    test_df = test_df.withColumn("BsmtHalfBath", test_df["BsmtHalfBath"].cast(IntegerType()))
    test_df = test_df.withColumn("GarageCars", test_df["GarageCars"].cast(IntegerType()))
    test_df = test_df.withColumn("GarageArea", test_df["GarageArea"].cast(IntegerType()))

    # Defining string columns to pass on to the String Indexer (= categorical feature encoding)

    train_string_columns = []

    for col, dtype in train_df.dtypes:
        if dtype == 'string':
            train_string_columns.append(col)



    indexers = [StringIndexer(inputCol=column, outputCol=column+'_index', handleInvalid='keep').fit(train_df) for column in train_string_columns]


    pipeline = Pipeline(stages=indexers)
    train_indexed = pipeline.fit(train_df).transform(train_df)

    test_string_columns = []

    for col, dtype in test_df.dtypes:
        if dtype == 'string':
            test_string_columns.append(col)


    indexers2 = [StringIndexer(inputCol=column, outputCol=column+'_index', handleInvalid='keep').fit(test_df) for column in test_string_columns]

    pipeline2 = Pipeline(stages=indexers2)
    test_indexed = pipeline2.fit(test_df).transform(test_df)


    def get_dtype(df,colname):
        return [dtype for name, dtype in df.dtypes if name == colname][0]

    num_cols_train = []
    for col in train_indexed.columns:
        if get_dtype(train_indexed,col) != 'string':
            num_cols_train.append(str(col))

    num_cols_test = []
    for col in test_indexed.columns:
        if get_dtype(test_indexed,col) != 'string':
            num_cols_test.append(str(col))

    train_indexed = train_indexed.select(num_cols_train)
    test_indexed = test_indexed.select(num_cols_test)

    
    vectorAssembler = VectorAssembler(inputCols = train_indexed.drop("SalePrice").columns, outputCol = 'features').setHandleInvalid("keep")

    train_vector = vectorAssembler.transform(train_indexed)

    vectorAssembler2 = VectorAssembler(inputCols = test_indexed.columns, outputCol = 'features').setHandleInvalid("keep")

    test_vector = vectorAssembler2.transform(test_indexed)

    from pyspark.sql.functions import lit

    test_vector = test_vector.withColumn("SalePrice", lit(0))

    # Train-test split 70-30 split

    splits = train_vector.randomSplit([0.7, 0.3])
    train = splits[0]
    val = splits[1]

    # Using Linear Regression

    from pyspark.ml.regression import LinearRegression

    lr = LinearRegression(featuresCol = 'features', labelCol='SalePrice', maxIter=10, 
                          regParam=0.8, elasticNetParam=0.1) # It is always a good idea to play with hyperparameters.
    lr_model = lr.fit(train)


    trainingSummary = lr_model.summary
    print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    print("r2: %f" % trainingSummary.r2)

    lr_predictions = lr_model.transform(val)
    lr_predictions.select("prediction","SalePrice","features").show(5)

    from pyspark.ml.evaluation import RegressionEvaluator
    lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                     labelCol="SalePrice",metricName="r2")

    print("R Squared (R2) on val data = %g" % lr_evaluator.evaluate(lr_predictions))

    #lr_model.write().overwrite().save(("lr_model"))

    pred = lr_predictions.select("Id","prediction")
    pred = pred.withColumnRenamed("prediction","SalePrice")

    from pyspark.sql.types import FloatType, IntegerType

    #pred.printSchema()
    pred = pred.withColumn("Id", pred["Id"].cast(IntegerType()))
    pred = pred.withColumn("SalePrice", pred["SalePrice"].cast(FloatType()))

    pred_pd = pred.toPandas()
    save = pred_pd.to_csv("submission.csv", index=False)
    os.system(f"sudo chmod 777 {save}")
    save