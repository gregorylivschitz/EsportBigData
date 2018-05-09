from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder, CrossValidator
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, LogisticRegressionSummary


df_dota_features = spark.read.json("/user/gl758/esports_features/dota_features/*.json")

df_dota_features_ordered = df_dota_features.orderBy('start_time')

df_dota_features_ordered = df_dota_features_ordered.withColumn('label', df_dota_features_ordered['radiant_win'].cast(IntegerType()).cast(DoubleType()))

df_dota_features_ordered = df_dota_features_ordered.drop('start_time')
df_dota_features_ordered = df_dota_features_ordered.drop('match_id')
df_dota_features_ordered = df_dota_features_ordered.drop('radiant_win')
features = ['kills_avg', 'deaths_avg', 'assists_avg', 'gold_per_min_avg']
df_unnest = ["label"]
for feature in features:
    df_unnest += [df_dota_features_ordered[feature][column] for column in range(10)]

df_dota_features_unnested = df_dota_features_ordered.select(*df_unnest)

features_assembler = VectorAssembler(
    inputCols=(df_dota_features_unnested.columns[1:]),
    outputCol="features")

df_dota_features_unnested = features_assembler.transform(df_dota_features_unnested)

df_dota_features = df_dota_features_unnested.select("label", "features")

lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=6)

train, test = df_dota_features.randomSplit([0.7, 0.3], seed=777)

paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).build()

crossval = CrossValidator(estimator=lr,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=2)

cvModel = crossval.fit(train)


cvModel.transform(test).select("label", "prediction", "probability", "rawPrediction").show()

cvModel.bestModel.summary.roc.show()
cvModel.bestModel.summary.accuracy
