from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.types import IntegerType
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import explode, substring, lit, col, avg, lag, collect_list,sort_array, size, struct, udf, split, max
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.feature import VectorAssembler
import operator

# EASY TRANSFORM
df_player_features_dropped = spark.read.json('/user/gl758/esports_players_cleaned/esports_players/*.json')
df_player_features_ordered = df_player_features_dropped.withColumn("kills_order_col",struct(["is_radiant","kills_avg"]))
df_player_features_ordered = df_player_features_ordered.withColumn("deaths_order_col",struct(["is_radiant","deaths_avg"]))
df_player_features_ordered = df_player_features_ordered.withColumn("assists_order_col",struct(["is_radiant","assists_avg"]))
df_player_features_ordered = df_player_features_ordered.withColumn("gold_per_min_col",struct(["is_radiant","gold_per_min_avg"]))

df_match_id_radiant = df_player_features_ordered.select('match_id', 'radiant_win', 'start_time').distinct()
df_stat_in_one_row = df_player_features_ordered.groupby("match_id").agg(collect_list("kills_order_col").alias("kills_order_col"), collect_list("deaths_order_col").alias("deaths_order_col"), collect_list("assists_order_col").alias("assists_order_col"), collect_list("gold_per_min_col").alias("gold_per_min_col"))

df_players_features_with_win = df_stat_in_one_row.join(df_match_id_radiant, "match_id")

def sorter(l):
    res = sorted(l, key=operator.itemgetter(0))
    items= [item[1] for item in res]
    return items

sort_udf = udf(sorter, ArrayType(DoubleType()))

df_players_features_with_win_in_one_row = df_players_features_with_win.select("match_id", "radiant_win","start_time", sort_udf("kills_order_col").alias("kills_avg"), sort_udf("deaths_order_col").alias("deaths_avg"), sort_udf("assists_order_col").alias("assists_avg"), sort_udf("gold_per_min_col").alias("gold_per_min_avg"))

df_dota_features = df_players_features_with_win_in_one_row.where(size(col("kills_avg")) == 10)



#MACHINE LEARNING MODEL
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
cvModel.bestModel.summary.accuracy.show()


# PREDICT A TEAM GAME
df_player_features_dropped = spark.read.json('/user/gl758/esports_players_cleaned/esports_players_no_lag/*.json')
df_find_by_team_id_secret = spark.read.json('/user/gl758/data_all/pro_team_players_data/1838315.json', mode='DROPMALFORMED')
df_find_by_team_id_keen_gaming = spark.read.json('/user/gl758/data_all/pro_team_players_data/2626685.json', mode='DROPMALFORMED')
df_find_by_team_id_secret = df_find_by_team_id_secret.select("account_id", lit(1).alias("is_radiant_predict"))
df_find_by_team_id_keen_gaming = df_find_by_team_id_keen_gaming.select("account_id", lit(0).alias("is_radiant_predict"))
df_2_team_union = df_find_by_team_id_secret.union(df_find_by_team_id_keen_gaming)
df_players_join_2_teams = df_player_features_dropped.join(df_2_team_union, "account_id")

df_players_last_start_time = df_players_join_2_teams.groupBy('account_id').agg(max('start_time').alias('start_time'))
df_players_features = df_players_last_start_time.join(df_players_join_2_teams, ["account_id", "start_time"])

df_player_features_for_2_teams = df_players_features.withColumn('match_id_predict', lit(1))
df_player_features_ordered = df_player_features_for_2_teams.withColumn("kills_order_col",struct(["is_radiant_predict","kills_avg"]))
df_player_features_ordered = df_player_features_ordered.withColumn("deaths_order_col",struct(["is_radiant_predict","deaths_avg"]))
df_player_features_ordered = df_player_features_ordered.withColumn("assists_order_col",struct(["is_radiant_predict","assists_avg"]))
df_player_features_ordered = df_player_features_ordered.withColumn("gold_per_min_col",struct(["is_radiant_predict","gold_per_min_avg"]))

df_match_id_radiant = df_player_features_ordered.select('match_id_predict', 'radiant_win', 'start_time').distinct()
df_stat_in_one_row = df_player_features_ordered.groupby("match_id_predict").agg(collect_list("kills_order_col").alias("kills_order_col"), collect_list("deaths_order_col").alias("deaths_order_col"), collect_list("assists_order_col").alias("assists_order_col"), collect_list("gold_per_min_col").alias("gold_per_min_col"))
def sorter(l):
    res = sorted(l, key=operator.itemgetter(0))
    items= [item[1] for item in res]
    return items
sort_udf = udf(sorter, ArrayType(DoubleType()))

df_players_features_with_win_in_one_row = df_stat_in_one_row.select(sort_udf("kills_order_col").alias("kills_avg"), sort_udf("deaths_order_col").alias("deaths_avg"), sort_udf("assists_order_col").alias("assists_avg"), sort_udf("gold_per_min_col").alias("gold_per_min_avg"))

df_only_valid_player_stats = df_players_features_with_win_in_one_row.where(size(col("kills_avg")) == 10)

# Begin Setting up for Machine Learning
features = ['kills_avg', 'deaths_avg', 'assists_avg', 'gold_per_min_avg']
df_unnest = []
for feature in features:
    df_unnest += [df_only_valid_player_stats[feature][column] for column in range(10)]

df_dota_features_unnested = df_only_valid_player_stats.select(*df_unnest)

features_assembler = VectorAssembler(
    inputCols=(df_dota_features_unnested.columns[:]),
    outputCol="features")

df_dota_features_unnested = features_assembler.transform(df_dota_features_unnested)
df_dota_features = df_dota_features_unnested.select("features")


cvModel.transform(df_dota_features).select("prediction", "probability", "rawPrediction").show()