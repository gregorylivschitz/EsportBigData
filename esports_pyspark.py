from pyspark.sql.functions import explode, lit, col, avg, lag, collect_list,sort_array, size, struct, udf, split
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
import operator


df_pro_match_data = spark.read.json("/user/gl758/pro_matches_data/*.json")
df_pro_match_players = df_pro_match_data.select('match_id', 'start_time', 'players')
df_player = df_pro_match_players.withColumn("players", explode(df_pro_match_players.players))

df_player = df_player.withColumn("kills", df_player.players.kills)
df_player = df_player.withColumn("assists", df_player.players.assists)
df_player = df_player.withColumn("deaths", df_player.players.deaths)
df_player = df_player.withColumn("gold_per_min", df_player.players.gold_per_min)

df_player = df_player.withColumn("account_id", df_player.players.account_id)
df_player = df_player.withColumn('radiant_win', df_player.players.radiant_win)
df_player = df_player.withColumn('is_radiant', df_player.players.isRadiant.cast("integer"))
df_player_features = df_player.drop('players')


features = ['kills', 'deaths', 'assists', 'gold_per_min']
for feature in features:
    df_player_features = df_player_features.withColumn('{}_lag'.format(feature),lag(df_player_features[feature]).over(Window.partitionBy("account_id").orderBy("start_time")))
    wspec = Window.partitionBy("account_id").orderBy("start_time").rowsBetween(-9999999999,0)
    df_player_features = df_player_features.withColumn("{}_avg".format(feature), avg(df_player_features['{}_lag'.format(feature)]).over(wspec))

df_player_features_dropped = df_player_features.dropna(subset=('kills_avg','deaths_avg','assists_avg'))

features = ['kills_avg', 'deaths_avg', 'assists_avg', 'gold_per_min_avg', 'is_radiant']
# df_player_features_ordered = df_player_features.orderBy("match_id", "is_radiant")

df_player_features_ordered = df_player_features_dropped.withColumn("kills_order_col",struct(["is_radiant","kills_avg"]))
df_player_features_ordered = df_player_features_ordered.withColumn("deaths_order_col",struct(["is_radiant","deaths_avg"]))
df_player_features_ordered = df_player_features_ordered.withColumn("assists_order_col",struct(["is_radiant","assists_avg"]))
df_player_features_ordered = df_player_features_ordered.withColumn("gold_per_min_col",struct(["is_radiant","gold_per_min_avg"]))

df_stat_in_one_row = df_player_features_ordered.groupby("match_id").agg(collect_list("kills_order_col").alias("kills_order_col"), collect_list("deaths_order_col").alias("deaths_order_col"), collect_list("assists_order_col").alias("assists_order_col"), collect_list("gold_per_min_col").alias("gold_per_min_col"))

# define udf
def sorter(l):
  res = sorted(l, key=operator.itemgetter(0))
  return [item[1] for item in res]

sort_udf = udf(sorter)

df_stat_in_one_row = df_stat_in_one_row.select("match_id", sort_udf("kills_order_col").alias("kills_avg"), sort_udf("deaths_order_col").alias("deaths_avg"), sort_udf("assists_order_col").alias("assists_avg"), sort_udf("gold_per_min_col").alias("gold_per_min_avg"))


df_stat_in_one_row_array = df_stat_in_one_row.withColumn("kills_avg", split(col("kills_avg"), ",\s*").cast("array<int>").alias("kills_avg"))
df_stat_in_one_row_array = df_stat_in_one_row_array.withColumn("deaths_avg", split(col("deaths_avg"), ",\s*").cast("array<int>").alias("deaths_avg"))
df_stat_in_one_row_array = df_stat_in_one_row_array.withColumn("assists_avg", split(col("assists_avg"), ",\s*").cast("array<int>").alias("assists_avg"))
df_stat_in_one_row_array = df_stat_in_one_row_array.withColumn("gold_per_min_avg", split(col("gold_per_min_avg"), ",\s*").cast("array<int>").alias("gold_per_min_avg"))

df_only_valid_player_stats = df_stat_in_one_row_array.where(size(col("kills_avg")) == 10)