%pyspark
import pyspark
from pyspark.sql.types import ArrayType
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import explode, lit, col, avg, lag, col, udf
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.linalg import Vectors


from pyspark.sql.functions import explode, lit, col, avg, lag, collect_list,sort_array, size, struct, udf, split


#labels = ["label", "weight", "p1kills", "p1assists", "p1deaths", "p1goldpm", "p2kills", "p2assists", "p2deaths", "p2goldpm", "p3kills", "p3assists", "p3deaths", "p3goldpm", "p4kills", "p4assists", "p4deaths", "p4goldpm", "p5kills", "p5assists", "p5deaths", "p5goldpm", "p6kills", "p6assists", "p6deaths", "p6goldpm", "p7kills", "p7assists", "p7deaths", "p7goldpm", "p8kills", "p8assists", "p8deaths", "p8goldpm", "p9kills", "p9assists", "p9deaths", "p9goldpm", "p10kills", "p10assists", "p10deaths", "p10goldpm"]

#train_vals = [(1.0, 1.0, 10.0, 94.0, 58.0, 79.0,90.0, 59.0, 83.0, 95.0,103.0, 96.0, 48.0, 59.0, 10.0, 39.0, 85.0, 39.0,17.0, 94.0, 87.0, 44.0,140.0, 6.0, 84.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 58.0, 39.0, 103.0, 92.0, 83.0, 98.0, 104.0, 96.0, 84.0, 94.0),
        (1.0, 1.0, 10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0, 10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0, 10.0, 9.0, 8.0, 9.0, 10.0, 9.0, 8.0, 9.0),
        (0.0, 1.0, 130.0, 94.0, 68.0, 79.0,106.0, 98.0, 82.0, 95.0,170.0, 59.0, 98.0, 79.0, 180.0, 59.0, 83.0, 99.0,210.0, 98.0, 855.0, 92.0,104.0, 29.0, 87.0, 93.0,120.0, 93.0, 85.0, 98.0,130.0, 92.0, 88.0, 29.0, 140.0, 29.0, 28.0, 79.0, 150.0, 97.0, 84.0, 96.0)]
        

#test_vals = [(0.0, 3.0, 55.0, 94.0, 85.0, 95.0,104.0, 59.0, 48.0, 95.0,104.0, 92.0, 88.0, 94.0, 710.0, 89.0, 58.0, 83.0,101.0, 49.0, 68.0, 79.0,510.0, 39.0, 84.0, 29.0,1440.0, 97.0, 88.0, 59.0,140.0, 95.0, 84.0, 29.0, 180.0, 49.0, 82.0, 94.0, 105.0, 91.0, 18.0, 59.0),
        (1.0, 1.0, 10.0, 9.0, 7.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0, 10.0, 5.0, 8.0, 9.0,16.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0, 10.0, 9.0, 8.0, 9.0, 10.0, 9.0, 8.0, 9.0),
        (0.0, 1.0, 10.0, 9.0, 3.0, 9.0,10.0, 9.0, 8.0, 6.0,10.0, 9.0, 8.0, 9.0, 10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0,10.0, 9.0, 8.0, 9.0, 10.0, 9.0, 8.0, 9.0, 10.0, 9.0, 8.0, 9.0)]
        

#mock_df = sqlContext.createDataFrame(train_vals, labels)

#mock_test_df = sqlContext.createDataFrame(test_vals, labels)

#assembler = VectorAssembler(
    inputCols=mock_df.columns[2:],
    outputCol="features")
    
df_dota_features = sqlContext.read.json("/user/gl758/esports_features/dota_features/*.json")


df_dota_features_ordered = df_dota_features.orderBy('start_time')




df_dota_features_ordered = df_dota_features_ordered.withColumn('label', df_dota_features_ordered['radiant_win'].cast(IntegerType()).cast(DoubleType()))

df_dota_features_ordered = df_dota_features_ordered.drop('start_time')
df_dota_features_ordered = df_dota_features_ordered.drop('match_id')
df_dota_features_ordered = df_dota_features_ordered.drop('radiant_win')
df_dota_features_ordered.show()

seqAsVector = udf((xs: Seq[Double]) for Vectors.dense(xs.toArray))

df_final = df.withColumn("features", seqAsVector(col("value")))

features_assembler = VectorAssembler(
    inputCols=(df_dota_features_ordered.columns[0:4]).cast(ArrayType()),
    outputCol="features")

#mock_df_with_features = assembler.transform(mock_df)

#mock_test_df_with_features = assembler.transform(mock_test_df)

df_dota_features_ordered.show()
df_dota_features_ordered = features_assembler.transform(df_dota_features_ordered)

df_dota_features_ordered.show()

#mock_test_df_with_features = assembler.transform(mock_test_df)

#mock_df_with_features.show()

lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=6)

lr_model = lr.fit(df_dota_features_ordered)

predictions = lr_model.transform(df_dota_features_ordered);

predictions.show(truncate=False)
