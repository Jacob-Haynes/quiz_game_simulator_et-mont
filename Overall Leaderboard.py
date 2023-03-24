# Databricks notebook source
import pyspark.sql.functions  as psf

# COMMAND ----------

#data import
raw_sdf = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/jacob.haynes@monterosa.co/entain/20kUsers")
#explode
lb_sdf = raw_sdf.select("data", psf.explode("data").alias("leaderboard"))
lb_sdf = lb_sdf.select('leaderboard.*')
#fix the bigint
lb_sdf = lb_sdf.withColumn('score', lb_sdf.score.cast('int'))
#group by userId
lbByID_sdf = lb_sdf.groupby('userId').agg(psf.collect_list('score'))
lbByID_sdf = lbByID_sdf.withColumnRenamed('collect_list(score)', 'scores')
#sort scores and get top 3
lbTop3_sdf = lbByID_sdf.withColumn('top3', psf.slice(psf.sort_array(psf.col('scores'), asc=False), start=1, length=3))
#sum top 3 scores per user
lbSum_sdf = lbTop3_sdf.withColumn('overallScore', psf.expr('AGGREGATE(top3, 0, (acc, x) -> acc + x)'))
#sort final leaderboard
lbRanked_sdf = lbSum_sdf.sort('overallScore', ascending = False)
#export
##TODO##

# COMMAND ----------

lbRanked_sdf.printSchema()
lbRanked_sdf.show(truncate=False)

# COMMAND ----------

