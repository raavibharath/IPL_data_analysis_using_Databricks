# Databricks notebook source
# MAGIC %md
# MAGIC # IPL data analysis using spark and spark sql 

# COMMAND ----------

from pyspark.sql import SparkSession
# creating session
spark = SparkSession.builder.appName('IPL_data_analysis').getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType,DecimalType

# Define the schema for the ball by ball dataset
Ball_by_Ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])


# COMMAND ----------

file_path= '/FileStore/tables/Ball_By_Ball.csv'
BBB_df = spark.read.schema(Ball_by_Ball_schema).csv(file_path, header=True)
display(BBB_df.limit(10))

# COMMAND ----------

#schema for match dataset
match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),  
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])


# COMMAND ----------

file_path= '/FileStore/tables/Match.csv'
match_df = spark.read.schema(match_schema).csv(file_path, header=True)
display(match_df.limit(10))


# COMMAND ----------

#schme for player datset
player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

file_path= '/FileStore/tables/Player.csv'
player_df= spark.read.schema(player_schema).csv(file_path, header=True)
display(player_df.limit(10))

# COMMAND ----------

#schema for player_match dataset
player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(10, 2), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", StringType(), True),  
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])


# COMMAND ----------

file_path= '/FileStore/tables/Player_match.csv'
player_match_df= spark.read.schema(player_match_schema).csv(file_path, header=True)
display(player_match_df.limit(10))

# COMMAND ----------

#schema for team dataset
team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id",IntegerType(),True),
    StructField("team_name", StringType(),True)
])

# COMMAND ----------

file_path= '/FileStore/tables/Team.csv'
team_df= spark.read.schema(team_schema).csv(file_path, header=True)
display(team_df.limit(10))

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformation on dataset starts from here 

# COMMAND ----------

#transformartions starts from here 
from pyspark.sql.functions import *
 #we are trying to eliminate wides and noballs. The resulting DataFrame BBB contains only valid deliveries.
BBB=BBB_df.filter((col("wides")==0) & (col("noballs")==0))
# Aggregation: Calculate the total and average runs scored in each match and inning
total_and_avg_runs = BBB_df.groupBy("match_id", "innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("average_runs")
)

# Window Function: Calculate running total of runs in each match for each over
windowSpec = Window.partitionBy("match_id","innings_no").orderBy("over_id")

BBB=BBB_df.withColumn(
    "running_total_runs",
    sum("runs_scored").over(windowSpec)
)

# Conditional Column: Flag for high impact balls (either a wicket or more than 6 runs including extras)
BBB=BBB_df.withColumn(
    "high_impact",
    when((col("runs_scored") + col("extra_runs") > 6) | (col("bowler_wicket") == True), True).otherwise(False)
)
display(BBB)

# COMMAND ----------

# Calculating Total Runs Scored by Each Player in a Season
total_runs_by_player = BBB_df.groupby(
    'match_id', 'season', 'Innings_NO'
).agg(
    sum('runs_scored').alias('total_runs')
).where(col('season') == 2017)
display(total_runs_by_player)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, when, col

# Assuming df_match is already defined and loaded with data
# For example, df_match = spark.read.format("csv").option("header", "true").load("path_to_csv_file")

# Extracting year, month, and day from the match date for more detailed time-based analysis
match_df = match_df.withColumn("year", year("match_date"))
match_df = match_df.withColumn("month", month("match_date"))
match_df = match_df.withColumn("day", dayofmonth("match_date"))

# High margin win: categorizing win margins into 'high', 'medium', and 'low'
match_df = match_df.withColumn(
    "win_margin_category",
    when((col("win_margin") >= 100), "High")
    .when((col("win_margin") >= 50) & (col("win_margin") < 100), "Medium")
    .otherwise("Low")
)

# Analyze the impact of the toss: who wins the toss and the match
match_df = match_df.withColumn(
    "toss_match_winner",
    when(col("toss_winner") == col("match_winner"), "Yes").otherwise("No")
)

# Show the enhanced match DataFrame
display(match_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import lower, regexp_replace

# Normalize and clean player names
player_df = player_df.withColumn("player_name", lower(regexp_replace("player_name", "[^a-zA-Z0-9 ]", "")))

# Handle missing values in 'batting_hand' and 'bowling_skill' with a default 'unknown'
player_df = player_df.na.fill({"batting_hand": "unknown", "bowling_skill": "unknown"})

# Categorizing players based on batting hand
player_df = player_df.withColumn(
    "batting_style",
    when(col("batting_hand").contains("Left-hand bat"), "Left-Handed").otherwise("Right-Handed")
)

# Show the modified player DataFrame
display(player_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, when, current_date, expr
# Add a 'veteran_status' column based on player age
player_match_df = player_match_df.withColumn(
     "veteran_status", 
     when(col("age_as_on_match")>=35,"Veteran").otherwise("Non-Veteran") 
)
# Dynamic column to calculate years since debut
player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date())-(col("season_year")))
)
display(player_match_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #SQL starts from here. 

# COMMAND ----------

#creating temp view to perform SQL quaries on data Frame
BBB_df.createOrReplaceTempView("ball_by_ball")
player_df.createOrReplaceTempView("player")
player_match_df.createOrReplaceTempView("player_match")
match_df.createOrReplaceTempView("match")
team_df.createOrReplaceTempView("team")


# COMMAND ----------

top_scoring_batsmen_per_season = spark.sql("""
    SELECT player_name, season_year, total_runs
    FROM (
        SELECT p.player_name, m.season_year,
               SUM(b.runs_scored) AS total_runs,
               ROW_NUMBER() OVER (PARTITION BY m.season_year ORDER BY SUM(b.runs_scored) DESC) AS rank
        FROM ball_by_ball b
        JOIN match m ON b.match_id = m.match_id
        JOIN player_match pm ON m.match_id = pm.match_id AND b.striker = pm.player_id
        JOIN player p ON p.player_id = pm.player_id
        GROUP BY p.player_name, m.season_year
    ) ranked
    WHERE rank = 1
    ORDER BY season_year
""")

display(top_scoring_batsmen_per_season)

# COMMAND ----------

top_Wicket_taker_per_season = spark.sql("""
    SELECT player_name, season_year, total_wickets
    FROM (
        SELECT 
            p.player_name, 
            m.season_year,
            SUM(CAST(b.bowler_wicket AS INT)) AS total_wickets,
            ROW_NUMBER() OVER (
                PARTITION BY m.season_year 
                ORDER BY SUM(CAST(b.bowler_wicket AS INT)) DESC
            ) AS rank
        FROM ball_by_ball b
        JOIN match m ON b.match_id = m.match_id
        JOIN player_match pm ON m.match_id = pm.match_id AND b.striker = pm.player_id
        JOIN player p ON p.player_id = pm.player_id
        GROUP BY p.player_name, m.season_year
    ) AS ranked 
    WHERE rank = 1 
    ORDER BY season_year
""")
display(top_Wicket_taker_per_season)

# COMMAND ----------

economic_runs = spark.sql("""
    SELECT 
        p.player_name, 
        AVG(b.runs_scored) AS economic_runs,
        SUM(CAST(b.bowler_wicket AS INT)) AS total_wickets
    FROM 
        ball_by_ball b 
    JOIN 
        player_match pm 
        ON b.match_id = pm.match_id 
        AND b.striker = pm.player_id
    JOIN 
        player p 
        ON pm.player_id = p.player_id
    WHERE 
        b.over_id <= 6 
    GROUP BY 
        p.player_name 
    HAVING 
        COUNT(*) >= 1
    ORDER BY 
        economic_runs, 
        total_wickets DESC
""")
display(economic_runs)

# COMMAND ----------

toss_impact_individual_matches = spark.sql("""
SELECT m.match_id, m.toss_winner, m.toss_name, m.match_winner,
       CASE WHEN m.toss_winner = m.match_winner THEN 'Won' ELSE 'Lost' END AS match_outcome
FROM match m
WHERE m.toss_name IS NOT NULL
ORDER BY m.match_id
""")
display(toss_impact_individual_matches)


# COMMAND ----------

import matplotlib.pyplot as plt
# Assuming 'top_scoring_batsmen_per_season' is already executed and available as a Spark DataFrame
top_scoring_batsmen_per_season_pd = top_scoring_batsmen_per_season.toPandas()
# Verify the column names
print(top_scoring_batsmen_per_season_pd.columns)
# Limiting to top 10 for clarity in the plot
top_scoring_batsmen_per_season_pd = top_scoring_batsmen_per_season_pd.nlargest(10, 'total_runs')
# Visualizing using Matplotlib
plt.figure(figsize=(12, 8))
plt.bar(
    top_scoring_batsmen_per_season_pd['player_name'], 
    top_scoring_batsmen_per_season_pd['total_runs'],
    color='skyblue'
)
plt.xlabel('Batter Name')
plt.ylabel('Average Runs per Ball')
plt.title('Most Economical Bowlers in Powerplay Overs (Top 10)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

import seaborn as sns

sns.countplot(x='toss_winner', hue='match_outcome', data=toss_impact_pd)
plt.title('Impact of Winning Toss on Match Outcomes')
plt.xlabel('Toss Winner')
plt.ylabel('Number of Matches')
plt.legend(title='Match Outcome')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

