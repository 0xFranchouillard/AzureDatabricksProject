# Databricks notebook source
import pandas as pd
import re
from databricks import feature_store


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, LongType
from pyspark.sql.functions import *


spark.conf.set("spark.sql.execution.arrow.enabled","true")
fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# %sql CREATE DATABASE IF NOT EXISTS groupe5;

# COMMAND ----------

def savedata(data, table):
    data.write.format("delta").mode("overwrite").option("userMetadata", "init").saveAsTable(f"groupe404.{table}")  

# COMMAND ----------

main_players = pd.read_csv("/dbfs/mnt/groupe404/main_players.csv")
main_players.head()

# COMMAND ----------

# main_players : player_id, tag, country ?, characters--->il faut prendre seulement la plus grande valeur avec le nom du perso.
data_players = main_players[['#', 'player_id', 'tag', 'country']]
data_players.head()

# COMMAND ----------




# COMMAND ----------

data_players.dtypes

# COMMAND ----------



mySchema = StructType([ StructField("id", LongType())
                        ,StructField("player_id", StringType())
                       ,StructField("tag", StringType())
                      ,StructField("country", StringType()) ])

#Create DataFrame by changing schema
sparkPlayer = spark.createDataFrame(data_players,schema=mySchema)
sparkPlayer  = sparkPlayer.withColumn("country", when(sparkPlayer.country == "NaN", None).otherwise(sparkPlayer.country))
sparkPlayer.printSchema()
sparkPlayer.show()




# COMMAND ----------

spark.catalog.tableExists("groupe5.players")

# COMMAND ----------

if spark.catalog.tableExists("groupe5.players"):
    fs.drop_table(
      name="groupe5.players"
    )


fs.create_table(
    name="groupe5.players",
    primary_keys=["player_id"],
    df=sparkPlayer,
)


fs.write_table(
    name="groupe5.players",
    df=sparkPlayer,
    mode="overwrite",
)

# COMMAND ----------

#Faire une table  "character played" et mettre l'id du player en clé étrangère, les personnages joué (une colonne) et le nombre de fois (une colonne)
# ON NE L UTILISE PAS CAR DURE TROP LONGTEMPS
# character_played = {'player_id': [],
#         'character': [],
#         'played': []
#         }

# main_character_played = pd.DataFrame(character_played)

# for index, row in main_players.iterrows():
#     if row['characters'] != '""':
#         list1 = row['characters'].split(",")
#         for details in list1:
#             list2 = details.split(":")
#             character = list2[0].replace("{", "").replace('"', "").replace("ultimate/", "").strip()
#             played = list2[1].replace("}", "").strip()
#             main_character_played = main_character_played.append({'player_id': row['player_id'],
#         'character': character,
#         'played': played},ignore_index=True)
            
                       

# COMMAND ----------

character_played = {'player_id': [],
        'character': [],
        'played': []
        }

main_character_played = pd.DataFrame(character_played)

print( 'mainplayer : ', main_character_played)


for index, row in main_players.iterrows():
    if row['characters'] != '""':
        list1 = row['characters'].split(",")
        character =""
        played = ""
        for details in list1:
            maxi = 0
            list2 = details.split(":")
            characterA = list2[0].replace("{", "").replace('"', "").replace("ultimate/", "").strip()
            playedA = list2[1].replace("}", "").strip()
            if int(playedA) > int(maxi):
                played = playedA
                character = characterA
                maxi = playedA
        print(index , ':',character, played)
        main_character_played = main_character_played.append({'player_id': row['player_id'],
        'character': character,
        'played': played},ignore_index=True)

# COMMAND ----------

main_character_played.dtypes

# COMMAND ----------

main_character_played["played"] = main_character_played["played"].apply(lambda x: int(x)) 

# COMMAND ----------

mySchemaCaracter = StructType([ StructField("player_id", StringType())
                       ,StructField("character", StringType())
                      ,StructField("played", IntegerType()) ])

#Create DataFrame by changing schema
sparkCharacter = spark.createDataFrame(main_character_played,schema=mySchemaCaracter)
sparkCharacter.printSchema()
sparkCharacter.show()


# COMMAND ----------

if spark.catalog.tableExists("groupe5.character_played"):
    fs.drop_table(
      name="groupe5.character_played"
    )
    
fs.create_table(
    name="groupe5.character_played",
    primary_keys=["player_id"],
    df=sparkCharacter,
)

fs.write_table(
    name="groupe5.character_played",
    df=sparkCharacter,
    mode="overwrite",
)

# COMMAND ----------

# main_sets : tournament_key, p1_id, p2_id, p1_score, p2_score, location_names --> Attention il s'agit d'une liste, best_of 
main_sets  = pd.read_csv("/dbfs/mnt/groupe404/main_sets.csv")
main_sets.head()

# COMMAND ----------

main_sets

# COMMAND ----------

data_sets = main_sets 

# COMMAND ----------

data_sets['location_names'] = data_sets['location_names'].apply(lambda x: re.findall(r'"(.*?)"', x)) 
data_sets['location_names'] = data_sets['location_names'].apply(lambda x: x[0]) 

data_sets['p1_score'] = data_sets['p1_score'].apply(lambda x: int(x))
data_sets['p2_score'] = data_sets['p2_score'].apply(lambda x: int(x))

data_sets = data_sets[['#', 'tournament_key', 'p1_id', 'p2_id', 'p1_score', 'p2_score', 'location_names', 'best_of', 'winner_id']]
data_sets.head()


# COMMAND ----------



# COMMAND ----------

data_sets

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled","true")

# COMMAND ----------

data_sets.dtypes

# COMMAND ----------

mySchemaSet = StructType([ StructField("#", LongType())
                           ,StructField("tournament_key", StringType())
                          ,StructField("p1_id", StringType())
                          ,StructField("p2_id", StringType())
                          ,StructField("p1_score", IntegerType())
                          ,StructField("p2_score", IntegerType())
                          ,StructField("location_names", StringType())
                          ,StructField("best_of", FloatType())
                          ,StructField("winner_id", StringType())
                         ])


#Create DataFrame by changing schema
sparkSets = spark.createDataFrame(data_sets,schema=mySchemaSet)
sparkSets.printSchema()
sparkSets.show()


# COMMAND ----------

sparkSets = sparkSets.withColumnRenamed("#", "id")

# COMMAND ----------

sparkSets.show()

# COMMAND ----------

if spark.catalog.tableExists("groupe5.sets"):
    fs.drop_table(
      name="groupe5.sets"
    )

fs.create_table(
    name="groupe5.sets",
    primary_keys= ["id"],
    df=sparkSets,
)


# COMMAND ----------

fs.write_table(
    name="groupe5.sets",
    df=sparkSets,
    mode="overwrite",
)

# COMMAND ----------

# main_tournament_info : key ,cleaned_name, country ?, entrants, online ?
main_tournament_info  = pd.read_csv("/dbfs/mnt/groupe404/main_tournament_info.csv")
main_tournament_info = main_tournament_info[["key", "cleaned_name", "country", "entrants", "online"]]
main_tournament_info.head()

# COMMAND ----------

mySchemaTournament_info = StructType([ StructField("key", StringType())
                       ,StructField("cleaned_name", StringType())
                       ,StructField("country", StringType())
                       ,StructField("entrants", IntegerType())
                       ,StructField("online", IntegerType())
                      ])

#Create DataFrame by changing schema
sparkTournament_info = spark.createDataFrame(main_tournament_info,schema=mySchemaTournament_info)
sparkTournament_info  = sparkTournament_info.withColumn("country", when(sparkTournament_info.country == "NaN", None).otherwise(sparkTournament_info.country))
# sparkTournament_info  = sparkTournament_info.withColumn("country", when(sparkTournament_info.country == "JP", "Japan").otherwise(sparkTournament_info.country))
# sparkTournament_info  = sparkTournament_info.withColumn("country", when(sparkTournament_info.country == "FR", "France").otherwise(sparkTournament_info.country))
# sparkTournament_info  = sparkTournament_info.withColumn("country", when(sparkTournament_info.country == "US", "United States").otherwise(sparkTournament_info.country))
# sparkTournament_info  = sparkTournament_info.withColumn("country", when(sparkTournament_info.country == "CA", "Canada").otherwise(sparkTournament_info.country))
# sparkTournament_info  = sparkTournament_info.withColumn("country", when(sparkTournament_info.country == "US", "United States").otherwise(sparkTournament_info.country))
# sparkTournament_info  = sparkTournament_info.withColumn("country", when(sparkTournament_info.country == "MX", "Mexico").otherwise(sparkTournament_info.country))
# sparkTournament_info  = sparkTournament_info.withColumn("country", when(sparkTournament_info.country == "GB", "United Kingdom").otherwise(sparkTournament_info.country))
sparkTournament_info.printSchema()
sparkTournament_info.show()


# COMMAND ----------

if spark.catalog.tableExists("groupe5.tournament"):
    fs.drop_table(
      name="groupe5.tournament"
    )

fs.create_table(
    name="groupe5.tournament",
    primary_keys=["key"],
    df=sparkTournament_info,
)



# COMMAND ----------

fs.write_table(
    name="groupe5.tournament",
    df=sparkTournament_info,
    mode="overwrite",
)

# COMMAND ----------

spark.catalog.tableExists("groupe5.players")

spark.catalog.tableExists("groupe5.character_played")

spark.catalog.tableExists("groupe5.sets")

spark.catalog.tableExists("groupe5.tournament")

