# Databricks notebook source
from databricks import feature_store
fs = feature_store.FeatureStoreClient()

players = fs.read_table("groupe5.players")
character_played = fs.read_table("groupe5.character_played")
sets = fs.read_table("groupe5.sets")
tournament = fs.read_table("groupe5.tournament")

# COMMAND ----------

playerCaracter1 = players.alias("player").join(character_played.alias("c"),players.player_id ==  character_played.player_id,"inner").select("player.player_id", "tag", "country", "character", "played" )
playerCaracter1 = playerCaracter1[[ "player.player_id", "country", "character", "played"]]

# COMMAND ----------



# COMMAND ----------

playerCaracter1 = players.alias("player").join(character_played.alias("c"),players.player_id ==  character_played.player_id,"inner").select("player.player_id", "tag", "country", "character", "played" )
playerCaracter1 = playerCaracter1[[ "player.player_id", "tag", "country", "character", "played"]]

playerCaracter1 = playerCaracter1.withColumnRenamed("player_id", "player1_id").withColumnRenamed("tag", "tag1").withColumnRenamed("country", "country1").withColumnRenamed("character", "character1").withColumnRenamed("played", "played1")

playerCaracter1.show()

# COMMAND ----------

playerCaracter2 = players.alias("player").join(character_played.alias("c"),players.player_id ==  character_played.player_id,"inner").select("player.player_id", "tag", "country", "character", "played" )
playerCaracter2 = playerCaracter2[["player.player_id", "tag", "country", "character", "played"]]
playerCaracter2 = playerCaracter2.withColumnRenamed("player_id", "player2_id").withColumnRenamed("tag", "tag2").withColumnRenamed("country", "country2").withColumnRenamed("character", "character2").withColumnRenamed("played", "played2")

playerCaracter2.show()

# COMMAND ----------

tournamentSet = tournament.join(sets, tournament.key == sets.tournament_key, "inner")
tournamentSet = tournamentSet.drop("id").drop("key")
tournamentSet.show()

# COMMAND ----------

tournamentSet.write.format("delta").mode("overwrite").option("userMetadata", "init").saveAsTable(f"groupe5.trainSet")

# COMMAND ----------

players.filter(players.player_id == "341694").show()

# COMMAND ----------

trainset = tournamentSet.join(playerCaracter2, tournamentSet.p2_id == playerCaracter2.player2_id,'inner').join(playerCaracter1, tournamentSet.p1_id == playerCaracter1.player1_id, 'inner')

# COMMAND ----------

trainset.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS groupe5.trainSetSmash

# COMMAND ----------

spark.catalog.tableExists("groupe5.trainSetSmash")

# COMMAND ----------

trainset.write.format("delta").mode("overwrite").option("userMetadata", "init").saveAsTable(f"groupe5.trainSetSmash")

# COMMAND ----------

tournamentSet.where(tournamentSet.winner_id =="341694" ).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM (
# MAGIC 
# MAGIC select player_id, (select count(*) FROM groupe5.trainSet Where trainSet.winner_id == players.player_id) AS x  from groupe5.players where (select count(*) FROM groupe5.trainSet Where trainSet.winner_id == players.player_id) > 17
# MAGIC  ) AS X 

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM groupe5.trainSet
# MAGIC where trainSet.winner_id = (select player_id from groupe5.players where (select count(*) FROM groupe5.trainSet Where trainSet.winner_id == players.player_id) < 20) 

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC SELECT Count(*) FROM groupe5.trainSet 
# MAGIC INNER JOIN (
# MAGIC select winner_id, count(winner_id) AS X from groupe5.trainSet Group by winner_id having x >= 20
# MAGIC ) x ON x.winner_id = trainSet.winner_id

# COMMAND ----------



# COMMAND ----------

playerwinCount = spark.sql(
    '''
        SELECT *, CASE WHEN winner_id = p1_id THEN 'P1' ELSE 'P2' END AS win FROM groupe5.sets 
        INNER JOIN (
        SELECT winner_id AS winner, COUNT(winner_id) AS X FROM groupe5.sets GROUP BY winner_id HAVING x >= 1000
        ) x ON x.winner = sets.winner_id

    '''
)

# COMMAND ----------

playerwinCount.show()

# COMMAND ----------

playerwinCount = playerwinCount.drop("X").drop("winner").drop("tournament_key").drop("id").drop("winner_id")

# COMMAND ----------

playerwinCount.show()

# COMMAND ----------

playerwinCount

# COMMAND ----------

winner = playerwinCount.select('winner_id').distinct()

# COMMAND ----------

print(winner.count())

# COMMAND ----------

print(playerwinCount.count())

# COMMAND ----------

print(t)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS groupe5.trainSet

# COMMAND ----------



# COMMAND ----------

spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false") 

# COMMAND ----------

playerwin.show()

# COMMAND ----------

playerwinCount.write.format("delta").mode("overwrite").option("userMetadata", "init").saveAsTable(f"groupe5.trainSet")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from groupe5.players1

# COMMAND ----------

playerwinCara = spark.sql(
    '''
    
        SELECT tournament.country as tCountry, p1_id, p2_id, p1.character AS c1, p2.character AS c2, p1.country AS countryP1, p2.country AS countryP2, location_names, CASE WHEN winner_id = p1_id THEN 'P1' ELSE 'P2' END AS win FROM groupe5.sets 
        INNER JOIN (
        SELECT winner_id AS winner, COUNT(winner_id) AS X FROM groupe5.sets GROUP BY winner_id HAVING x >= 1000
        ) x ON x.winner = sets.winner_id
        INNER JOIN groupe5.players1 p1 ON p1_id = p1.player_id
        INNER JOIN groupe5.players1 p2 ON p2_id = p2.player_id
        INNER JOIN groupe5.tournament ON tournament.key == sets.tournament_key
        '''
)


# COMMAND ----------

playerwinCara.show()

# COMMAND ----------

playerwinCara.write.format("delta").mode("overwrite").option("userMetadata", "init").saveAsTable(f"groupe5.trainSet3")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM (
# MAGIC SELECT winner_id AS winner, COUNT(winner_id) AS X FROM groupe5.trainSet GROUP BY winner_id HAVING x >= 100) AS X

# COMMAND ----------


