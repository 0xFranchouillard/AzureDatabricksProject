# Databricks notebook source
from databricks import feature_store
fs = feature_store.FeatureStoreClient()

players = fs.read_table("groupe5.players")
character_played = fs.read_table("groupe5.character_played")

# COMMAND ----------

character_played.show()
