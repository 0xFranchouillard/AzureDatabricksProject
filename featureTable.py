# Databricks notebook source
from databricks import feature_store

fs = feature_store.FeatureStoreClient()

fs.create_table(
    name="",
    primary_keys=[""],
    df=,
)

fs.write(
    name="",
    df=,
    mode="overwrite",
)
