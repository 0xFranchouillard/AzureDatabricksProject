# Databricks notebook source
if "/mnt/groupe404" not in str(dbutils.fs.ls('/mnt')):
    dbutils.fs.mount(
    source = "wasbs://newcontainer@storageaccountesgi404.blob.core.windows.net",
    mount_point = "/mnt/groupe404",
    extra_configs = {"fs.azure.account.key.storageaccountesgi404.blob.core.windows.net":dbutils.secrets.get(scope = "scope404", key = "secret-key-404-project")})
else:
    print("mountingPoint already mounted !")
