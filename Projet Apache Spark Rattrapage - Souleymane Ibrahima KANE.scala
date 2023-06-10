// Databricks notebook source
// MAGIC %md
// MAGIC **Nom Etudiant :**  KANE
// MAGIC
// MAGIC **Prenom Etudiant:**  Souleymane Ibrahima 
// MAGIC
// MAGIC **Classe :** Master Big Data Analytics / Promo 2
// MAGIC
// MAGIC **Date limite de dépot : 10/06/2023 avant 23h 59**
// MAGIC
// MAGIC **Merci de partagé le projet dans un repos Git public**
// MAGIC
// MAGIC
// MAGIC # Travail à Faire:
// MAGIC Télécharger le Datasets sur le lien Drive : https://drive.google.com/file/d/1-yxZ7BcPyLXl5uhGTjlFY-7-EL_Ol_yR/view?usp=share_link 
// MAGIC
// MAGIC Repondre les questions ci-dessous avec le maximum de precisions et de détails.   
// MAGIC Remplir `FILL_IN` avec les methodes qui correspondent à la réponse adéquate
// MAGIC
// MAGIC ### Revenus des achats
// MAGIC 1. Extraire les revenus d'achat pour chaque événement
// MAGIC 2. Filtrer les événements dont le revenu n'est pas nul
// MAGIC 3. Vérifiez quels sont les types d'événements qui génèrent des revenus
// MAGIC 4. Supprimez la colonne inutile
// MAGIC
// MAGIC ### Revenus par Traffic  
// MAGIC Obtenir les 3 sources de trafic générant le revenu total le plus élevé.  
// MAGIC 5. Revenus cumulés par source de trafic  
// MAGIC 7. Obtenir les 3 principales sources de trafic par revenu total  
// MAGIC 6. Nettoyer les colonnes de revenus pour avoir deux décimales  
// MAGIC 8. Sauvegarder les données  
// MAGIC
// MAGIC ### Obtenir les utilisateurs les plus actifs (active_users) par jour
// MAGIC
// MAGIC ### Obtenir le nombre moyen d'utilisateurs actifs par jour de la semaine
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC ##### Methods
// MAGIC - DataFrame: `select`, `drop`, `withColumn`, `filter`, `dropDuplicates`,  `groupBy`, `sort`, `limit`
// MAGIC - Column: `isNotNull`, `alias`, `desc`, `cast`, `operators`

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.SparkSession
// MAGIC val spark = SparkSession.builder.
// MAGIC                         appName("Projet Spark").
// MAGIC                         config("spark.ui.port", "0").
// MAGIC                         master("local[*]").
// MAGIC                         getOrCreate()

// COMMAND ----------

// MAGIC %scala
// MAGIC val eventsPath = "/FileStore/tables/events.json"

// COMMAND ----------

// MAGIC %scala
// MAGIC val eventsDF = spark.read.json(eventsPath)
// MAGIC eventsDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Extraire les revenus d'achat pour chaque événement
// MAGIC Ajouter une nouvelle colonne **`revenue`** en faisant l'extration de **`ecommerce.purchase_revenue_in_usd`**

// COMMAND ----------

// MAGIC %scala
// MAGIC // TODO
// MAGIC import org.apache.spark.sql.functions._
// MAGIC val revenueDF = eventsDF.withColumn("revenue",col("ecommerce.purchase_revenue_in_usd"))
// MAGIC revenueDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Filtrer les événements dont le revenu n'est pas null

// COMMAND ----------

// TODO
val purchasesDF = revenueDF.filter(col("revenue").isNotNull)
purchasesDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Vérifiez quels sont les types d'événements qui génèrent des revenus
// MAGIC Trouvez des valeurs **`event_name`** uniques dans **`purchasesDF`**. Il y a deux façons de faire :
// MAGIC - Sélectionnez "event_name" et recupérer les enregistrements distincts

// COMMAND ----------

// TODO
val distinctDF = purchasesDF.select(col("event_name")).distinct()
distinctDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Supprimez la colonne inutile
// MAGIC Puisqu'il n'y a qu'un seul type d'événement, supprimez **`event_name`** de **`purchasesDF`**.

// COMMAND ----------

// TODO
val cleanDF = purchasesDF.drop("event_name")
cleanDF.show

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Revenus cumulés par source de trafic
// MAGIC - Obtenir la somme de **`revenue`** comme **`total_rev`**
// MAGIC - Obtenir la moyenne de **`revenue`** comme **`avg_rev`**
// MAGIC
// MAGIC N'oubliez pas d'importer toutes les fonctions intégrées nécessaires.

// COMMAND ----------

// TODO
val trafficDF = cleanDF.groupBy("traffic_source").agg(sum("revenue").alias("total_rev"),avg("revenue").alias("avg_rev"))
trafficDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6. Obtenir les cinqs principales sources de trafic par revenu total

// COMMAND ----------

//TODO
val topTrafficDF = trafficDF.sort(col("total_rev").desc).limit(5)
topTrafficDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7. Limitez les colonnes de revenus à deux décimales pointés
// MAGIC - Modifier les colonnes **`avg_rev`** et **`total_rev`** pour les convertir en des nombres avec deux décimales pointés

// COMMAND ----------

// TODO
val finalDF = topTrafficDF.withColumn("avg_rev",round(col("avg_rev"),2)).withColumn("total_rev",round(col("total_rev"),2))
finalDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8. Sauvegarder les données 
// MAGIC Sauvegarder les données sous le format parquet

// COMMAND ----------

// TODO
finalDF.write.parquet("backup.parquet")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9. Obtenir les utilisateurs les plus actifs (active_users) par jour

// COMMAND ----------

// MAGIC %python
// MAGIC // TODO
// MAGIC val activeUsersDF = eventsDF.FILL_IN
// MAGIC
// MAGIC activeUsersDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 10. Obtenir le nombre moyen d'utilisateurs actifs par jour de la semaine 

// COMMAND ----------

// MAGIC %python
// MAGIC // TODO
// MAGIC val activeDowDF = activeUsersDF.FILL_IN
// MAGIC activeDowDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 11. Sauvegarder les données 
// MAGIC Sauvegarder les données sous le format parquet

// COMMAND ----------

// MAGIC %python
// MAGIC activeDowDF.FILL_IN

// COMMAND ----------

// MAGIC %md
// MAGIC ### 12. Industrialiser et Deployer le code source dans le cluster Hadoop  
// MAGIC En s'appuyant sur le code source https://drive.google.com/drive/folders/1AWjgscAdtlpemf-Qj1lEecxg3jvsMSIj?usp=sharing, industrialiser ce notebook en code Spark et le déployer job en mode cluster dans HadoopVagrant en stockant le dataframe final dans Hive en format parquet

// COMMAND ----------


