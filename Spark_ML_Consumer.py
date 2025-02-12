from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, mean, stddev, lit, when, count, greatest
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeansModel
from pyspark.ml.linalg import Vectors
import xgboost as xgb
import numpy as np
import os

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("KafkaSparkConsumerWithKMeansAndXGBoost") \
    .getOrCreate()

# Chargement des modèles
kmeans_model_path = "file:///app/kmeans_model"
xgb_model_path = "/app/xgb_energy_model.json"

kmeans_model = KMeansModel.load(kmeans_model_path)
centroids = kmeans_model.clusterCenters()
centroids_broadcast = spark.sparkContext.broadcast(centroids)

if not os.path.exists(xgb_model_path):
    raise FileNotFoundError(f"Le modèle XGBoost est introuvable à l'emplacement : {xgb_model_path}")

xgb_model = xgb.Booster()
xgb_model.load_model(xgb_model_path)

# Définition du schéma
schema = StructType([
    StructField("V", FloatType(), True),
    StructField("I", FloatType(), True),
    StructField("P", FloatType(), True),
    StructField("E", FloatType(), True),
    StructField("FP", FloatType(), True),
    StructField("F", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "test") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load()


# Transformation JSON des messages Kafka
df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Préparation des features
assembler = VectorAssembler(inputCols=["V", "I", "P", "E", "FP", "F"], outputCol="features")

# Fonction pour calculer les distances aux centres
def compute_distance(features, prediction):
    centers = centroids_broadcast.value
    return float(Vectors.squared_distance(features, centers[prediction]))

# Fonction pour effectuer des prédictions avec XGBoost
def predict_batch(features_array):
    dmatrix = xgb.DMatrix(features_array)
    return xgb_model.predict(dmatrix)

# Fonction principale pour le traitement en streaming
def process_stream(micro_batch, batch_id):
    if micro_batch.isEmpty():
        print(f"Batch {batch_id} est vide")
        return

    # Nettoyage des données
    df_cleaned = micro_batch.dropna(subset=["V", "I", "P", "E", "FP", "F"])
    if df_cleaned.isEmpty():
        print(f"Batch {batch_id} est vide après nettoyage")
        return

    # Transformation en vecteurs de features
    data_transformed = assembler.transform(df_cleaned)

    # Application de KMeans
    predictions = kmeans_model.transform(data_transformed)

    # Calcul des distances au centre
    predictions_with_distance = predictions.withColumn(
        "distance_to_center",
        udf(lambda features, prediction: compute_distance(features, prediction), FloatType())(
            col("features"), col("prediction"))
    )

    # Calcul des statistiques
    statistics = predictions_with_distance.groupBy("prediction").agg(
        mean("distance_to_center").alias("mean_distance"),
        stddev("distance_to_center").alias("stddev_distance"),
        count("distance_to_center").alias("count")
    )

    # Détection des anomalies
    predictions_with_anomalies = predictions_with_distance.join(statistics, on="prediction", how="left") \
        .withColumn(
            "anomaly_threshold",
            when(col("count") > 1, col("mean_distance") + 1 * col("stddev_distance"))
            .otherwise(greatest(lit(1000.0), col("mean_distance")))
        ) \
        .withColumn(
            "is_anomaly",
            when(col("distance_to_center") > col("anomaly_threshold"), lit(True))
            .otherwise(lit(False))
        ) \


    # Extraction des features pour XGBoost
    features_np = np.array(predictions_with_anomalies.select("features").rdd.map(lambda row: row[0].toArray()).collect())

    # Prédictions XGBoost
    energy_predictions_np = predict_batch(features_np)
    energy_predictions_list = list(energy_predictions_np)

    # Ajout des prédictions XGBoost au DataFrame
    final_predictions = predictions_with_anomalies.withColumn("energy_prediction", lit(energy_predictions_list[0]))

    # Écriture des résultats
    output_path = "/app/results_combined"
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)

    final_predictions.write \
        .mode("append") \
        .parquet(output_path)

    print(f"Batch {batch_id} traité avec succès")

# Démarrage de la requête de streaming
query = df_parsed.writeStream \
    .foreachBatch(process_stream) \
    .option("checkpointLocation", "/app/checkpoint") \
    .start()

query.awaitTermination()