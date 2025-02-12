import dask.dataframe as dd
import pandas as pd
import numpy as np
from pymongo import MongoClient

# Spécifiez le chemin du fichier Parquet
file_path = r"C:\Users\majda\Downloads\my project\results_combined\*.parquet"

# Lire le fichier Parquet avec Dask
df_dask = dd.read_parquet(file_path)

# Renommer les colonnes
df_dask = df_dask.rename(columns={
    'V': 'tension',
    'I': 'courant',
    'P': 'puissance',
    'E': 'energie',
    'FP': 'facteur_puissance',
    'F': 'frequence',
    'timestamp': 'Date_enregistrement',
    'features': 'features',
    'distance_to_center': 'distance_to_center',
    'mean_distance': 'mean_distance',
    'stddev_distance': 'stddev_distance',
    'count': 'count',
    'anomaly_threshold': 'anomaly_threshold',
    'is_anomaly': 'is_anomaly',
    'energy_prediction': 'energy_prediction'
})

# Supprimer la colonne 'features' si elle contient des données de type numpy.ndarray
if 'features' in df_dask.columns:
    df_dask = df_dask.drop(columns=['features'])

# Fonction pour convertir les arrays NumPy en listes Python
def convert_ndarray_to_list(val):
    if isinstance(val, np.ndarray):  # Vérifie si la valeur est un tableau NumPy
        return val.tolist()  # Convertit en liste Python
    return val  # Sinon, retourne la valeur sans modification

# Convertir les colonnes du DataFrame Dask (si nécessaire) avant de le convertir en Pandas
df_dask = df_dask.map_partitions(lambda df: df.applymap(convert_ndarray_to_list))

# Ajouter une colonne de prix estimé en Dirhams (DH)
df_dask['prix_estime'] = (df_dask['puissance'] * df_dask['energie'] * df_dask['facteur_puissance']) / 1000
df_dask['prix_estime'] = df_dask['prix_estime'] * 9.5  # Exemple de conversion en MAD avec un facteur de 9.5 DH

# Convertir le DataFrame Dask en Pandas pour insertion dans MongoDB
df = df_dask.compute()  

# Se connecter à MongoDB avec authentification
mongo_username = 'root'  # Nom d'utilisateur
mongo_password = 'password'  # Mot de passe
mongo_uri = f'mongodb://{mongo_username}:{mongo_password}@localhost:27017/?authSource=admin'

# Se connecter à MongoDB
client = MongoClient(mongo_uri)
db = client['mydatabase']
collection = db['results_combined']

# Convertir le DataFrame Pandas en dictionnaire et insérer dans MongoDB
records = df.to_dict(orient='records')

# Insérer les données dans MongoDB
collection.insert_many(records)

print(f"Les données ont été insérées dans MongoDB avec succès.")