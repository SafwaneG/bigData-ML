from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, lit, concat
import os
import shutil

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Log Aggregation with Article ID") \
    .getOrCreate()

# Répertoires de base
base_input_dir = "/mnt/c/Users/safouane/Desktop/nifi"  # Dossier contenant les logs organisés par heure
output_dir = "/mnt/c/Users/safouane/Desktop/output"  # Dossier de sortie pour les fichiers agrégés

# Créer le dossier de sortie s'il n'existe pas
os.makedirs(output_dir, exist_ok=True)

# Parcourir tous les dossiers dans le répertoire de base
hour_dirs = [os.path.join(base_input_dir, d) for d in os.listdir(base_input_dir) if os.path.isdir(os.path.join(base_input_dir, d))]

if not hour_dirs:
    print("Aucun dossier d'heures trouvé dans le répertoire 'nifi'.")
    spark.stop()
    exit()

for hour_dir in hour_dirs:
    hour = os.path.basename(hour_dir)  # Récupérer le nom du dossier (exemple : "2023051012")

    try:
        # Lire tous les fichiers logs du dossier de l'heure
        logs_df = spark.read.option("delimiter", "|") \
            .csv(os.path.join(hour_dir, "*.txt"), header=False)

        # Renommer les colonnes pour donner du sens aux données
        logs_df = logs_df.withColumnRenamed("_c0", "timestamp") \
                         .withColumnRenamed("_c1", "article") \
                         .withColumnRenamed("_c2", "id") \
                         .withColumnRenamed("_c3", "price") \
                         .withColumn("price", col("price").cast("long"))  # Convertir "price" en entier

        # Agréger les données par article et ID pour calculer le total des ventes
        aggregated_df = logs_df.groupBy("article", "id") \
            .agg(_sum("price").alias("total_sales"))

        # Ajouter l'information de l'heure dans le résultat final
        result_df = aggregated_df.withColumn("hour", lit(hour)) \
            .select(concat(
                lit(hour[:4] + "/" + hour[4:6] + "/" + hour[6:8] + " ")
            ).alias("date_id"),
                col("id"),
                col("article"),
                col("total_sales"))

        # Formater la sortie selon le format demandé : date id | article | totalvente
        result_df = result_df.withColumn("output", concat(
            col("date_id"), lit(" "), col("id"), lit("|"), col("article"), lit("|"), col("total_sales")
        ))

        # Réduire à une seule partition pour générer un seul fichier
        result_df = result_df.select("output").coalesce(1)

        # Écrire directement dans un fichier texte dans un répertoire temporaire
        temp_dir = os.path.join(output_dir, f"{hour}_temp")
        result_df.write.mode("overwrite").text(temp_dir)

        # Déplacer le fichier généré (part-00000) vers le répertoire de sortie avec le bon nom
        for file_name in os.listdir(temp_dir):
            if file_name.startswith("part-") and file_name.endswith(".txt"):
                shutil.move(os.path.join(temp_dir, file_name), os.path.join(output_dir, f"{hour}.txt"))

        # Supprimer le répertoire temporaire
        shutil.rmtree(temp_dir)

        print(f"Fichier agrégé écrit pour l'heure {hour} : {os.path.join(output_dir, f'{hour}.txt')}")

    except Exception as e:
        print(f"Erreur lors du traitement du dossier {hour_dir} : {e}")

# Arrêter la session Spark
spark.stop()