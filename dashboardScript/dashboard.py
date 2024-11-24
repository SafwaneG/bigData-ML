import os
import pandas as pd
import streamlit as st
import plotly.express as px

# Répertoire contenant les fichiers agrégés
OUTPUT_DIR = "/mnt/c/Users/safouane/Desktop/output"

def load_data(start_date, end_date):
    """
    Charger et filtrer les données entre deux dates.
    """
    all_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith('.txt')]
    all_data = []

    for file_name in all_files:
        # Extraire la date du fichier (exemple : "2024112212.txt" -> "20241122")
        file_date = file_name[:8]
        if start_date <= file_date <= end_date:
            file_path = os.path.join(OUTPUT_DIR, file_name)
            
            # Lire le fichier et ajouter les données au DataFrame
            with open(file_path, 'r') as file:
                lines = file.readlines()  # Lire toutes les lignes du fichier
                for line in lines:
                    try:
                        # Diviser la ligne en deux parties principales : "date_id + id" et le reste
                        date_id_and_id, rest = line.strip().split("  ", 1)  # Séparer par deux espaces
                        
                        # Extraire les parties individuelles
                        date_id = date_id_and_id.strip()  # Récupérer "date_id"
                        article_id, article, total_sales = rest.split("|")  # Diviser le reste par "|"
                        
                        # Ajouter les données au tableau final
                        all_data.append([date_id, article_id.strip(), article.strip(), int(total_sales.strip())])
                    except Exception as e:
                        print(f"Ligne invalide ignorée : {line.strip()} - {e}")

    # Créer un DataFrame à partir des données collectées
    df = pd.DataFrame(all_data, columns=["Date", "Article ID", "Article", "Total Sales"])
    return df


# ---------------------
# Interface utilisateur
# ---------------------
st.title("Dashboard des ventes")
st.sidebar.header("Filtres")

# Entrée des dates (YYYYMMDD)
start_date = st.sidebar.text_input("Date de début (YYYYMMDD)", "20241101")
end_date = st.sidebar.text_input("Date de fin (YYYYMMDD)", "20241130")

if st.sidebar.button("Charger les données"):
    # Charger les données filtrées
    data = load_data(start_date, end_date)

    if not data.empty:
        st.write(f"### Données entre {start_date} et {end_date}")
        
        # Afficher le tableau des données
        st.write(data)

        # Graphique 1 : Total des ventes par Article
        fig1 = px.bar(data, x="Article", y="Total Sales", color="Article", title="Total des ventes par Article")
        st.plotly_chart(fig1)

        # Graphique 2 : Total des ventes par jour
        aggregated_by_date = data.groupby("Date").sum().reset_index()
        fig2 = px.line(aggregated_by_date, x="Date", y="Total Sales", title="Total des ventes par jour")
        st.plotly_chart(fig2)
    else:
        st.error("Aucune donnée trouvée pour cette plage de dates.")