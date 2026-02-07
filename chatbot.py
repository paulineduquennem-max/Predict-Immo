import pandas as pd
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
import streamlit as st

api_key = st.secrets["GOOGLE_API_KEY"]

# A. Chargement
df = pd.read_csv("scoring.csv")
# On ne garde que les colonnes utiles (exemple)

colonnes_utiles = ['nom_quartier_parent', 'nb_jeunes_actifs', 'nb_seniors', 'taux_proprio', 'score_final']

df_filtre = df[colonnes_utiles]

# B. Transformation en Documents (Ta logique Option A)
documents = []
for index, row in df_filtre.iterrows():
    contenu = f"Quartier: {row['nom_quartier_parent']}, Taux propriétaire: {row['taux_proprio']}, Score: {row['score_final']}, Nombre jeunes actifs: {row['nb_jeunes_actifs']}, Nombre seniors: {row['nb_seniors']}"    
    doc = Document(page_content=contenu, metadata={"nom": row['nom_quartier_parent']})
    documents.append(doc)

# C. Création de la base de données
embeddings = GoogleGenerativeAIEmbeddings(model="gemini-embedding-001", google_api_key= api_key)
vectorstore = FAISS.from_documents(documents, embeddings)

# D. Sauvegarde locale (Crée un dossier sur ton ordi)
vectorstore.save_local("faiss_index_immobilier")
print("Base de données créée avec succès !")