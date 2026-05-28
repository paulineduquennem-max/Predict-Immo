# 🏠 Outil de Scoring Immobilier — Optimisation de la Prospection (Bordeaux)

![Aperçu de l'outil de scoring immobilier](Capture%20d'écran%202026-02-20%20112535.jpg)

## 📌 Contexte du Projet
Ce projet a été développé dans le cadre de ma formation de Data Analyst à la Wild Code School. L'objectif était de concevoir un outil décisionnel à destination des professionnels de l'immobilier pour optimiser leur prospection terrain sur la ville de Bordeaux. Il leur permet de repérer rapidement les zones géographiques ayant la plus forte probabilité de voir des biens mis en vente, afin de se positionner en amont sur le marché caché et d'obtenir des mandats exclusifs.

## 🎯 Objectifs Business & Techniques
* **Collecte de données hétérogènes :** Centralisation et fusion de plusieurs sources de données publiques et massives (DVF pour l'historique des ventes, INSEE pour la démographie/revenus, ADEME pour les données DPE de Bordeaux).
* **Création d'un algorithme de scoring prédictif :** Définition de règles métiers et de coefficients pondérés pour attribuer une note de potentiel de vente à chaque secteur géographique / quartier de la ville.
* **Enrichissement de l'information terrain :** Intégration des données DPE dans les infobulles par quartier pour fournir des renseignements complémentaires aux conseillers et leur permettre d'adapter leur discours commercial.
* **Outil d'aide à la prospection :** Fournir une interface claire aux équipes commerciales pour piloter leurs actions de prospection de manière chirurgicale.

## 🛠️ Stack Technique
* **Langage :** Python
* **Data Processing & Analyse :** Pandas, NumPy
* **Analyse Géospatiale :** GeoPandas
* **Visualisation & Interface :** Power BI / Streamlit

## 📊 Méthodologie & Étapes clés
1. **Data Ingestion & Cleaning :** Nettoyage approfondi de volumes de données massifs (notamment les fichiers DVF), filtrage sur la commune de Bordeaux, traitement des valeurs aberrantes et normalisation des données avec Pandas.
2. **Feature Engineering :** Création d'indicateurs personnalisés de rotation immobilière (ancienneté des propriétaires, typologie des logements, dynamique des ventes locales).
3. 3. **Calcul du Score & Enrichissement :** Développement d'une fonction de scoring pondérant les critères immobiliers et démographiques pour évaluer la probabilité de mise en vente. Intégration des données environnementales (DPE) comme indicateurs contextuels textuels.
4. **Visualisation :** Restitution sous forme de cartes interactives et de graphiques analytiques pour guider visuellement les agents immobiliers sur le terrain.
