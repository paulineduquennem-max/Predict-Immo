import pandas as pd
import time
import requests
from datetime import datetime
import ast
from shapely.geometry import Point, Polygon
import geopandas as gpd

# --- 1. UTILITAIRES DE GÉOMÉTRIE ---

def preparation_coord_quartier(string, reverse_lat_lon=False):
    try:
        data = ast.literal_eval(string)
        coords = data['geometry']['coordinates'][0]
        if reverse_lat_lon:
            return [tuple(c[::-1]) for c in coords]
        return [tuple(c) for c in coords]
    except: return None

def preparation_coord(data):
    if isinstance(data, str):
        try: data = ast.literal_eval(data)
        except: return None
    if isinstance(data, dict):
        try:
            coords = data['geometry']['coordinates'][0]
            return [tuple(c) for c in coords]
        except: return None
    return None

def preparation_coordonee_point(string):
    coord = string.split(",")
    return float(coord[1]), float(coord[0])

def attribution_sous_quartier(dataframe, gdf_sous_quartiers): 
    dataframe['geometry'] = dataframe['point'].apply(Point)
    gdf_points = gpd.GeoDataFrame(dataframe, geometry='geometry', crs="EPSG:4326")
    res = gpd.sjoin(gdf_points, gdf_sous_quartiers[['gid', 'geometry']], how='left', predicate='within')
    return res.dropna(subset=['gid'])

def read_csv():
    path_in = "dags/df.quartiers_polygone.csv"
    df_quartiers = pd.read_csv(path_in)
    df_quartiers['polygone'] = df_quartiers['geo_shape_quartiers'].apply(preparation_coord_quartier)
    return df_quartiers

# --- 2. FONCTIONS APPELÉES PAR LE DAG ---

def telecharger_dataset_en_csv():
    df_quart_ref = read_csv() 
    url = "https://datahub.bordeaux-metropole.fr/api/explore/v2.1/catalog/datasets/se_filosofi_200_s/records"
    tous_les_resultats = []
    offset, limite = 0, 100
    while True:
        params = {"where": "insee='33063'", "limit": limite, "offset": offset}
        r = requests.get(url, params=params)
        if r.status_code != 200: break
        lignes = r.json().get("results", [])
        if not lignes: break
        tous_les_resultats += lignes
        offset += limite
        if len(lignes) < limite: break

    df = pd.DataFrame(tous_les_resultats)
    df['polygone'] = df['geo_shape'].apply(preparation_coord)
    df['geometry'] = df['polygone'].apply(Polygon)
    gdf_sq = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    gdf_q = gpd.GeoDataFrame(df_quart_ref, geometry=df_quart_ref['polygone'].apply(Polygon), crs="EPSG:4326")
    
    gdf_temp = gdf_sq.copy()
    gdf_temp['geometry'] = gdf_temp.geometry.centroid
    df_j = gpd.sjoin(gdf_temp, gdf_q[['nom', 'geometry']], how='left', predicate='within')
    gdf_sq['nom_quartier_parent'] = df_j['nom']
    gdf_sq.dropna(subset="nom_quartier_parent", inplace=True)
    gdf_sq.to_csv("gdf_sous_quartiers.csv", index=False)
    return "OK"

def get_dpe_neuf():
    # On charge la référence générée par t1
    df_ref = pd.read_csv("gdf_sous_quartiers.csv")
    df_ref['geometry'] = df_ref['polygone'].apply(ast.literal_eval).apply(Polygon)
    gdf_ref = gpd.GeoDataFrame(df_ref, geometry='geometry', crs="EPSG:4326")

    url = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe02neuf/lines"
    params = {"code_insee_ban_eq": "33063", "date_etablissement_dpe_gte": "2020", "size": 1000}
    
    resp = requests.get(url, params=params)
    results = resp.json().get('results', [])
    df = pd.json_normalize(results)
    df['point'] = df["_geopoint"].apply(preparation_coordonee_point)
    df = attribution_sous_quartier(df, gdf_ref)
    df.to_csv("df_dpe_neuf.csv", index=False)
    return "DPE Neuf OK"

def get_dpe_ancien():
    df_ref = pd.read_csv("gdf_sous_quartiers.csv")
    df_ref['geometry'] = df_ref['polygone'].apply(ast.literal_eval).apply(Polygon)
    gdf_ref = gpd.GeoDataFrame(df_ref, geometry='geometry', crs="EPSG:4326")

    url = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
    params = {"code_insee_ban_eq": "33063", "date_etablissement_dpe_gte": "2020", "size": 1000}
    
    resp = requests.get(url, params=params)
    results = resp.json().get('results', [])
    df = pd.json_normalize(results)
    df['point'] = df["_geopoint"].apply(preparation_coordonee_point)
    df = attribution_sous_quartier(df, gdf_ref)
    df.to_csv("df_dpe_ancien.csv", index=False)
    return "DPE Ancien OK"

def get_ventes_foncieres():
    df_ref = pd.read_csv("gdf_sous_quartiers.csv")
    df_ref['geometry'] = df_ref['polygone'].apply(ast.literal_eval).apply(Polygon)
    gdf_ref = gpd.GeoDataFrame(df_ref, geometry='geometry', crs="EPSG:4326")

    base_url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/buildingref-france-demande-de-valeurs-foncieres-geolocalisee-millesime/records/"
    params = {
        "select": "date_mutation, nature_mutation, valeur_fonciere, latitude, longitude",
        "where": "com_code = 33063 AND date_mutation >= '2023-01-01'",
        "limit": 100
    }
    r = requests.get(base_url, params=params)
    df = pd.DataFrame(r.json().get('results', []))
    df["point"] = df.apply(lambda x: (float(x['longitude']), float(x['latitude'])), axis=1)
    df = attribution_sous_quartier(df, gdf_ref)
    df['Annee'] = pd.to_datetime(df['date_mutation']).dt.year
    df.to_csv("df_ventes.csv", index=False)
    return "Ventes OK"

def scoring():
    # 1. Chargement des données générées par les tâches précédentes
    df_sq = pd.read_csv("gdf_sous_quartiers.csv")
    df_v = pd.read_csv("df_ventes.csv")
    
    df_scoring = df_sq.copy()

    # 2. Calcul des agrégats (Logements, Jeunes, Seniors)
    df_scoring['nb_logements'] = (df_sq['nb_log_av45'] + df_sq['nb_log_45_70'] + 
                                  df_sq['nb_log_70_90'] + df_sq['nb_log_ap90'] + 
                                  df_sq['nb_log_soc'])
    
    df_scoring['nb_jeunes_actifs'] = (df_sq['nb_ind_18_24'] + df_sq['nb_ind_25_39'] + 
                                      df_sq['nb_ind_40_54'])
    
    df_scoring['nb_seniors'] = (df_sq['nb_ind_55_64'] + df_sq['nb_ind_65_79'] + 
                                df_sq['nb_ind_80p'])

    # 3. Calcul des ventes moyennes annuelles
    ventes_annuelles = df_v.groupby(['gid', 'Annee']).size().reset_index(name='nb_ventes')
    df_score_ventes = ventes_annuelles.pivot(index='gid', columns='Annee', values='nb_ventes').fillna(0)
    
    # On calcule la moyenne sur le nombre d'années présentes dans les données
    df_score_ventes['moyenne_ventes_annuelle'] = df_score_ventes.mean(axis=1)
    df_score_ventes = df_score_ventes.reset_index()

    # 4. Fusion des données
    df_scoring = pd.merge(left=df_scoring, right=df_score_ventes[['gid', 'moyenne_ventes_annuelle']], 
                          how='inner', on='gid')

    # 5. Création des Ratios (Indicateurs relatifs)
    df_scoring['taux_proprio'] = df_scoring['nb_men_prop'] / df_scoring['nb_men'].replace(0, 1)
    df_scoring['taux_rotation'] = df_scoring['moyenne_ventes_annuelle'] / df_scoring['nb_logements'].replace(0, 1)
    df_scoring['indice_seniors'] = df_scoring['nb_seniors'] / df_scoring['nb_ind'].replace(0, 1)
    df_scoring['indice_jeunes'] = df_scoring['nb_jeunes_actifs'] / df_scoring['nb_ind'].replace(0, 1)

    # 6. Normalisation Min-Max (0 à 1)
    for col in ['taux_proprio', 'taux_rotation', 'indice_seniors', 'indice_jeunes']:
        mini = df_scoring[col].min()
        maxi = df_scoring[col].max()
        # Sécurité pour éviter la division par zéro si mini == maxi
        if maxi - mini != 0:
            df_scoring[f'{col}_n'] = (df_scoring[col] - mini) / (maxi - mini)
        else:
            df_scoring[f'{col}_n'] = 0

    # 7. Calcul du Score Final Pondéré
    df_scoring['score_final'] = (
        (df_scoring['taux_proprio_n'] * 0.35) +   # Stock de mandats
        (df_scoring['taux_rotation_n'] * 0.30) +  # Dynamisme historique
        (df_scoring['indice_seniors_n'] * 0.20) + # Potentiel succession
        (df_scoring['indice_jeunes_n'] * 0.15)    # Renouvellement
    ) * 100

    # 8. Tri et Sauvegarde
    df_final = df_scoring.sort_values(by='score_final', ascending=False)
    df_final.to_csv("scoring.csv", index=False)
    return "Scoring complet terminé avec succès"