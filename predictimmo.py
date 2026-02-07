# Import de bibliothèques
import pandas as pd
from tqdm import tqdm
tqdm.pandas()
import time
import requests
from datetime import datetime
import ast
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import geopandas as gpd
from shapely.geometry import shape
import json
import streamlit as st
import pydeck as pdk
import base64

def preparation_coord_quartier(string, reverse_lat_lon=False):
    try:
        # Transformation de la chaîne en dictionnaire
        data = ast.literal_eval(string)

        # Extraction du premier anneau du polygone
        # (L'index [0] récupère les contours extérieurs)
        coords = data['geometry']['coordinates'][0]

        if reverse_lat_lon:
            # On inverse pour avoir (Latitude, Longitude)
            return [tuple(c[::-1]) for c in coords]
        else:
            # On garde (Longitude, Latitude) mais en tuples propres
            return [tuple(c) for c in coords]

    except (ValueError, KeyError, TypeError):
        return None
    
# Appel des df
df_quartiers = pd.read_csv(r"Datasets/df.quartiers_polygone (2).csv")
df_quartiers['polygone'] = df_quartiers['geo_shape_quartiers'].apply(preparation_coord_quartier)
df_quartiers.to_csv(r"df_quartiers.csv", index = False)

# Fonction préparation coordonnée sous-quartier / création polygone
def preparation_coord(data):
  # Si la donnée est une chaîne de caractères, on la convertit
    if isinstance(data, str):
        try:
            import ast
            data = ast.literal_eval(data)
        except Exception:
            return None # Ou gérer l'erreur si le format est invalide

    # Si après conversion (ou déjà au départ) c'est un dictionnaire
    if isinstance(data, dict):
        try:
            # Extraction des coordonnées selon la structure vue dans ton erreur
            # Structure : {'type': 'Feature', 'geometry': {'coordinates': [[[...]]], 'type': 'Polygon'}}
            coords = data['geometry']['coordinates'][0]
            
            # Transformation en liste de tuples (lon, lat) pour Shapely
            tuple_coord = [tuple(c) for c in coords]
            return tuple_coord
        except (KeyError, IndexError):
            return None
            
    return None

# Fonction création point pour les df DPE 
def preparation_coordonee_point(string):
  coord = string.split(",")
  lon = float(coord[1])
  lat = float(coord[0])
  return lon, lat

# Requêtage d'API

# API DataHub Bordeaux_sous-quartier
def telecharger_dataset_en_csv():
    url = "https://datahub.bordeaux-metropole.fr/api/explore/v2.1/catalog/datasets/se_filosofi_200_s/records"

    tous_les_resultats = []
    offset = 0
    limite = 100

    print("📡 Connexion à l'API Bordeaux Métropole...")

    while True:
        params = {
        "where": "insee='33063'", 
        "limit": limite,
        "offset": offset
    }
        r = requests.get(url, params=params)

        if r.status_code != 200:
            print(f"❌ Erreur API : Code {r.status_code}")
            return None

        data = r.json()
        lignes = data.get("results", [])

        if not lignes:
            break

        tous_les_resultats += lignes
        print(f"📥 Récupération : {len(tous_les_resultats)} lignes...")

        offset += limite
        if len(lignes) < limite:
            break

    df = pd.DataFrame(tous_les_resultats)

    print("🗺️ Reconstruction des polygones...")
    df['polygone'] = df['geo_shape'].apply(preparation_coord)
    # 1. Préparation des sous-quartiers (les petits polygones)
    df['geometry'] = df['polygone'].apply(Polygon)
    gdf_sous_quartiers = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")

    try:
        # Note : Je suppose que df_quartiers est chargé globalement
        # On s'assure qu'il est aussi en GeoDataFrame
        gdf_quartiers = gpd.GeoDataFrame(df_quartiers, 
                                         geometry=df_quartiers['polygone'].apply(Polygon), 
                                         crs="EPSG:4326")

        print("🔗 Jointure spatiale des quartiers parents...")
        gdf_temp_points = gdf_sous_quartiers.copy()
        gdf_temp_points['geometry'] = gdf_temp_points.geometry.centroid

        df_jointure = gpd.sjoin(
            gdf_temp_points,
            gdf_quartiers[['nom', 'geometry']],
            how='left',
            predicate='within'
        )

        gdf_sous_quartiers['nom_quartier_parent'] = df_jointure['nom']
        
    except Exception as e:
        print(f"⚠️ Erreur lors de la jointure quartiers : {e}")
    gdf_sous_quartiers = gdf_sous_quartiers.drop(columns = 
                                                 ["geo_point_2d", "geo_shape", "geom_err", "ident", "val_approchee", 
                                                  "cdate", "mdate", "nb_ind_nc", "nb_log_nc"])
    gdf_sous_quartiers.dropna(subset = "nom_quartier_parent", inplace= True)
    gdf_sous_quartiers.to_csv("gdf_sous_quartiers.csv", index=False)
    print("✅ Fichier gdf_sous_quartiers.csv généré avec succès.")
    return gdf_sous_quartiers

# Fonction pour attribuer les sous-quartier aux df
# Convertir la chaîne de caractères en dictionnaire, puis en objet géométrique
def attribution_sous_quartier(dataframe, gdf_sous_quartiers): 
    #df_sous_quartier = pd.read_csv("gdf_sous_quartiers.csv")
    # 3. Préparer également le DF des points (DPE)
    # Si vos points sont des tuples du type (-0.74, 44.88)
    dataframe['geometry'] = dataframe['point'].apply(Point)
    gdf_points = gpd.GeoDataFrame(dataframe, geometry='geometry', crs="EPSG:4326")

    # 4. Faire la jointure spatiale (sjoin) pour trouver dans quel quartier est chaque point
    # 'within' vérifie si le point est à l'intérieur du polygone
    dataframe = gpd.sjoin(gdf_points, gdf_sous_quartiers[['gid', 'geometry']], how='left', predicate='within')
    dataframe = dataframe.dropna(subset=['gid'])
    return dataframe

# API ADEME - DPE Neuf
def get_dpe_neuf(gdf_ref):
    # 1. On part de l'URL qui vous donne 100k résultats sur le site
    url = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe02neuf/lines"
    params = {
        "select": "date_etablissement_dpe,etiquette_dpe,etiquette_ges,type_batiment,annee_construction,numero_voie_ban,nom_rue_ban,nom_commune_ban,code_postal_ban,_geopoint",
        "code_insee_ban_eq": "33063",
        "date_etablissement_dpe_gte": "2020",
        "size": 1000 # On peut monter à 1000 ici
    }

    all_dfs = []
    # On initialise avec notre URL de départ
    next_url = url
    is_first_page = True

    print("Début de l'extraction (100k+ lignes attendues)...")

    while next_url:
        try:
            # La première fois on utilise params, les fois suivantes l'URL 'next' contient déjà tout
            if is_first_page:
                resp = requests.get(next_url, params=params, timeout=30)
                is_first_page = False
            else:
                resp = requests.get(next_url, timeout=30)

            resp.raise_for_status()
            data = resp.json()

            results = data.get('results', [])
            if not results:
                break

            all_dfs.append(pd.json_normalize(results))

            # Affichage de la progression
            total_actuel = sum(len(df) for df in all_dfs)
            if total_actuel % 5000 == 0:
                print(f"Progression : {total_actuel} lignes récupérées...")

            # CLEF DU SUCCÈS : On récupère le lien vers la page suivante fourni par l'ADEME
            next_url = data.get('next')

            # Petite pause pour ne pas saturer le serveur
            time.sleep(0.1)

        except Exception as e:
            print(f"Erreur : {e}. Pause de 10s avant tentative...")
            time.sleep(10)
            continue # On ne change pas next_url, on va donc retenter la même page

    df_final = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
    print(f"Terminé ! Total final : {len(df_final)} lignes.")
    df_final['adresse_complete'] = df_final.apply(
        lambda x: f"{x['numero_voie_ban']} {x['nom_rue_ban']} {x['code_postal_ban']} {x['nom_commune_ban']}"
                if pd.notna(x['numero_voie_ban'])
                else f"{x['nom_rue_ban']} {x['code_postal_ban']} {x['nom_commune_ban']}",
                axis=1)
    df_final['point'] = df_final["_geopoint"].apply(preparation_coordonee_point)
    df_final = attribution_sous_quartier(df_final, gdf_ref)
    df_final.to_csv("df_dpe_neuf.csv", index=False)
    return df_final

# API ADEME - DPE ancien
def get_dpe_ancien(gdf_ref):
    # 1. On part de l'URL qui vous donne 100k résultats sur le site
    url = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe03existant/lines"
    params = {
        "select": "date_etablissement_dpe,etiquette_dpe,etiquette_ges,type_batiment,annee_construction,numero_voie_ban,nom_rue_ban,nom_commune_ban,code_postal_ban,_geopoint",
        "code_insee_ban_eq": "33063",
        "date_etablissement_dpe_gte": "2020",
        "size": 1000 # On peut monter à 1000 ici
    }

    all_dfs = []
    # On initialise avec notre URL de départ
    next_url = url
    is_first_page = True

    print("Début de l'extraction (100k+ lignes attendues)...")

    while next_url:
        try:
            # La première fois on utilise params, les fois suivantes l'URL 'next' contient déjà tout
            if is_first_page:
                resp = requests.get(next_url, params=params, timeout=30)
                is_first_page = False
            else:
                resp = requests.get(next_url, timeout=30)

            resp.raise_for_status()
            data = resp.json()

            results = data.get('results', [])
            if not results:
                break

            all_dfs.append(pd.json_normalize(results))

            # Affichage de la progression
            total_actuel = sum(len(df) for df in all_dfs)
            if total_actuel % 5000 == 0:
                print(f"Progression : {total_actuel} lignes récupérées...")

            # CLEF DU SUCCÈS : On récupère le lien vers la page suivante fourni par l'ADEME
            next_url = data.get('next')

            # Petite pause pour ne pas saturer le serveur
            time.sleep(0.1)

        except Exception as e:
            print(f"Erreur : {e}. Pause de 10s avant tentative...")
            time.sleep(10)
            continue # On ne change pas next_url, on va donc retenter la même page

    df_final = pd.concat(all_dfs, ignore_index=True).drop_duplicates()
    print(f"Terminé ! Total final : {len(df_final)} lignes.")
    df_final['adresse_complete'] = df_final.apply(
        lambda x: f"{x['numero_voie_ban']} {x['nom_rue_ban']} {x['code_postal_ban']} {x['nom_commune_ban']}"
                if pd.notna(x['numero_voie_ban'])
                else f"{x['nom_rue_ban']} {x['code_postal_ban']} {x['nom_commune_ban']}",
                axis=1)
    df_final['point'] = df_final["_geopoint"].apply(preparation_coordonee_point)
    df_final = attribution_sous_quartier(df_final, gdf_ref)
    df_final.to_csv("df_dpe_ancien.csv", index=False)
    return df_final

# API DATA-HUB - Ventes foncière
def get_ventes_foncieres(gdf_ref):
    base_url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/buildingref-france-demande-de-valeurs-foncieres-geolocalisee-millesime/records/"
    
    # Votre dictionnaire de renommage

    all_data = []
    current_year = datetime.now().year
    start_year = current_year - 12 # Pour avoir les 10 dernières années

    # Headers sans API Key mais avec User-Agent pour éviter le blocage
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
    }

    print(f"Lancement de l'extraction complète de {start_year} à {current_year}...")

    for year in range(start_year, current_year + 1):
        offset = 0
        limit = 100
        print(f"--- Année {year} ---")
        
        while True:
            params = {
                "select": "date_mutation, nature_mutation, valeur_fonciere, adresse_numero, adresse_nom_voie, code_postal, com_name, id_parcelle, type_local, surface_reelle_bati, longitude, latitude",
                "where": f"com_code = 33063 AND date_mutation >= '{year}-01-01' AND date_mutation <= '{year}-12-31'",
                "exclude": "type_local:Dépendance",
                "limit": limit,
                "offset": offset,
                "order_by": "date_mutation ASC"
            }

            try:
                # Requête sans authentification
                response = requests.get(base_url, headers=headers, params=params)
                
                if response.status_code == 429:
                    print("Trop de requêtes (Rate limit). Pause de 30 secondes...")
                    time.sleep(30)
                    continue
                
                response.raise_for_status()
                data = response.json()
                batch = data.get('results', [])

                if not batch:
                    break

                all_data.extend(batch)
                offset += limit
                
                # Sécurité pagination Opendatasoft
                if offset >= 9900: break
                
                time.sleep(0.2) # Pause un peu plus longue sans clé pour être "gentil" avec le serveur
                
            except Exception as e:
                print(f"Erreur lors de la récupération de l'année {year}: {e}")
                break

    # Création du DataFrame et sauvegarde
    if all_data:
        df = pd.DataFrame(all_data)
        # On s'assure que le renommage est cohérent si besoin 
        # (L'API renvoie déjà les noms du 'select', donc ici c'est déjà propre)
        
        #df.to_csv('df_ventes_complet_10ans.csv', index=False)
        print(f"Extraction terminée : {len(df)} lignes sauvegardées.")
        df["_geopoint"] = df["latitude"].astype(str) + "," + df["longitude"].astype(str)
        df['point'] = df["_geopoint"].apply(preparation_coordonee_point)
        df = attribution_sous_quartier(df, gdf_ref)
        df['date_mutation'] = pd.to_datetime(df['date_mutation'])
        df['Annee'] = df['date_mutation'].dt.year
        df.to_csv("df_ventes.csv", index=False)
        return df
    else:
        print("Aucune donnée récupérée.")
        return None

# Fonction nettoyage

# Appel des df propres
df_sous_quartier = telecharger_dataset_en_csv()
df_ventes = get_ventes_foncieres(df_sous_quartier)
df_dpe_ancien = get_dpe_ancien(df_sous_quartier)
df_dpe_neuf = get_dpe_neuf(df_sous_quartier)

def scoring():
    df_scoring = df_sous_quartier.copy()
    df_scoring['nb_logements'] = df_sous_quartier['nb_log_av45'] + 	df_sous_quartier['nb_log_45_70']	+ df_sous_quartier['nb_log_70_90'] +	df_sous_quartier['nb_log_ap90']	+ df_sous_quartier ['nb_log_soc']
    df_scoring['nb_jeunes_actifs'] = df_sous_quartier['nb_ind_18_24']	+ df_sous_quartier['nb_ind_25_39']	+ df_sous_quartier['nb_ind_40_54']
    df_scoring['nb_seniors'] = df_scoring['nb_ind_55_64'] +	df_scoring['nb_ind_65_79']	+ df_scoring['nb_ind_80p']

    # Serie de traitement pour avoir les ventes moyenne annuelles
    # On groupe par quartier (gid) et par année pour compter le nombre de ventes
    ventes_annuelles = df_ventes.groupby(['gid', 'Annee']).size().reset_index(name='nb_ventes')
    # On "pivote" le tableau pour avoir une colonne par année (plus facile pour le scoring)
    df_score_ventes = ventes_annuelles.pivot(index='gid', columns='Annee', values='nb_ventes').fillna(0)
    df_score_ventes['total_vente'] = df_score_ventes.sum(axis=1)
    df_score_ventes['moyenne_ventes_annuelle'] = round(df_score_ventes['total_vente'] / len(df_score_ventes.columns), 2)
    df_score_ventes = df_score_ventes.reset_index()
    # On merge le nouveau df qui contient moyenne vente avec df scoring
    df_scoring = pd.merge(left= df_scoring, right= df_score_ventes, how= 'inner', on= 'gid')
    df_scoring = df_scoring.drop(columns= 
                                ['surf_log', 'nv_ind', 'nb_log_av45', 'nb_log_45_70', 'nb_log_70_90', 'nb_log_ap90', 
                                'nb_log_soc', 'nb_ind_0_3', 'nb_ind_4_5', 'nb_ind_6_10', 'nb_ind_11_17', 
                                'nb_ind_18_24', "nb_ind_25_39", "nb_ind_40_54", "nb_ind_55_64", "nb_ind_65_79", 
                                "nb_ind_80p", 'insee', "nb_men_pauv", "nb_men_1ind", "nb_men_5ind", "nb_men_monop"])
    # --- 1. Préparation des Ratios (Indicateurs relatifs) ---
    # On divise par le total pour pouvoir comparer un petit sous-quartier avec un grand
    df_scoring['taux_proprio'] = df_scoring['nb_men_prop'] / df_scoring['nb_men'].replace(0, 1)
    df_scoring['taux_rotation'] = df_scoring['moyenne_ventes_annuelle'] / df_scoring['nb_logements'].replace(0, 1)
    df_scoring['indice_seniors'] = df_scoring['nb_seniors'] / df_scoring['nb_ind'].replace(0, 1)
    df_scoring['indice_jeunes'] = df_scoring['nb_jeunes_actifs'] / df_scoring['nb_ind'].replace(0, 1)

    # --- 2. Normalisation Min-Max (0 à 1) ---
    # Indispensable pour additionner des pourcentages et des volumes
    for col in ['taux_proprio', 'taux_rotation', 'indice_seniors', 'indice_jeunes']:
        df_scoring[f'{col}_n'] = (df_scoring[col] - df_scoring[col].min()) / (df_scoring[col].max() - df_scoring[col].min())

    # --- 3. Calcul du Score Final (Pondération) ---
    df_scoring['score_final'] = (
        (df_scoring['taux_proprio_n'] * 0.35) +   # Stock de mandats
        (df_scoring['taux_rotation_n'] * 0.30) +  # Preuve historique
        (df_scoring['indice_seniors_n'] * 0.20) + # Anticipation Décès
        (df_scoring['indice_jeunes_n'] * 0.15)    # Anticipation Déplacement
    ) * 100

    # Tri pour obtenir les "Hotspots" (Points Chauds)
    df_final = df_scoring.sort_values(by='score_final', ascending=False)
    df_final.to_csv("scoring.csv", index=False)
    return df_final

df_scoring = scoring()

