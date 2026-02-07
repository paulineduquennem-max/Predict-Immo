import streamlit as st
import pandas as pd
import pydeck as pdk
import ast
import base64

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="Predict Immo", layout="wide")
def set_bg(img_path, opacity=0.25):
    try:
        with open(img_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        st.markdown(f"""
            <style>
            .stApp {{ background: transparent; }}
            .stApp::before {{
                content: ""; position: fixed; inset: 0;
                background-image: url("data:image/png;base64,{b64}");
                background-size: cover; background-position: center;
                opacity: {opacity}; z-index: -1;
            }}
            </style>
        """, unsafe_allow_html=True)
    except:
        st.warning("⚠️ Image de fond 'background.png' non trouvée.")

set_bg("background.png", 0.25)

# --- 2. CHARGEMENT ET PRÉPARATION DES DONNÉES ---
@st.cache_data
def load_data():
    df_sq = pd.read_csv("scoring.csv")
    df_q = pd.read_csv("df_quartiers.csv")
    
    try:
        dpe_all = pd.concat([pd.read_csv("df_dpe_ancien.csv"), pd.read_csv("df_dpe_neuf.csv")], ignore_index=True)
    except:
        dpe_all = pd.DataFrame(columns=['gid', 'etiquette_dpe'])
    
    df_sq['poly_list'] = df_sq['polygone'].apply(ast.literal_eval)
    df_q['poly_list'] = df_q['polygone'].apply(ast.literal_eval)
    
    # Préparation Tooltip
    df_sq['score_show'] = df_sq['score_final'].round(2)
    
    if not dpe_all.empty:
        dpe_dist = dpe_all.groupby(['gid', 'etiquette_dpe']).size().unstack(fill_value=0)
        dpe_pct = dpe_dist.div(dpe_dist.sum(axis=1), axis=0)
        def format_dpe(row):
            items = [f"<b>{k}</b>:{v:.0%}" for k, v in row.items() if v > 0]
            return " ".join(items)
        df_sq['dpe_str'] = df_sq['gid'].map(dpe_pct.apply(format_dpe, axis=1)).fillna("Non dispo.")
    
    df_sq['txt_logements'] = df_sq.apply(lambda r: f"🏠 Maisons: {r['nb_men_hab_ind']/(r['nb_men'] if r['nb_men']>0 else 1):.0%} | Apparts: {r['nb_men_hab_col']/(r['nb_men'] if r['nb_men']>0 else 1):.0%}", axis=1)
    df_sq['txt_nb_logements'] = df_sq.apply(lambda r: f"🏠 Maisons: {r['nb_men_hab_ind']} | Apparts: {r['nb_men_hab_col']}", axis=1)
    df_sq['txt_proprio'] = df_sq.apply(lambda r: f"🔑 Proprios: {r['nb_men_prop']/(r['nb_men'] if r['nb_men']>0 else 1):.0%} | Locat.: {1-(r['nb_men_prop']/(r['nb_men'] if r['nb_men']>0 else 1)):.0%}", axis=1)
    df_sq['txt_demo'] = df_sq.apply(lambda r: f"👥 Jeunes: {r['indice_jeunes']:.0%} | Séniors: {r['indice_seniors']:.1%}", axis=1)
    
    return df_sq, df_q

df_sq, df_q = load_data()

# --- 3. LOGO ET TITRE ---
col_logo, col_titre = st.columns([1, 8])
with col_logo:
    st.image("logo_predictimmo.png", width=100)
with col_titre:
    st.markdown("<h1 style='margin-top: 15px;'>Predict'Immo : Potentiel de prospection</h1>", unsafe_allow_html=True)

# 4. FILTRE ET BANDEAU D'INFORMATIONS (Le retour !) ---
parents = sorted(df_sq["nom_quartier_parent"].dropna().unique().tolist())
search_quartier = st.selectbox("🔍 Sélectionner un quartier :", ["— Bordeaux —"] + parents)

if search_quartier != "— Bordeaux —":
    subset = df_sq[df_sq["nom_quartier_parent"] == search_quartier]
    m = subset['nb_men'].sum() if subset['nb_men'].sum() > 0 else 1
    
    st.markdown(f"### 🏙️ Analyse globale : {search_quartier}")
    # Calcul des indicateurs pour le bandeau
    prop_q = subset['nb_men_prop'].sum() / m
    indiv_q = subset['nb_men_hab_ind'].sum() / m
    
    # Affichage en 4 colonnes
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Propriétaires", f"{prop_q:.1%}")
    c2.metric("Locataires", f"{1-prop_q:.1%}")
    c3.metric("Maisons", f"{indiv_q:.1%}")
    c4.metric("Appartements", f"{1-indiv_q:.1%}")
else:
    # --- VUE GÉNÉRALE : PALMARÈS DES QUARTIERS ---
    st.markdown("### 🏆 Palmarès des quartiers ###")
    
    # Calcul des moyennes par quartier parent
    df_palmares = df_sq.groupby("nom_quartier_parent")["score_final"].mean().reset_index()
    df_palmares.columns = ["Quartier", "Score Moyen"]
    df_palmares = df_palmares.sort_values(by="Score Moyen", ascending=False)
    
    # Affichage en colonnes pour ne pas prendre trop de place verticale
    n_cols = 4
    for i, row in enumerate(df_palmares.itertuples()):
        if i % n_cols == 0:
            cols = st.columns(n_cols)
        with cols[i % n_cols]:
            st.metric(label=row.Quartier, value=f"{row._2:.1f}")

    st.info("💡 Sélectionnez un quartier dans la liste ci-dessus pour zoomer et voir les indicateurs démographiques détaillés.")

# --- 5. ZOOM ET CARTE ---
lat, lon, zoom = 44.85, -0.579, 11.2
if search_quartier != "— Bordeaux —":
    df_view = df_sq[df_sq["nom_quartier_parent"] == search_quartier].copy()
    if not df_view.empty:
        all_coords = [p for poly in df_view['poly_list'] for p in poly]
        lat, lon, zoom = sum(c[1] for c in all_coords)/len(all_coords), sum(c[0] for c in all_coords)/len(all_coords), 12.5
else:
    df_view = df_sq.copy()

s_min, s_max = df_sq["score_final"].min(), df_sq["score_final"].max()
df_view['fill_color'] = df_view['score_final'].apply(lambda x: [int(255*((x-s_min)/(s_max-s_min))), int(255*(1-(x-s_min)/(s_max-s_min))), 40, 160])

# --- 6. RÉGLAGE DU TOOLTIP POUR ÉVITER LES COUPURES ---
# On utilise un style qui force l'infobulle à se repositionner (transform-origin)
tooltip_html = """
<div style="font-family: 'Segoe UI', sans-serif; padding: 10px; 
            background-color: rgba(28, 28, 28, 0.98); color: white; 
            border-radius: 8px; border: 1px solid #e63946; 
            font-size: 11.5px; line-height: 1.3; min-width: 220px;
            pointer-events: none; transform: translateY(-100%); margin-top: -10px;">
    <div style="color: #e63946; font-weight: bold;">SECTEUR GID {gid}</div>
    <div style="font-size: 15px; font-weight: bold; margin-bottom: 3px;">🎯 Score : {score_show}</div>
    <hr style="margin: 5px 0; border: 0.2px solid #555;">
    {txt_demo}<br/>{txt_logements}<br/>{txt_nb_logements}<br/>{txt_proprio}
    <hr style="margin: 5px 0; border: 0.2px solid #555;">
    <span style="color: #ffb703; font-weight: bold;">⚡ DPE :</span> {dpe_str}
</div>
"""

deck = pdk.Deck(
    layers=[
        pdk.Layer("PolygonLayer", data=df_q, get_polygon="poly_list", filled=False, stroked=True, get_line_color=[0, 0, 0, 255], get_line_width=12),
        pdk.Layer("PolygonLayer", data=df_view, get_polygon="poly_list", get_fill_color="fill_color", get_line_color=[255, 255, 255, 30], pickable=True, auto_highlight=True)
    ],
    initial_view_state=pdk.ViewState(latitude=lat, longitude=lon, zoom=zoom),
    map_style="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
    height=750,
    tooltip={"html": tooltip_html, "style": {"backgroundColor": "transparent", "zIndex": "10000"}}
)

st.pydeck_chart(deck, use_container_width=True)