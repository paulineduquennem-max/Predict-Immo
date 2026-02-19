from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator   
from new_predictimmo import (telecharger_dataset_en_csv, get_dpe_neuf, get_dpe_ancien, get_ventes_foncieres, scoring)  

with DAG("predict-immo_pipeline", start_date=datetime(2026,6,30), schedule_interval="0 8 1 */6 *",catchup=False) as dag:
    t1 = PythonOperator(
        task_id="prepare_base_quartiers",
        python_callable=telecharger_dataset_en_csv
    )

    # Étape 2 : Récupération des DPE Neufs
    t3 = PythonOperator(
        task_id="get_dpe_neuf",
        python_callable=get_dpe_neuf
    )

    # Étape 3 : Récupération des DPE Anciens
    t4 = PythonOperator(
        task_id="get_dpe_ancien",
        python_callable=get_dpe_ancien
    )

    # Étape 4 : Récupération des Ventes
    t5 = PythonOperator(
        task_id="get_ventes_foncieres",
        python_callable=get_ventes_foncieres
    )

    # Étape 5 : Calcul du score final (lit les CSV générés par t1, t3, t4, t5)
    t8 = PythonOperator(
        task_id="calcul_scoring",
        python_callable=scoring
    )

# Définition de l'ordre d'exécution
t1 >> [t3, t4, t5] >> t8