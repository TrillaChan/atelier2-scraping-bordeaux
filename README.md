# Atelier 2 – Collecte en ligne (Scraping)

## Objectif
Collecter des données touristiques depuis le site https://www.bordeaux-tourisme.com
afin d’enrichir un datalake local utilisé dans un projet de modélisation du trafic.

Les données ne sont pas disponibles en open data et sont collectées via scraping
responsable.

## Structure du projet
- `scrape_agenda_list.py` : extraction de la liste des événements (agenda)
- `scrape_agenda_details.py` : exploration des pages détail pour enrichissement
- `data/raw/` : données brutes (parquet, non versionnées)
- `data/curated/` : données enrichies (parquet, non versionnées)

## Installation
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
python -m pip install -r requirements.txt
