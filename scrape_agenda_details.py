from __future__ import annotations

import time
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


def mask_domain(url: str) -> str:
    return url.replace("bordeaux-tourisme.com", "tourisme.example")


def unmask_domain(url: str) -> str:
    return url.replace("tourisme.example", "bordeaux-tourisme.com")


def fetch_html(url: str) -> str:
    headers = {"User-Agent": "EPSI-scraper/1.0 (projet pedagogique)"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text


def parse_detail(html: str) -> dict:
    """
    Parse une page détail d'événement.
    On fait robuste : on récupère ce qu'on peut, sinon None.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Titre (souvent un H1)
    h1 = soup.select_one("h1")
    title_detail = h1.get_text(strip=True) if h1 else None

    # Texte date (souvent près du titre, parfois dans un bloc spécifique)
    # Ici on tente plusieurs sélecteurs possibles, sinon None.
    date_detail = None
    for sel in ["time", ".Date", ".Event-date", ".date", ".field--name-field-date"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            date_detail = el.get_text(" ", strip=True)
            break

    # Description : on tente des zones “contenu”
    description = None
    for sel in [".field--name-body", ".node__content .field", ".wysiwyg", ".content", "article p"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            description = el.get_text(" ", strip=True)
            break

    return {
        "detail_title": title_detail,
        "detail_date_text": date_detail,
        "description": description,
    }


def main() -> None:
    # 1) trouver le dernier fichier parquet "liste"
    raw_dir = Path("data/raw")
    list_files = sorted(raw_dir.glob("agenda_list_*.parquet"))
    if not list_files:
        raise FileNotFoundError("Aucun fichier agenda_list_*.parquet trouvé dans data/raw")

    input_path = list_files[-1]
    df = pd.read_parquet(input_path)

    # 2) on limite pendant les tests (tu pourras enlever ensuite)
    df = df.head(10)

    details = []
    for i, row in df.iterrows():
        masked_url = row["url"]
        real_url = unmask_domain(masked_url)

        try:
            html = fetch_html(real_url)
            parsed = parse_detail(html)

            details.append(
                {
                    "url": masked_url,  # on garde la version masquée en sortie
                    **parsed,
                }
            )

            # Scraping responsable : petite pause
            time.sleep(random.uniform(1.0, 2.0))

        except Exception as e:
            details.append(
                {
                    "url": masked_url,
                    "detail_title": None,
                    "detail_date_text": None,
                    "description": None,
                    "error": str(e),
                }
            )

    df_details = pd.DataFrame(details)

    # 3) merger (enrichir)
    df_enriched = df.merge(df_details, on="url", how="left")

    # 4) écrire parquet enrichi
    Path("data/curated").mkdir(parents=True, exist_ok=True)
    out = f"data/curated/agenda_enriched_{datetime.now().strftime('%Y-%m-%dT%H%M%S')}.parquet"
    df_enriched.to_parquet(out, index=False)
    print("Saved:", out)
    print(df_enriched.head(3))


if __name__ == "__main__":
    main()
