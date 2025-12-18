from __future__ import annotations

from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE = "https://www.bordeaux-tourisme.com"
LIST_URL = "https://www.bordeaux-tourisme.com/agenda.html"


def fetch_html(url: str) -> str:
    headers = {"User-Agent": "EPSI-scraper/1.0 (projet pedagogique)"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text


def mask_domain(url: str) -> str:
    return url.replace("bordeaux-tourisme.com", "tourisme.example")


def main() -> None:
    html = fetch_html(LIST_URL)
    soup = BeautifulSoup(html, "html.parser")

    cards = soup.select("div.ListSit-item.js-list-sit-item")
    print("Cards found:", len(cards))

    rows = []
    for card in cards:
        a = card.select_one("a.Card[href]")
        title_el = card.select_one("p.Card-title")
        date_el = card.select_one("p.Card-label")

        if not a or not title_el:
            continue

        raw_url = a.get("href")
        full_url = raw_url if raw_url.startswith("http") else urljoin(BASE, raw_url)

        rows.append(
            {
                "title": title_el.get_text(strip=True),
                "date_text": date_el.get_text(strip=True) if date_el else None,
                "url": mask_domain(full_url),
                "scraped_at": datetime.now().isoformat(timespec="seconds"),
                "source_page": mask_domain(LIST_URL),
            }
        )

    df = pd.DataFrame(rows)

    out = f"data/raw/agenda_list_{datetime.now().strftime('%Y-%m-%dT%H%M%S')}.parquet"
    df.to_parquet(out, index=False)
    print("Saved:", out)
    print(df.head(3))


if __name__ == "__main__":
    main()
