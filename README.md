# GDELT News Module

Ein leichtgewichtiges Python-Modul, das die GDELT DOC 2.0 API als primaere Quelle fuer globale Nachrichtenberichterstattung nutzt und die Ergebnisse in ein konsistentes, nachgelagert nutzbares Format ueberfuehrt.

## Was das Modul abdeckt

- Abruf von Artikeldaten fuer definierte Zeitraeume ueber `STARTDATETIME` / `ENDDATETIME` oder `timespan`
- Inhaltsfilter fuer Keywords, exakte Phrasen, Themen, Entitaeten, Domains, Quelllaender und Quellsprachen
- Normalisierte Ausgabe als JSON-Dataset
- Optionaler CSV-Export fuer tabellarische Weiterverarbeitung
- Analysefreundliche Nebenprodukte fuer Trends, Sentiment und Entitaeten-Tracking
- Optionales eigenes Scraping der gefundenen Artikel-URLs zur Textanreicherung
- Zeitfenster-Batching und Deduplikation fuer groessere Abfragen
- Schema-Validierung fuer reproduzierbare Downstream-Nutzung

## Projektstruktur

- `src/gdelt_news/client.py`: GDELT-Client, Query-Building und Rate-Limit-konformer Abruf
- `src/gdelt_news/scraper.py`: Optionales Scraping und HTML-Extraktion pro Artikel-URL
- `src/gdelt_news/normalize.py`: Normalisierung der Rohantworten
- `src/gdelt_news/analysis.py`: Aggregationen fuer Trend- und Entitaetsreihen
- `src/gdelt_news/cli.py`: CLI zum Starten des Abrufs
- `src/gdelt_news/validate.py`: Validierung gegen das Dataset-Schema
- `schemas/gdelt_news_dataset.schema.json`: JSON-Schema fuer die Ausgabedatei
- `examples/climate_request.json`: Beispielkonfiguration

## Installation

```bash
python3 -m pip install -e .
```

Ohne Installation direkt aus dem Projektordner:

```bash
PYTHONPATH=src python3 -m gdelt_news --help
```

## Schnellstart

Mit einer JSON-Konfiguration:

```bash
gdelt-news \
  --query-file examples/climate_request.json \
  --ca-bundle "$(python3 -m certifi)" \
  --output data/climate_dataset.json \
  --csv-output data/climate_articles.csv
```

Direkt ueber CLI-Parameter ohne JSON-Datei:

```bash
gdelt-news \
  --label "label for json" \
  --keyword "keyword1" \
  --keyword "keyword2" \
  --entity "entity1" \
  --entity "enitity2" \
  --source-country opt1 \
  --source-country opt2 \
  --source-language lang1 \
  --source-language lang2 \
  --start 2026-04-16T00:00:00Z \
  --end 2026-04-16T23:59:59Z \
  --match-mode any \
  --sort DateDesc \
  --bucket-size hour \
  --batch-window-hours 24 \
  --scrape-articles \
  --scrape-limit 10 \
  --scrape-timeout 15 \
  --output data/title.json \
  --csv-output data/title.csv \
  --ca-bundle "$(python3 -m certifi)"
```

Interaktiv mit Rueckfragen im Terminal:

```bash
gdelt-news --interactive
```

Dann fragt das Tool dich Schritt fuer Schritt nach Zeitraum, Keywords, Entitaeten, Laendern, Sprachen, Ausgabe-Dateinamen und optionalem Scraping.

## Nutzung

Wichtige Regel:

- Mit `--query-file` kommen Filter und Zeitraum aus der JSON-Datei.
- Wenn du alles direkt im Command eingeben willst, lass `--query-file` weg.
- Wenn du nicht alle Flags jedes Mal ausschreiben willst, nutze `--interactive`.
- Den Namen des Runs setzt du mit `--label`.
- Den Namen der Ausgabedateien setzt du mit `--output` fuer JSON und `--csv-output` fuer CSV.
- Mehrere Keywords, Entitaeten, Laender oder Sprachen gibst du an, indem du dasselbe Flag mehrfach verwendest.

Empfohlener Ablauf:

1. Erst einen kleinen Testlauf ohne `--scrape-articles`, `timeline` und `tone_timeline`.
2. Danach bei Bedarf den Zeitraum vergroessern.
3. Ganz zum Schluss optional Scraping und Timeline-Calls aktivieren.

Schneller Testlauf:

```bash
gdelt-news \
  --label "quick-test" \
  --keyword "Friedrich Merz" \
  --source-country germany \
  --start 2026-04-14T00:00:00Z \
  --end 2026-04-16T23:59:59Z \
  --sort DateDesc \
  --bucket-size day \
  --batch-window-hours 72 \
  --no-timeline \
  --no-tone-timeline \
  --output data/quick_test.json \
  --csv-output data/quick_test.csv \
  --ca-bundle "$(python3 -m certifi)"
```

Interaktiver Testlauf:

```bash
gdelt-news --interactive
```

Typische Rueckfragen dabei:

- Keywords, exakte Phrasen, Entitaeten, Themen
- Quelllaender und Quellsprachen
- Startdatum/Enddatum oder alternativ `timespan`
- Timeline, Tone-Timeline und optionales Scraping
- JSON- und CSV-Ausgabepfade

## CLI-Felder

### Inhalte und Suchlogik

| Feld | Was es macht | Typischer Einsatz |
| --- | --- | --- |
| `--keyword "..."` | Normales Suchwort oder Suchphrase. Mehrfach nutzbar. | `--keyword "climate change"` |
| `--phrase "..."` | Exakte Phrase. | `--phrase "Green Deal"` |
| `--entity "..."` | Person, Organisation oder Institution, die vorkommen soll. | `--entity "European Union"` |
| `--theme "..."` | GDELT-Theme-Code. | `--theme ENV_CLIMATECHANGE` |
| `--exclude "..."` | Schliest Treffer mit diesem Begriff aus. | `--exclude sports` |
| `--domain "..."` | Begrenzt Treffer auf eine Domain-Gruppe. | `--domain reuters.com` |
| `--exact-domain "..."` | Begrenzt Treffer auf genau diese Domain. | `--exact-domain www.reuters.com` |
| `--raw-fragment "..."` | Eigener GDELT-Query-Teil fuer Spezialfaelle. | Fuer fortgeschrittene GDELT-Queries |
| `--match-mode any` | Mindestens einer der Inhaltsfilter reicht aus. | Breitere Suche |
| `--match-mode all` | Alle Inhaltsfilter muessen vorkommen. | Engere Suche |

### Quelle der Berichterstattung

| Feld | Was es macht | Typischer Einsatz |
| --- | --- | --- |
| `--source-country ...` | Filtert nach Herkunftsland der Quelle. Mehrfach nutzbar. | `--source-country germany --source-country france` |
| `--source-language ...` | Filtert nach Sprache der Quelle. Mehrfach nutzbar. | `--source-language english --source-language german` |

Mehrere `source-country`-Werte werden als `ODER` kombiniert. Mehrere `source-language`-Werte ebenfalls. Zusammen mit Keywords und Entitaeten werden diese Gruppen dann als `UND` kombiniert.

### Zeitraum

| Feld | Was es macht | Typischer Einsatz |
| --- | --- | --- |
| `--start ...` | Startzeitpunkt in ISO-8601 und idealerweise UTC mit `Z`. | `--start 2026-04-16T00:00:00Z` |
| `--end ...` | Endzeitpunkt in ISO-8601 und idealerweise UTC mit `Z`. | `--end 2026-04-16T23:59:59Z` |
| `--timespan ...` | Relative Zeit statt `start/end`. | `--timespan 24h` oder `--timespan 7days` |

Nutze entweder `--start` und `--end` oder `--timespan`, nicht beides zusammen.

### Ausgabe

| Feld | Was es macht | Typischer Einsatz |
| --- | --- | --- |
| `--interactive` | Startet einen gefuehrten Prompt statt alle Filter direkt im Command zu verlangen. | `gdelt-news --interactive` |
| `--label "..."` | Frei waehlbarer Name des Runs. Wird im JSON gespeichert. | `--label "eu-climate-2026-04-16"` |
| `--output ...` | Pfad und Dateiname der JSON-Ausgabe. | `--output data/output.json` |
| `--csv-output ...` | Pfad und Dateiname der CSV-Ausgabe. | `--csv-output data/output.csv` |
| `--sort ...` | Sortierung der Artikelliste. | `DateDesc` ist meist am sinnvollsten |
| `--bucket-size ...` | Groesse der Zeit-Buckets fuer Trend- und Sentiment-Reihen. | `minute`, `hour`, `day` |

`--bucket-size` aendert nicht die Suche selbst, sondern nur die Granularitaet der Analyse-Ausgabe.

### Leistung, API-Last und Zusatzfunktionen

| Feld | Was es macht | Typischer Einsatz |
| --- | --- | --- |
| `--batch-window-hours ...` | Zerlegt grosse Zeitraeume in kleinere API-Fenster. | Hoeher = weniger Requests |
| `--max-records ...` | Maximale Trefferzahl pro GDELT-Artikelfenster. | Standard meist ausreichend |
| `--no-timeline` | Ueberspringt die Trend-Zeitreihe von GDELT. | Schnellere Testlaeufe |
| `--no-tone-timeline` | Ueberspringt die Tone- bzw. Sentiment-Zeitreihe. | Schnellere Testlaeufe |
| `--scrape-articles` | Laedt zusaetzlich die gefundenen Artikel-Seiten. | Nur wenn du echten Seitentext brauchst |
| `--scrape-limit ...` | Begrenzung fuer die Zahl der zu scrapenden Artikel. | `--scrape-limit 10` |
| `--scrape-timeout ...` | Timeout pro gescraptem Artikel in Sekunden. | `--scrape-timeout 15` |
| `--timeout ...` | Timeout fuer den API-Request. | Standard meist okay |
| `--min-request-interval ...` | Mindestabstand zwischen API-Requests. | Hoeher hilft gegen Rate-Limits |
| `--rate-limit-retries ...` | Wie oft bei `429 Too Many Requests` erneut versucht wird. | Standard meist okay |
| `--rate-limit-backoff-seconds ...` | Basiswartezeit bei `429`. | Fuer harte Limits erhoehen |
| `--skip-schema-validation` | Schaltet die JSON-Schema-Pruefung aus. | Nur fuer Debugging |
| `--ca-bundle ...` | PEM-Datei fuer SSL-Zertifikate. | Auf macOS oft `$(python3 -m certifi)` |

## Request-Format fuer JSON-Dateien

```json
{
  "request_label": "climate-monitoring",
  "filters": {
    "keywords": ["climate change", "global warming"],
    "entities": ["European Union", "United Nations"],
    "source_countries": ["germany", "france"],
    "source_languages": ["english", "german"],
    "exclude_terms": ["sports"],
    "match_mode": "any"
  },
  "date_range": {
    "start": "2026-04-15T00:00:00Z",
    "end": "2026-04-16T23:59:59Z"
  },
  "max_records": 100,
  "sort": "DateDesc",
  "bucket_size": "hour",
  "batch_window_hours": 12,
  "deduplicate": true,
  "validate_schema": true,
  "scrape_articles": true,
  "scrape_limit": 10,
  "scrape_timeout": 15,
  "include_timeline": true,
  "include_tone_timeline": true
}
```

## Ausgabe

Die JSON-Ausgabe enthaelt vier Hauptbereiche:

- `request`: Query-Metadaten, Parameter und Zeitbereich
- `articles`: normalisierte Artikeldaten
- `analytics`: vorbereitete Reihen fuer Trend-, Sentiment- und Entitaetsanalysen
- `diagnostics`: Warnungen, Fetch-Qualitaet, Schema-Validierung

Wenn GDELT `timelinevolraw` oder `timelinetone` nicht liefert, faellt das Modul kontrolliert auf lokale Aggregationen der Artikelliste zurueck. Das ist im Feld `diagnostics` sichtbar.

Wichtige Analysefelder im Output:

- `analytics.analysis_targets`: beschreibt die vorgesehenen Analyseobjekte
- `analytics.trend_series`: Zeitreihe fuer Volumen- und Trendanalysen
- `analytics.sentiment_series`: Zeitreihe fuer Sentiment bzw. Tone-Auswertungen
- `analytics.entity_tracking`: Zeitreihe fuer das Tracking der angefragten Entitaeten

Wichtige Scraping-Felder im Output:

- `articles[].scraped_text`: der extrahierte Artikeltext, sofern Scraping erfolgreich war
- `articles[].scraped_title` und `articles[].scraped_description`: angereicherte Seitendaten
- `articles[].scrape_status`: Status des Scraping-Versuchs
- `diagnostics.scraping`: Gesamtuebersicht ueber versuchte, erfolgreiche und fehlgeschlagene Scrapes

## Troubleshooting

### CSV ist leer

- Pruefe zuerst `articles` in der JSON-Datei. Wenn dort `0` Eintraege stehen, hat die Anfrage keine Treffer geliefert.
- Zu enge Filter sind der haeufigste Grund: zu viele Laender, zu viele Sprachfilter oder ein zu kleiner Zeitraum.
- Fuer einen Testlauf zuerst weniger Filter setzen und `--scrape-articles` weglassen.

### HTTP 429 Too Many Requests

- GDELT hat die Anfrage gedrosselt.
- Warte `1-2 Minuten` und probiere es erneut.
- Verkleinere die Last mit `--no-timeline`, `--no-tone-timeline` und einem groesseren `--batch-window-hours`.
- Ein laengerer Zeitraum mit sehr kleinen API-Fenstern fuehrt schnell zu vielen Requests.

### CERTIFICATE_VERIFY_FAILED

- Auf macOS hilft oft `open "/Applications/Python 3.13/Install Certificates.command"`.
- Alternativ den Run mit `--ca-bundle "$(python3 -m certifi)"` starten.

## Qualitaetsmerkmale

- Explizite Zeitraeume koennen in kleinere Fenster zerlegt werden, damit grosse Zeitraeume stabiler abgefragt werden.
- Artikel werden standardmaessig ueber URL oder Fallback-Schluessel dedupliziert.
- Fenster, die das `max_records`-Limit erreichen, werden als potenziell abgeschnitten markiert.
- Das Ausgabe-JSON wird standardmaessig gegen das bereitgestellte Schema geprueft.
- Optionales Scraping reichert die GDELT-Treffer mit direkt extrahierten Seitentexten an und verbessert damit Keyword- und Entity-Matching.

## Hinweise zur GDELT-API

- Das Modul taktet API-Aufrufe standardmaessig mit `6` Sekunden Abstand und versucht bei `429 Too Many Requests` automatisch erneut, um das GDELT-Rate-Limit besser einzuhalten.
- `themes` erwarten gueltige GKG-Theme-Codes aus dem GDELT-Oekosystem.
- `max_records` ist fuer `artlist` auf `250` begrenzt.
- Wenn ein Zeitfenster exakt `max_records` erreicht, kann GDELT noch weitere Treffer enthalten. Diese Fenster werden in `diagnostics.article_fetch.windows` markiert.
- Bei `CERTIFICATE_VERIFY_FAILED` auf macOS hilft oft `open "/Applications/Python 3.13/Install Certificates.command"` oder der Start mit `--ca-bundle "$(python3 -m certifi)"`.
