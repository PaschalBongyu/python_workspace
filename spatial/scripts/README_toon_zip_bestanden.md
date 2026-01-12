README - toon_zip_bestanden.py
Doel

Dit project bevat het script toon_zip_bestanden.py, waarmee je een ZIP-bestand vanuit het Databricks File System (DBFS) kunt uitlezen. Het script toont alle bestanden in het ZIP-archief, inclusief hun laatste wijzigingsdatum.

Wat doet het script?

Kopieert het ZIP-bestand (test.zip) van DBFS naar de lokale driver node (/tmp).

Opent het ZIP-bestand met Python's zipfile module.

Print een lijst van alle bestandsnamen in het archief.

Toont de laatste wijzigingsdatum van elk bestand

Voorbeeld output
Bestanden in test.zip met laatste wijzigingsdatum:

data/file1.gml - Laatst gewijzigd: 2023-08-15 10:22:00
data/file2.gml - Laatst gewijzigd: 2023-08-15 10:22:00
...

Benodigdheden

Toegang tot een Databricks-omgeving.

Een ZIP-bestand beschikbaar in DBFS (bijv. dbfs:/mnt/rawdata/test.zip).

Python met toegang tot dbutils (beschikbaar in Databricks notebooks).

Gebruik

Plaats toon_zip_bestanden.py in je Databricks workspace of cluster en voer het uit. Pas eventueel het DBFS-pad aan naar het ZIP-bestand dat je wilt openen.

Bestanden in deze repository

toon_zip_bestanden.py – Het Python-script voor het uitlezen van ZIP-bestanden.

README.md (of README_toon_zip_bestanden.md) – Deze documentatie.
