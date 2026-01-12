GML Tag Analyzer
Overzicht

Dit script analyseert GML-bestanden in een opgegeven map en zoekt de meest voorkomende XML-tags in een fragment van elk bestand.
Het helpt om snel inzicht te krijgen in de belangrijkste tags en een geschikte “rowTag” te selecteren voor verdere verwerking.

Wat doet het script?

Scant alle .gml bestanden in een specifieke map.

Leest de eerste 1000 regels van elk bestand (exclusief commentaar).

Vindt en telt XML-tags met een regex-patroon.

Filtert tags om onbelangrijke of irrelevante tags uit te sluiten.

Toont de top 5 meest voorkomende tags per bestand.

Stelt een geschikte “rowTag” voor op basis van geldige keywords en uitsluitingscriteria.

Wat is een “rowTag” en waarom is het belangrijk?

In GML-bestanden worden data vaak opgeslagen in herhalende elementen (tags) die elk één object of record representeren.

De “rowTag” is het XML-element dat één individuele data-eenheid (of “rij”) in het bestand aangeeft.

Waarom is dit belangrijk?

Om data uit GML-bestanden te halen en verder te verwerken, moet je weten welk tag-element één record voorstelt.

Dit maakt het makkelijker om de data correct te parseren en om te zetten naar bijvoorbeeld tabellen of databases.

Het script helpt een logische “rowTag” te vinden door de meest voorkomende en relevante tags te identificeren.

Gebruik

Pas de variabele gml_folder aan naar de map met jouw GML-bestanden (bijv. "/dbfs/mnt/processeddata/unzipped_files").

Voer het script uit in een Python-omgeving met toegang tot de bestanden.

Bekijk in de output de meest voorkomende tags en de voorgestelde rowTag per bestand.

Benodigdheden

Python 3.x

Standaard Python libraries: os, re, collections

Voorbeeldoutput
Bestand: 'voorbeeld.gml'
  Tag: cityObjectMember - Aantal keer gevonden: 45
  Tag: gml:name - Aantal keer gevonden: 30
  Tag: Object - Aantal keer gevonden: 25
  Tag: gml:pos - Aantal keer gevonden: 20
  Tag: creationDate - Aantal keer gevonden: 15
Voorgestelde rowTag: cityObjectMember

Contributie

Suggesties en verbeteringen zijn welkom! Open een issue of stuur een pull request.