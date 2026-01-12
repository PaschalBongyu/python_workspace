import os
import re
from collections import Counter

# Stap 1: Definieer de map waar de GML-bestanden zijn opgeslagen in DBFS (lokale driver pad)
gml_folder = "/dbfs/mnt/processeddata/unzipped_files"

# Stap 2: Maak een lijst van alle GML-bestanden in de map
gml_files = [f for f in os.listdir(gml_folder) if f.endswith(".gml")]

def find_tags(lines):
    """
    Extraheert XML openende tags uit regels tekst met regex en telt hun frequentie.
    Voorbeeld tag: <gml:featureMember>
    """
    pattern = re.compile(r'<([a-zA-Z0-9_:]+)')  # Regex voor tag-namen
    tags = []
    for line in lines:
        tags.extend(pattern.findall(line))
    return Counter(tags)

# Stap 3: Loop door elk GML-bestand om de tags te inspecteren
for file in gml_files:
    path = os.path.join(gml_folder, file)
    
    # Stap 4: Lees de eerste 100 regels om een voorbeeldfragment van het bestand te krijgen
    with open(path, 'r', encoding='utf-8') as f:
        snippet = [f.readline().strip() for _ in range(100)]
    
    # Stap 5: Vind alle tags en tel het aantal keren dat ze voorkomen in het fragment

