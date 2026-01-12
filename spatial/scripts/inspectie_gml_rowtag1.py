import os
import re
from collections import Counter

# Stap 1: Geef de map op waar de GML-bestanden staan (opgeslagen in DBFS)
gml_folder = "/dbfs/mnt/processeddata/unzipped_files"

# Stap 2: Maak een lijst van alle GML-bestanden in de map
gml_files = [f for f in os.listdir(gml_folder) if f.endswith(".gml")]

def find_tags(lines):
    """
    Haalt XML-tags uit regels tekst met behulp van regex en telt hoe vaak ze voorkomen.
    Voorbeeld van een tag: <gml:featureMember>
    """
    pattern = re.compile(r'<([a-zA-Z0-9_:]+)')  # Regex om tag-namen te vinden
    tags = []
    for line in lines:
        tags.extend(pattern.findall(line))
    return Counter(tags)

# Stap 3: Ga elk GML-bestand af om de tags te bekijken
for file in gml_files:
    path = os.path.join(gml_folder, file)

   

    # Leest de eerste 300 regels van elk GML-bestand (in plaats van 30) 
    # Slaat lege regels en regels met XML-commentaar (<!-- ... -->) over 
    # Zoekt naar XML-tags in deze regels en telt hoe vaak ze voorkomen 
    # Print de 5 meest voorkomende tags, zelfs als ze maar 1 keer voorkomen 
    # Stelt de tag voor die het vaakst voorkomt als mogelijke 'rowTag' 

    # Stap 4: Lees de eerste 300 regels van het bestand (en verwijder lege regels en comments)
    with open(path, 'r', encoding='utf-8') as f:
        snippet = []
        for _ in range(300):
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if line and not line.startswith("<!--"):
                snippet.append(line)

    # Stap 5: Zoek alle tags in de snippet en tel hoe vaak ze voorkomen
    tag_counts = find_tags(snippet)

    # Stap 6: Toon de top 5 tags (ongeacht hoe vaak ze voorkomen)
    candidate_rowtags = dict(tag_counts.most_common(5))

    print(f"\nBestand: '{file}'")
    if candidate_rowtags:
        for tag, count in candidate_rowtags.items():
            print(f"  Tag: {tag} - Aantal keer gevonden in sample: {count}")
        
        best_tag = max(candidate_rowtags, key=candidate_rowtags.get)
        print(f"Voorgestelde rowTag: {best_tag}")
    else:
        print("Geen bruikbare tags gevonden — sample mogelijk vergroten of bestand handmatig inspecteren.")
