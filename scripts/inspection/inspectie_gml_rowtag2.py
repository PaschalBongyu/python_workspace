import os
import re
from collections import Counter

# Stap 1: Map met GML-bestanden
gml_folder = "/dbfs/mnt/processeddata/unzipped_files"

# Stap 2: Lijst van alle GML-bestanden
gml_files = [f for f in os.listdir(gml_folder) if f.endswith(".gml")]

def find_tags(lines):
    """
    Haalt XML-tags uit regels tekst met regex en telt hoe vaak ze voorkomen.
    Voorbeeld van een tag: <gml:featureMember>
    """
    pattern = re.compile(r'<([a-zA-Z0-9_:]+)')  # Regex om tag-namen te vinden
    tags = []
    for line in lines:
        tags.extend(pattern.findall(line))
    return Counter(tags)

# Definieer ongeldige prefixes (veldjes die géén rowTag zijn)
invalid_prefixes = [
    't:', 'gml:pos', 'gml:Point', 'ko:', 'imgeo:', 'h:', 'oz:', 'kad:', 'creationDate'
]

# Sleutelwoorden voor geldige rowTags
valid_keywords = ['Member', 'featureMember', 'Object', 'cityObjectMember']

# Stap 3: Verwerk elk GML-bestand
for file in gml_files:
    path = os.path.join(gml_folder, file)

    # Leest 300 regels
    # Slaat lege regels en commentaar over
    # Filtert veldjes eruit
    # Stelt alleen logische rowTags voor

    with open(path, 'r', encoding='utf-8') as f:
        snippet = []
        for _ in range(300):
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if line and not line.startswith("<!--"):
                snippet.append(line)

    tag_counts = find_tags(snippet)
    top_tags = dict(tag_counts.most_common(5))

    print(f"\nBestand: '{file}'")
    for tag, count in top_tags.items():
        print(f"  Tag: {tag} - Aantal keer gevonden: {count}")

    # Filter: alleen geldige rowTag-kandidaten overhouden
    candidate_rowtags = {
        tag: count for tag, count in tag_counts.items()
        if not any(tag.startswith(prefix) for prefix in invalid_prefixes)
        and any(key in tag for key in valid_keywords)
    }

    if candidate_rowtags:
        best_tag = max(candidate_rowtags, key=candidate_rowtags.get)
        print(f"Voorgestelde rowTag: {best_tag}")
    else:
        print("Geen logische rowTag gevonden — controleer eventueel handmatig.")

