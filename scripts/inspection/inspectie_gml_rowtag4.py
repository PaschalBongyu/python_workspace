import os
import re
from collections import Counter

gml_folder = "/dbfs/mnt/processeddata/unzipped_files"
gml_files = [f for f in os.listdir(gml_folder) if f.endswith(".gml")]

def find_tags(lines):
    pattern = re.compile(r'<([a-zA-Z0-9_:]+)')
    tags = []
    for line in lines:
        tags.extend(pattern.findall(line))
    return Counter(tags)

invalid_prefixes = [
    't:', 'gml:pos', 'gml:Point', 'ko:', 'imgeo:', 'h:', 'oz:', 'kad:', 'creationDate'
]

valid_keywords = ['Member', 'featureMember', 'Object', 'cityObjectMember']

for file in gml_files:
    path = os.path.join(gml_folder, file)

    with open(path, 'r', encoding='utf-8') as f:
        snippet = []
        for _ in range(1000):
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

    # Kies alleen rowTags uit top 5 tags
    candidate_rowtags = {
        tag: count for tag, count in top_tags.items()
        if (not any(tag.startswith(prefix) for prefix in invalid_prefixes))
        and any(key in tag for key in valid_keywords)
    }

    if candidate_rowtags:
        best_tag = max(candidate_rowtags, key=candidate_rowtags.get)
        print(f"Voorgestelde rowTag: {best_tag}")
    else:
        print("Geen logische rowTag gevonden — controleer eventueel handmatig.")