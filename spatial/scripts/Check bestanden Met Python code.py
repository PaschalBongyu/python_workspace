import os  # Om met mappen en bestanden te werken

pad = "/dbfs/mnt/rawdata"  # Waar we zoeken naar bestanden
bestanden = []  # Lijst voor gevonden bestanden

# Loop door alle mappen en bestanden in 'pad'
for root, dirs, files in os.walk(pad):
    # Loop door de bestanden in de huidige map
    for file in files:
        # Volledige bestandsnaam met pad maken
        volledige_pad = os.path.join(root, file)
        bestanden.append(volledige_pad)  # Bestand toevoegen aan lijst

# Laat alle gevonden bestanden zien
print("Gevonden bestanden:")
for bestand in bestanden:
    print(bestand)
