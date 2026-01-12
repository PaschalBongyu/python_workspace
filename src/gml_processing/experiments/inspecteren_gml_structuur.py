file_path = "/dbfs/mnt/processeddata/unzipped_files/kadastralekaart_openbareruimtelabel.gml"

with open(file_path, 'r', encoding='utf-8') as f:
    for _ in range(50):
        print(f.readline().strip())

