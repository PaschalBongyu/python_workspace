%sh
# Step 1: Download ZIP directly to DBFS
wget -O /dbfs/mnt/rawdata/kadastralekaart-gml-nl-nohist.zip \
"https://api.pdok.nl/kadaster/kadastralekaart/download/v5_0/full/predefined/kadastralekaart-gml-nl-nohist.zip"
