df_flat = df_perceel.selectExpr(
    "oz:`Perceel`.`_gml:id` as perceel_id",
    "oz:`Perceel`.`ko:identificatie`.`_VALUE` as identificatie_id",
    "oz:`Perceel`.`ko:identificatie`.`_domein` as identificatie_domein",
    "oz:`Perceel`.`oz:begrenzingPerceel`.`gml:Polygon`.`_gml:id` as geometry_id",
    "oz:`Perceel`.`oz:begrenzingPerceel`.`gml:Polygon`.`gml:exterior`.`gml:LinearRing`.`gml:posList` as geometry_poslist",
    "oz:`Perceel`.`oz:historie`.`h:beginGeldigheid` as geldigheid",
    "oz:`Perceel`.`oz:historie`.`h:statusHistorie`.`h:StatusHistorie`.`t:code` as status_code",
    "oz:`Perceel`.`oz:historie`.`h:statusHistorie`.`h:StatusHistorie`.`t:waarde` as status_waarde",
    "oz:`Perceel`.`oz:kadastraleAanduiding`.`oz:TypeKadastraleAanduiding`.`oz:perceelnummer` as perceelnummer",
    "oz:`Perceel`.`oz:kadastraleAanduiding`.`oz:TypeKadastraleAanduiding`.`oz:sectie` as sectie"
    # add more if needed
)

# Sla de DataFrame op als Delta-tabel
df_perceel.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dpt_test_bronze.bron.kadastraal_data_delta")
print("Data succesvol opgeslagen als Delta-tabel.")
