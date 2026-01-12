# Inspecteer schema en voorbeelddata voor pand
print("Schema kadastralekaart_pand.gml:")
df_pand.printSchema()
print("Voorbeeld data kadastralekaart_pand.gml:")
df_pand.show(5, truncate=False)

# Inspecteer schema en voorbeelddata voor perceel
print("Schema kadastralekaart_perceel.gml:")
df_perceel.printSchema()
print("Voorbeeld data kadastralekaart_perceel.gml:")
df_perceel.show(5, truncate=False)
