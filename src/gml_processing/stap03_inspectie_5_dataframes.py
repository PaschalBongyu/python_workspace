# Inspecteer schema en voorbeelddata voor pand
print("\nSchema kadastralekaart_pand.gml:")
df_pand.printSchema()

print("\nVoorbeeld data kadastralekaart_pand.gml:")
df_pand.show(5, truncate=False)

# Inspecteer schema en voorbeelddata voor perceel
print("\nSchema kadastralekaart_perceel.gml:")
df_perceel.printSchema()

print("\nVoorbeeld data kadastralekaart_perceel.gml:")
df_perceel.show(5, truncate=False)