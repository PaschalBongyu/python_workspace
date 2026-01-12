from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col

# Verwachte kolommen in geom_df
expected_columns_geom = ["pand_id", "posList", "wkt_polygon"]

print("=== Validation for geom_df ===\n")

# Check of alle verwachte kolommen aanwezig zijn
missing_geom = [c for c in expected_columns_geom if c not in geom_df.columns]
if missing_geom:
    print(f"Ontbrekende kolommen: {missing_geom}")
else:
    print("Alle verwachte kolommen zijn aanwezig.")

# Check op nested kolommen (posList hoort een string te zijn, dus geen Struct/Array)
nested_cols_geom = [f.name for f in geom_df.schema.fields if isinstance(f.dataType, (StructType, ArrayType))]
if nested_cols_geom:
    print(f"Nested kolommen gevonden: {nested_cols_geom}")
else:
    print("Geen nested kolommen gevonden.")

# Toon een paar voorbeeldrijen met de belangrijke kolommen
print("\nVoorbeeld data:")
geom_df.select(expected_columns_geom).show(5, truncate=False)

# Tel het aantal null waarden in wkt_polygon (kan wijzen op parse problemen)
null_wkt_count = geom_df.filter(col("wkt_polygon").isNull()).count()
total_count = geom_df.count()
print(f"\nAantal null WKT polygons: {null_wkt_count} van de {total_count} rijen.")
