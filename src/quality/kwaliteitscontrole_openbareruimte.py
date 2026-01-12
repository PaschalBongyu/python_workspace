from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col

# Verwachte kolommen na flattening
expected_columns = [
    "label_id",
    "creation_date",
    "publicatiedatum",
    "bgt_status",
    "bronhouder",
    "lokaalID",
    "namespace",
    "bag_opr",
    "naam",
    "type",
    "plus_status",
    "hoogteligging",
    "tijdstip_registratie",
    "positie",
    "hoek",
    "coord_str",
    "coords"
]

print("=== Validatie voor label_df_flat ===\n")

# Controle op ontbrekende kolommen
missing_cols = [c for c in expected_columns if c not in label_with_posities.columns]
if missing_cols:
    print(f"Ontbrekende kolommen: {missing_cols}")
else:
    print("Alle verwachte kolommen zijn aanwezig.")

# Controle op nested kolommen (er mogen eigenlijk geen StructType of ArrayType zijn)
nested_cols = [f.name for f in label_with_posities.schema.fields if isinstance(f.dataType, (StructType, ArrayType))]
if nested_cols:
    print(f"Nested kolommen gevonden: {nested_cols}")
else:
    print("Geen nested kolommen gevonden.")

# Null-waarden tellen in belangrijke kolommen (voorbeeld: naam, type, coords)
for col_name in ["naam", "type", "coords"]:
    null_count = label_with_posities.filter(col(col_name).isNull()).count()
    total_count = label_with_posities.count()
    print(f"Null waarden in '{col_name}': {null_count} van {total_count} rijen.")

# Voorbeeld data tonen
print("\nVoorbeeld data:")
label_with_posities.select(expected_columns).show(5, truncate=False)

