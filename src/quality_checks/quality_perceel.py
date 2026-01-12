from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col

def controleer_df(df, verwachte_kolommen, naam="DataFrame"):
    print(f"\n=== Controle voor {naam} ===")

    # 1. Ontbrekende kolommen checken
    ontbrekend = [c for c in verwachte_kolommen if c not in df.columns]
    if ontbrekend:
        print(f"Ontbrekende kolommen: {ontbrekend}")
    else:
        print("Alle verwachte kolommen zijn aanwezig.")

    # 2. Nested kolommen checken
    nested = [f.name for f in df.schema.fields if isinstance(f.dataType, (StructType, ArrayType))]
    if nested:
        print(f"Nested kolommen gevonden: {nested}")
    else:
        print("Geen nested kolommen gevonden.")

    # 3. Null waardes per kolom tellen
    print("\nAantal null waardes per kolom:")
    for c in verwachte_kolommen:
        null_count = df.filter(col(c).isNull()).count()
        print(f" - {c}: {null_count}")

    # 4. Samenvatting tonen
    print("\nSamenvatting statistieken:")
    df.select(verwachte_kolommen).summary("count", "min", "max").show(truncate=False)


# Voorbeeld gebruik
verwachte_kolommen_flat = [
    "perceel_id", "identificatie_waarde", "identificatie_domein",
    "begin_geldigheid", "tijdstip_registratie", "status_historie",
    "gemeente_code", "sectie", "perceelnummer", "oppervlakte",
    "rotatie", "deltaX", "deltaY", "coords_punt", "exterior_coords"
]

verwachte_kolommen_met_holes = verwachte_kolommen_flat + ["interior_ring", "interior_coords"]

controleer_df(perceel_df_flat, verwachte_kolommen_flat, "perceel_df_flat")
controleer_df(perceel_df_with_holes, verwachte_kolommen_met_holes, "perceel_df_with_holes")
