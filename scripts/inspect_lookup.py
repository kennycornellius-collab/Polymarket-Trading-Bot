import polars as pl
df = pl.read_parquet('data/bars/_market_lookup.parquet')
print(f'Lookup rows: {len(df)}')
print(f'Schema: {df.schema}')
print(f'Null created_at: {df["created_at"].null_count()}')
print(df.head())
