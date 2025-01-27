import pandas as pd
import sys

files = sys.argv[1:]

for file in files:
    df = pd.read_parquet(file)
    df.to_csv(file.replace("parquet", "csv"))
