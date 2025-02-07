import pandas as pd
import sys
import re

# TODO: add private schools

file = sys.argv[1]

df = pd.read_csv(file)

# preprocess column names to remove [Public School] 2018-2019
for column in map(str, df.columns):
    if column.startswith("School Name") or column.startswith("State Name"):
        continue

    stripped = re.sub(" \[Public School\].*$", "", column)
    df = df.rename(columns={column: stripped})

schools_file_columns = ["sp_id", "stco", "latitude", "longitude", "elevation"]

column_mappings = {
    "School ID - NCES Assigned": "sp_id",
    "County Number": "stco",
    "Latitude": "latitude",
    "Longitude": "longitude",
}

df = df.rename(columns=column_mappings)
df = df.drop(columns=[col for col in df.columns if col not in schools_file_columns])

# initialize the elevation column to -1 as it should be present for consistency
# but it not actually used
df["elevation"] = -1

# reorder columns to match households.txt column order
df = df.reindex(columns=schools_file_columns)

print(df)

# save dataframe as tsv
df.to_csv("schools.txt", sep="\t", index=False)
