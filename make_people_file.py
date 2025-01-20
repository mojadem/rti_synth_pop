import sys
import pandas as pd

file = sys.argv[1]

df = pd.read_csv(file)

# drop unnamed index column since one is created by default
df = df.drop(columns=["Unnamed: 0"])

# drop other unneeded columns
df = df.drop(columns=["serialno", "sporder"])

# rename colums to people file column names
df = df.rename(
    columns={"hh_id": "sp_hh_id", "rac1p": "race", "agep": "age", "relshipp": "relate"}
)

# update index column name
df = df.rename_axis("sp_id")


# this is the mapping of the 'relshipp' PUMS var to the expected values
# in the 'relate' column according to the following mapping
# - 20 (reference person) -> 0 (householder)
# - 21 (opposite sex spouse) -> 1 (spouse)
# - 22 (opposite sex partner) -> 10 (partner)
# - 23 (same sex spouse) -> 1 (spouse)
# - 24 (same sex partner) -> 10 (partner)
# - 25 (biologial child) -> 2 (child)
# - 26 (adopted child) -> 2 (child)
# - 27 (step child) -> 2 (child)
# - 28 (sibling) -> 3 (sibling)
# - 29 (parent) -> 4 (parent)
# - 30 (grandchild) -> 5 (grandchild)
# - 31 (parent in law) -> 6 (in law)
# - 32 (child in law) -> 6 (in law)
# - 33 (other relative) -> 7 (other relative)
# - 34 (roomate or housemate) -> 9 (housemate)
# - 35 (foster child) -> 11 (foster child)
# - 36 (other nonrelative)-> 12 (other nonrelative)
# - 37 (institutionalized gq pop)-> 13 (institutionalized gq pop)
# - 38 (non-institutionalized gq pop)-> 14 (non-institutionalized gq pop)
relshipp_to_relate_mapping = {
    20: 0,
    21: 1,
    22: 10,
    23: 1,
    24: 10,
    25: 2,
    26: 2,
    27: 2,
    28: 3,
    29: 4,
    30: 5,
    31: 6,
    32: 6,
    33: 7,
    34: 9,
    35: 11,
    36: 12,
    37: 13,
    38: 14,
}

df["relate"] = df.apply(lambda row: relshipp_to_relate_mapping[row["relate"]], axis=1)

# for now, assign X to school_id and work_id columns
df["school_id"] = "X"
df["work_id"] = "X"

# reorder the columns to match people.txt column order
df = df.reindex(
    columns=["sp_hh_id", "age", "sex", "race", "relate", "school_id", "work_id"]
)

print(df)

# save dataframe as tab separated csv
df.to_csv("people.txt", sep="\t")
