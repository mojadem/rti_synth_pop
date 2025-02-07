import sys
from numpy import random
import pandas as pd

file = sys.argv[1]

df = pd.read_csv(file)

households_file_columns = [
    "sp_id",
    "stcotrbg",
    "race",
    "hh_income",
    "latitude",
    "longitude",
    "elevation",
]

column_mappings = {
    "hh_id": "sp_id",
    "GEOID": "stcotrbg",
    "hh_race": "race",
    "hh_income": "hh_income",
    "lat_4326": "latitude",
    "lon_4326": "longitude",
}

df = df.rename(columns=column_mappings)
df = df.drop(columns=[col for col in df.columns if col not in households_file_columns])

# remap race column
#
# the population output has the following values for race:
# - 0: white
# - 1: black
# - 2: asian
# - 3: other
# - 4: two or more races
#
# the households.txt uses the following values for race:
# - WHITE = 1,
# - AFRICAN_AMERICAN = 2,
# - AMERICAN_INDIAN = 3,
# - ALASKA_NATIVE = 4,
# - TRIBAL = 5,
# - ASIAN = 6,
# - HAWAIIAN_NATIVE = 7,
# - OTHER_RACE = 8,
# - MULTIPLE_RACE = 9,
#
# note that 3 is simply mapped to 8, even though it could be 3, 4, 5, or 7
race_mapping = {
    0: 1,
    1: 2,
    2: 6,
    3: 8,
    4: 9,
}

df["race"] = df.apply(lambda row: race_mapping[row["race"]], axis=1)


# remap income column
#
# the population income has income values binned accordingly:
# - 0: <10k
# - 1: 10k-15k
# - 2: 15k-25k
# - 3: 25k-35k
# - 4: 35k-50k
# - 5: 50k-100k
# - 6: >100k
#
# the households.txt income column is discrete
# so, we need to uniformly sample values for our mapping
# in the case of bin 6, we will for now use a rough estimation function
# explained more in the documentation
def map_income(bin):
    match bin:
        case 0:
            return int(random.uniform(0, 10000))
        case 1:
            return int(random.uniform(10000, 15000))
        case 2:
            return int(random.uniform(15000, 25000))
        case 3:
            return int(random.uniform(25000, 35000))
        case 4:
            return int(random.uniform(35000, 50000))
        case 5:
            return int(random.uniform(50000, 100000))
        case 6:
            x = random.random()
            y = int(90 * 10000**x + 99910)
            assert y >= 100000 and y < 1000000
            return y


df["hh_income"] = df.apply(lambda row: map_income(row["hh_income"]), axis=1)

# initialize the elevation column to -1 as it should be present for consistency
# but it not actually used
df["elevation"] = -1

# reorder columns to match households.txt column order
df = df.reindex(columns=households_file_columns)

print(df)

# save dataframe as tsv
df.to_csv("households.txt", sep="\t", index=False)
