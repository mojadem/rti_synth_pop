import sys
import pandas as pd
import numpy as np
import re
from itertools import product

# persons_w_enrollment.csv
p_file = sys.argv[1]

# households_w_geom.csv
hh_file = sys.argv[2]

# public_schools.csv
ps_file = sys.argv[3]

# TODO: private schools

p_df = pd.read_csv(p_file, index_col=0)

# this is what we want to initialize
p_df["school_id"] = "X"

hh_df = pd.read_csv(hh_file)
hh_df = hh_df.set_index("hh_id")

ps_df = pd.read_csv(ps_file)

# preprocess column names to remove "[Public School] 20xx-xx"
for column in map(str, ps_df.columns):
    if column.startswith("School Name") or column.startswith("State Name"):
        continue

    stripped = re.sub(" \[Public School\].*$", "", column)
    ps_df = ps_df.rename(columns={column: stripped})

# assign more useful column names
column_mappings = {
    "School ID (12-digit) - NCES Assigned": "sp_id",
    "County Number": "stco",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Prekindergarten offered": "prek",
    "Kindergarten offered": "k",
    "Grade 1 offered": "grade_1",
    "Grade 2 offered": "grade_2",
    "Grade 3 offered": "grade_3",
    "Grade 4 offered": "grade_4",
    "Grade 5 offered": "grade_5",
    "Grade 6 offered": "grade_6",
    "Grade 7 offered": "grade_7",
    "Grade 8 offered": "grade_8",
    "Grade 9 offered": "grade_9",
    "Grade 10 offered": "grade_10",
    "Grade 11 offered": "grade_11",
    "Grade 12 offered": "grade_12",
    "Total Students All Grades (Excludes AE)": "capacity",
}

ps_df = ps_df.rename(columns=column_mappings)
ps_df = ps_df.set_index("sp_id")

# change grade offering columns to bools
grade_offered_map = {"1-Yes": True, "2-No": False}
grade_offered_cols = [
    "prek",
    "k",
    "grade_1",
    "grade_2",
    "grade_3",
    "grade_4",
    "grade_5",
    "grade_6",
    "grade_7",
    "grade_8",
    "grade_9",
    "grade_10",
    "grade_11",
    "grade_12",
]

ps_df[grade_offered_cols] = ps_df[grade_offered_cols].map(
    lambda x: grade_offered_map[x]
)


# we must assign all households to schools that have at least one school aged child
#
# person enrollment = 3 if they are not school aged
def check_household_for_school_aged_children(household: pd.Series) -> bool:
    hh_persons = p_df[p_df["hh_id"] == household.name]
    return (hh_persons["enrollment"] != 3).any()


# TODO: delete after testing
# hh_df = hh_df.head(1000)

hh_df["needs_assignment"] = hh_df.apply(
    check_household_for_school_aged_children, axis=1
)
hh_df = hh_df[hh_df["needs_assignment"]]
hh_df["assigned"] = False

# rename columns for consistency in next step
hh_df = hh_df.rename(
    columns={"lat_4326": "latitude", "lon_4326": "longitude", "county_fips": "stco"}
)


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two points on Earth using the Haversine formula.

    Parameters:
    lat1, lon1: Latitude and longitude of the first point in degrees.
    lat2, lon2: Latitude and longitude of the second point in degrees.

    Returns:
    Distance in kilometers.
    """
    R = 3956  # Radius of Earth in miles

    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))

    distance = R * c
    return distance


counties = hh_df["stco"].unique()
# TODO: uncomment after testing
# assert counties == hh_df["county_fips"].unique()


def distance_between_hh_and_ps(hh_id: int, ps_id: int):
    hh = hh_df.loc[hh_id]
    ps = ps_df.loc[ps_id]
    return haversine(hh["latitude"], hh["longitude"], ps["latitude"], ps["longitude"])


for county in counties:
    print(f"county {county}")
    # get households and public schools in county
    hh_ids = hh_df[hh_df["stco"] == county].index.values
    ps_ids = ps_df[ps_df["stco"] == county].index.values

    print(f"found {len(hh_ids)} households and {len(ps_ids)} public schools")

    # find the distance between every combination of household and school
    hh_ps_df = pd.DataFrame(list(product(hh_ids, ps_ids)), columns=["hh_id", "ps_id"])
    hh_ps_df["distance"] = hh_ps_df.apply(
        lambda row: distance_between_hh_and_ps(row["hh_id"], row["ps_id"]), axis=1
    )
    hh_ps_df = hh_ps_df.sort_values(by="distance", ascending=True)

    # create an enrollment map for each school
    enrollment = {}
    for ps_id in ps_ids:
        enrollment[ps_id] = 0

    for edge in hh_ps_df.itertuples():
        ps = ps_df.loc[edge.ps_id]
        if enrollment[edge.ps_id] > ps["capacity"]:
            continue

        hh = hh_df.loc[edge.hh_id]
        if hh["assigned"]:
            continue

        hh_df.loc[edge.hh_id, "assigned"] = True

        hh_p_df = p_df[p_df["hh_id"] == edge.hh_id]

        # children enrolled in public schools have enrollment 0
        hh_p_df = hh_p_df[hh_p_df["enrollment"] == 0]

        # assign school and update enrollment
        p_df.loc[hh_p_df.index, "school_id"] = edge.ps_id
        enrollment[edge.ps_id] += len(hh_p_df)

    # assign households that didn't get assigned
    hh_leftover_df = hh_df[hh_df["stco"] == county & ~hh_df["assigned"]]
    print(f"{len(hh_leftover_df)} households leftover")
    if len(hh_leftover_df) > 0:
        # for now, just do a second pass over edges ignoring school capacity
        for edge in hh_ps_df.itertuples():
            hh = hh_df.loc[edge.hh_id]
            if hh["assigned"]:
                continue

            hh_df.loc[edge.hh_id, "assigned"] = True

            hh_p_df = p_df[p_df["hh_id"] == edge.hh_id]

            # children enrolled in public schools have enrollment 0
            hh_p_df = hh_p_df[hh_p_df["enrollment"] == 0]

            # assign school and update enrollment
            p_df.loc[hh_p_df.index, "school_id"] = edge.ps_id
            enrollment[edge.ps_id] += len(hh_p_df)

# assign households that didn't get assigned
hh_leftover_df = hh_df[~hh_df["assigned"]]
print(f"{len(hh_leftover_df)} households at end leftover")
for hh in hh_leftover_df.iterrows():
    print(hh)

print(hh_leftover_df)


print(p_df)

# we should probably do some sanity check between school aged children totals and
# public school enrollment totals

p_df.to_csv(f"{p_file.removesuffix('w_enrollment.csv')}w_school_assignment.csv")
# # this will store all combinations
# #
# #
# ps_hh_df =

# for id in ps_ids:
#     school = ps_df.loc[id]
#     ps_hh = pd.DataFrame()
#     ps_hh["distance"] = hh_df.apply(
#         lambda hh: haversine(
#             school["latitude"], school["longitude"], hh["latitude"], hh["longitude"]
#         ),
#         axis=1,
#     )
#     ps_hh_map[id] = ps_hh
