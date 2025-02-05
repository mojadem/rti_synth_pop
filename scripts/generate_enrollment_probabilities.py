import requests
import pandas as pd
import numpy as np
import sys

import requests_cache

requests_cache.install_cache()

STATE_FIPS = 56  # WYOMING
YEAR = 2019

# we assign enrollment to all school-aged children per household using a simple
# proportion of public/private/home school enrollees to total population
#
# NOTE: in this case, we are using ACS variables representing people who are
# school-aged and not enrolled in school to reflect people who are home-schooled
#
# school aged in this case is ages 3-17
#
# TODO: split the 18-19 age bucket into half school aged half not


# these columns represent school-aged children enrolled in public schools
PUBLIC_SCHOOL_COLUMNS_SCHOOL_AGED = [
    "B14003_004E",  # male ages 3-4
    "B14003_005E",  # male ages 5-9
    "B14003_006E",  # male ages 10-14
    "B14003_007E",  # male ages 15-17
    "B14003_032E",  # female ages 3-4
    "B14003_033E",  # female ages 5-9
    "B14003_034E",  # female ages 10-14
    "B14003_035E",  # female ages 15-17
]

# these columns represent non-school-aged people enrolled in public schools
PUBLIC_SCHOOL_COLUMNS_NON_SCHOOL_AGED = [
    "B14003_008E",  # male ages 18-19
    "B14003_009E",  # male ages 20-24
    "B14003_010E",  # male ages 25-34
    "B14003_011E",  # male ages 35+
    "B14003_036E",  # female ages 18-19
    "B14003_037E",  # female ages 20-24
    "B14003_038E",  # female ages 25-34
    "B14003_039E",  # female ages 35+
]

# these columns represent school-aged children enrolled in private schools
PRIVATE_SCHOOL_COLUMNS_SCHOOL_AGED = [
    "B14003_013E",  # male ages 3-4
    "B14003_014E",  # male ages 5-9
    "B14003_015E",  # male ages 10-14
    "B14003_016E",  # male ages 15-17
    "B14003_041E",  # female ages 3-4
    "B14003_042E",  # female ages 5-9
    "B14003_043E",  # female ages 10-14
    "B14003_044E",  # female ages 15-17
]

# these columns represent non-school-aged people enrolled in private schools
PRIVATE_SCHOOL_COLUMNS_NON_SCHOOL_AGED = [
    "B14003_017E",  # male ages 18-19
    "B14003_018E",  # male ages 20-24
    "B14003_019E",  # male ages 25-34
    "B14003_020E",  # male ages 35+
    "B14003_045E",  # female ages 18-19
    "B14003_046E",  # female ages 20-24
    "B14003_047E",  # female ages 25-34
    "B14003_048E",  # female ages 35+
]

# these columns represent school-aged children not enrolled in either public
# or private schools
NOT_ENROLLED_COLUMNS_SCHOOL_AGED = [
    "B14003_022E",  # male ages 3-4
    "B14003_023E",  # male ages 5-9
    "B14003_024E",  # male ages 10-14
    "B14003_025E",  # male ages 15-17
    "B14003_050E",  # female ages 3-4
    "B14003_051E",  # female ages 5-9
    "B14003_052E",  # female ages 10-14
    "B14003_053E",  # female ages 15-17
]

# these columns represent non-school-aged people not enrolled in either public
# or private schools
NOT_ENROLLED_COLUMNS_NON_SCHOOL_AGED = [
    "B14003_026E",  # male ages 18-19
    "B14003_027E",  # male ages 20-24
    "B14003_028E",  # male ages 25-34
    "B14003_029E",  # male ages 35+
    "B14003_054E",  # female ages 18-19
    "B14003_055E",  # female ages 20-24
    "B14003_056E",  # female ages 25-34
    "B14003_057E",  # female ages 35+
]

# these colums represent total enrollment counts and are used for verification
TOTAL_ENROLLMENT_COLUMNS = [
    "B14001_002E",  # total enrolled in school (public and private)
    "B14001_010E",  # total not enrolled in school
]

API_VARS = (
    PUBLIC_SCHOOL_COLUMNS_SCHOOL_AGED
    + PUBLIC_SCHOOL_COLUMNS_NON_SCHOOL_AGED
    + PRIVATE_SCHOOL_COLUMNS_SCHOOL_AGED
    + PRIVATE_SCHOOL_COLUMNS_NON_SCHOOL_AGED
    + NOT_ENROLLED_COLUMNS_SCHOOL_AGED
    + NOT_ENROLLED_COLUMNS_NON_SCHOOL_AGED
    + TOTAL_ENROLLMENT_COLUMNS
)

# data is pulled per county
#
# we split this call in to to avoid api var limit of 50

res = requests.get(
    f"https://api.census.gov/data/{YEAR}/acs/acs5?get={','.join(API_VARS)}&for=county:*&in=state:{STATE_FIPS}"
)

if res.status_code != 200:
    print("request error")
    print(res.text)
    sys.exit()

data = res.json()
df = pd.DataFrame(data[1:], columns=data[0])

# assign data columns to int
df[API_VARS] = df[API_VARS].astype("int32")

# combine school-aged children enrolled in public schools
df["public_total"] = df[PUBLIC_SCHOOL_COLUMNS_SCHOOL_AGED].sum(axis=1)

# combine school-aged children enrolled in private schools
df["private_total"] = df[PRIVATE_SCHOOL_COLUMNS_SCHOOL_AGED].sum(axis=1)

# combine school-aged children not enrolled in public or private schools
df["home_total"] = df[NOT_ENROLLED_COLUMNS_SCHOOL_AGED].sum(axis=1)

# we verify these numbers using the TOTAL_ENROLLMENT_COLUMNS
#
# these represent total enrollment of public and private schools combined, and
# total of people not enrolled in either public or private schools
#
# this includes non-school-aged people, so we subtract the those totals

df = df.rename(columns={"B14001_002E": "enrolled_total"})
df = df.rename(columns={"B14001_010E": "not_enrolled_total"})

df["enrolled_non_school_aged_total"] = df[
    PUBLIC_SCHOOL_COLUMNS_NON_SCHOOL_AGED + PRIVATE_SCHOOL_COLUMNS_NON_SCHOOL_AGED
].sum(axis=1)

df["not_enrolled_non_school_aged_total"] = df[NOT_ENROLLED_COLUMNS_NON_SCHOOL_AGED].sum(
    axis=1
)

df["public_and_private_total_test"] = (
    df["enrolled_total"] - df["enrolled_non_school_aged_total"]
)
df["home_total_test"] = (
    df["not_enrolled_total"] - df["not_enrolled_non_school_aged_total"]
)

df["public_and_private_total"] = df["public_total"] + df["private_total"]

assert df["public_and_private_total"].equals(df["public_and_private_total_test"])
assert df["home_total"].equals(df["home_total_test"])

# we need the total number of school-aged children for generating probabilities
df["school_aged_total"] = df["public_and_private_total"] + df["home_total"]

# now we generate the probability a school-aged child is either public, private,
# or home-schooled
df["prob_public"] = df["public_total"] / df["school_aged_total"]
df["prob_private"] = df["private_total"] / df["school_aged_total"]
df["prob_home"] = df["home_total"] / df["school_aged_total"]

# assert these add back up to 1
df["prob_sum"] = df[["prob_public", "prob_private", "prob_home"]].sum(axis=1)

assert np.isclose(df["prob_sum"], 1).all()

# combine state and county columns into county_fips
df["county_fips"] = df["state"] + df["county"]
df = df.sort_values(by="county_fips", ascending=True)

# save relevant columns
df[["county_fips", "prob_public", "prob_private", "prob_home"]].to_csv(
    f"{STATE_FIPS}_enrollment_probabilities.csv", index=False
)
