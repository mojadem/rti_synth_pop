import sys
import random
import pandas as pd
import numpy as np

random.seed(123)

# synth pop persons csv file
person_file = sys.argv[1]

# synth pop households csv file
household_file = sys.argv[2]

# generated enrollment probability file
enrollment_file = sys.argv[3]

person_df = pd.read_csv(person_file, index_col=0)

household_df = pd.read_csv(household_file)
household_df = household_df.set_index("hh_id")

enrollment_df = pd.read_csv(enrollment_file)
enrollment_df = enrollment_df.set_index("county_fips")


PUBLIC = 0
PRIVATE = 1
HOME = 2
NON_SCHOOL_AGED = 3


# enrollment status is assigned per household, as it is more common for siblings
# to share enrollment status
def assign_enrollment_to_households(household: pd.Series):
    enrollment_probabilities = enrollment_df.loc[household["county_fips"]]
    enrollment = np.random.choice([PUBLIC, PRIVATE, HOME], p=enrollment_probabilities)

    return enrollment


household_df["enrollment"] = household_df.apply(assign_enrollment_to_households, axis=1)


# next, we propagate shool-aged children's enrollment status from their household
def assign_enrollment_to_people(person: pd.Series):
    if person["agep"] < 3 or person["agep"] > 17:
        return NON_SCHOOL_AGED

    household = household_df.loc[person["hh_id"]]
    return household["enrollment"]


person_df["enrollment"] = person_df.apply(assign_enrollment_to_people, axis=1)

# resave persons file with enrollment column
person_df.to_csv(f"{person_file.removesuffix('.csv')}_w_enrollment.csv")
