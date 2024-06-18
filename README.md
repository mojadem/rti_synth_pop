# rti_synth_pop Codebase: Quick Start Guide (README)

June, 2024

Prepared by
RTI International
3040 Cornwallis Road
Research Triangle Park, NC 27709

# Overview and Introduction
RTI has developed this code for creation of synthetic populations from the U.S. Census TIGER and American Community Survey (ACS) data with geospatial placement leveraging ORNL's LandScan USA data product. The resulting synthetic populations provide detailed and spatially explicit representation of the socio-demographic distribution of the U.S. population suitable for geospatial analyses and modeling use cases including microsimulation, individual and agent-based modeling (IBM/ABM).

## Project Organization

    
    ├── data
    │   ├── external        <- Data from third party sources.
    │   ├── interim         <- Intermediate data that has been transformed.
    │   ├── processed       <- The final, canonical data sets for modeling.
    │   └── raw             <- The original, immutable data.
    └── rti_synth_pop       <- Source code for use in this project.
    │   ├── __init__.py     <- Makes rti_synth_pop a Python module.
    │   ├── config.py       <- Configuration file.
    │   ├── sample_pums.py  <- PUMS sampling.
    │   ├── task_*          <- task files with task functions to be executed on the command
    │                          line by invoking 'pytask'
    ├── environment.yml     <- The requirements file for reproducing the analysis environment.
    ├── .here               <- Empty file that will stop the search if none of the other criteria
    │                          apply when searching head of project.
    ├── setup.py            <- Makes project pip installable (pip install -e .)
    │                          so synth_pop can be imported.
    │── .gitignore          <- standard gitignore
    ├── LICENSE             <- License information
    ├── CITATION.cff        <- Citation metadata
    ├── README.md           <- The top-level README for developers using this project.
    


---
Project based on the [cookiecutter conda data science project template](https://github.com/jvelezmagic/cookiecutter-conda-data-science).

# Citing this Code
The correct citation for this code is:

Kruskamp, N., Kery, C., & Rineer, J. rti_synth_pop [Computer software].[https://github.com/RTIInternational/rti_synth_pop](https://github.com/RTIInternational/rti_synth_pop)

# License
See the [LICENSE](LICENSE) file at the root of this repo. This code is licensed under:

Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International Public License

CC BY-NC-SA 4.0

# Installation, Configuration, Execution and Outputs
## Installation
### Prerequisites

- [Conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/download.html)
- Optional [Mamba](https://mamba.readthedocs.io/en/latest/)

### Create environment

```bash
conda env create -f environment.yml
activate rti_synth_pop
```

or 

```bash
mamba env create -f environment.yml
activate rti_synth_pop
```
The packages necessary to run the project are now installed inside the conda environment.

**Note: The following sections assume you are located in your conda environment.**

### Set up project's module

Currently, the installation of the `rti_synth_pop` module is included in the environment file. If you need to re/install it manually, please follow these instructions:

To move beyond notebook prototyping, all reusable code should go into the `rti_synth_pop/` folder package. To use that package inside your project, install the project's module in editable mode, so you can edit files in the `rti_synth_pop` folder and use the modules inside your notebooks :

```bash
pip install --editable .
```

To use the module inside your notebooks, add `%autoreload` at the top of your notebook :

```python
%load_ext autoreload
%autoreload 2
```

Example of module usage :

```python
from rti_synth_pop.utils.paths import data_dir
data_dir()
```

## Configuration
The original `config.py` file from this repo is set to run the state of Wyoming (WY) for 2021. To change this in the config you can specify a list of tuples containing the state abbreviations and FIPS GEOIDs for the states you would like to
create synthetic populations for. See lines 19 and 21 in `config.py` and follow this format `[("st_abbr1", "st_fips1"), ("st_abbr2", "st_fips2")]`. Pytask will generate a synthetic population for each state in the list.

A data directory is included to store the inputs, outputs and intermediate files. This folder structure can be found and customized as needed in the `config.py` file.
```
data/raw          <- raw data from ACS sources, TIGER, and LandScan are stored here
data/interim      <- intermediate files created during the process are put here
data/processed    <- final synthetic population files (a person and household file
                     for each state and year) are created here.
```
**You must manually download the LandScan population density data from [ORNL's LandScan Website](https://landscan.ornl.gov/). All other data should be downloaded automatically for you. The LandScan tif should be placed into the `/data/raw` folder and must be renamed to `popdensity.tif`. By example the `config.py` file is initially setup for WY and for 2021 as an example, thus for WY 2021 you will need to download from LandScan the LandScan USA, 2021, Night, zip file, extract it's contents and copy or move the resulting `landscan-usa-2021-conus-night.tif` file into the `/data/raw` folder. You then must also rename the tif file to `popdensity.tif`. Note that LandScan USA is split into multiple files, and you must make sure the file download covers the states you are running the code for. As well other rasters that can be interpreted as a population probability surface might work but have not been tested.**

## Execution
This project uses [pytask](https://pytask-dev.readthedocs.io/en/stable/) to execute a series of tasks for creation of a synthetic populations. Once you have installed and activated the rti_synth_pop environment and are in root directory for of this cloned repo simply invoke:

`pytask` 

on the command line to execute all tasks in order for each state and year in config.py.

To execute a specific task, use this syntax:

`pytask -k <task file>::<task function name>` (ex: `pytask -k task_1_download_census_data.py::task_download_census_data`).

Each task depends on specific input files being created from previous tasks, which are defined in the `_create_parametrization` call above each task. Outputs to a task are indicated in the function definition. They are marked as `Annotated[Path, Product]` to show they are products of this task. If all output files already exist for a task, the task will not rerun.

## Outputs
The synthetic populations are output in tables and records of households and persons for each state and year.

Output files are in the format of:

`{st_abbr}_{year}_{households/persons}.parquet`. 

A households geospatial file will be created with a geometry point column in the format of:

`{st_abbr}_{year}_{households/persons}_w_geom.parquet`

### Spatial Data Reference
All spatial data are in EPSG:4326. Latitude and longitude columns also have the EPSG number in the column name as a reminder.

# Additional Notes
## Demographic Data

Synthetic population data are generated from tables published by the U.S. Census Bureau in the American Community Survey. ACS 5-year data are used to generate the synthetic population. For a complete list of tables from the ACS, please see the config.py file where the exact tables and columns are provided.

## Geospatial Data

To distribute household point locations in our synthetic population, we use the LandScan USA 2020 population density data under the [CC BY 4.0](https://landscan.ornl.gov/licensing) licensing with citation (below). These data must be downloaded manually and put into the raw data folder. [Please see the LandScan data portal to download the data.](https://landscan.ornl.gov/)

Weber, E., Moehl, J., Weston, S., Rose, A., & Sims, K. (2021). LandScan USA 2020 [Data set]. Oak Ridge National Laboratory. https://doi.org/10.48690/1523373

Census TIGER data are also used.

## Sampling Expansion Logic

When sampling households from the PUMS to fill our synthetic population a series of expanding criteria are applied.

1. Sample from within the same PUMA, look for matching on all variables
2. Sample from the entire state (with records weighted by how similar their PUMA is to the source PUMA). Look for matching on all variables
3. Sample from within same PUMA. Look for households with all variables matching except for household size, where we allow +/- one difference in size, or +/- 2 if the size is the minimum or maximum.
4. Same as 3 but sampling from the entire state with weights.
5. Sample from within same PUMA but in addition to household size we allow for the age of the head of householder to be off by +/- one group.
6. Same as 5 but sampling from the entire state with weights.
7. Sample from the same PUMA but on top of loosening restrictions for household size and head of householder age, we also loosen the criteria for household income to allow households with +/- one income group difference.
8. Same as 7 but sample from the entire state with weights.
9. Sample from within same PUMA but now allow head of householder ethnicity to be anything on top of having no restrictions for ethnicity and looser restrictions for household size, income, and age.
10. Same as 9 but sample from the entire state with weights.
11. Sample from within same PUMA but now also allow the household to have any income on top of the earlier expanded criteria.
12. Same as 11 but sample from the entire state with weights.

NOTE: most households are found in steps 1 and 2, with just a small percentage requiring further steps. 

