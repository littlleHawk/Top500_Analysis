###################################################
#
#              DATAFRAME BUILDERS
#
###################################################
"""
DATAFRAME BUILDERS Module
=========================

Builds integrated DataFrames combining HPC system data, chip specs, and
electricity carbon intensity for sustainability analysis.

Main Functions:
---------------

**HPC & Chip:**
- `build_computer_info`: Merges Top500 and Green500 data over a year/month
                         range; adds fields like `HashedID`, `perf_percent`.
- `build_chip_info`: Links CPU/GPU names to TDP datasets to estimate total
                     and idle power use.

**Electricity Impact:**
- `build_electricity_impact_info`: Joins Ember and Electricity Maps data;
                                computes absolute carbon intensity differences.

**Full Merge:**
- `connect_cores_computers_electricity`: Combines cores, systems, and 
                                    electricity data for region-based analysis.

**Fuzzy Matching** 

Data Paths:
-----------
- CPU/GPU Specs:     ./data/CPU_TDP_data.csv, ./data/GPU_TDP_data.csv  
- Electricity Maps:  ./data/ElectricityMaps_coutry_abbr_list.csv  
- Ember Data:        ./data/monthly_full_release_long_format.csv  
- Output:            ./data/output_data/

Dependencies:
-------------
- `project_lib` (aliases for pandas, numpy, etc.)
- `utilities` (helper functions for data filtering and transformation)
"""



import project_lib as m
import utilities as u

m.pd.set_option('future.no_silent_downcasting', True)

############################################################################
#
# Functions to build datasets
#
############################################################################

# computer info
    # formerly all_df
def buildComputerInfo(year_range:tuple = (2012, 2025), 
                      year_range_green:tuple = (2014, 2025), 
                      save:bool = False,
                      verbose:bool = m.VERBOSE_VAL):
    """
    Build the computer info dataframe from the top500 and green500 data.
    Args:
        year_range (tuple): The range of years to include in the dataframe.
        year_range_green (tuple): The range of years to include in the green500 
                                  dataframe.
        save (bool): If true, saves the dataframe to a csv file.
        verbose(bool): If true, prints progress messages
    Returns:
        m.pd.DataFrame: The concatenated dataframe of top500 and green500 data.
    Dependencies:
        read_filter_top500_data
        read_filter_green500_data
        concat_all_df
        reason_of_leaving_list
    """

    # set fields of interest.
    # shared_fields = ['Computer', 'Year', 'Country', 'Cores', 'Rank','Name',
    #                   'Site', 'RMax', 'RPeak', 'HashedID', 
    #                   'rank_year', 'RPeak-RMax', 
    #                   'Accelerator/Co-Processor', 'Listing']
    main_green_fields = ['Computer', 'Year', 'Country', 'Cores', 'Name', 
                         'Site', 'Power', 'RMax', 'RPeak', 'TOP500 Rank', 
                         'Rank', 'Accelerator/Co-Processor', 'G_eff', 
                         'HashedID', 'rank_year', 'RPeak-RMax', 'Listing', 
                         'Accelerator/Co-Processor Cores']

    top500_fields = ['Rank','Site','Manufacturer','Computer','Country',
                     'Year','RMax', 'RPeak','Nmax','Nhalf', 'Processor',
                     'System Family', 'Operating System','Architecture',
                     'Segment','Interconnect Family', 'Interconnect',
                     'Continent','Cores','perf_percent','rank_year',
                     'Listing', 'Name', 'HashedID', 'RPeak-RMax',
                     'Accelerator/Co-Processor', 
                     'Accelerator/Co-Processor Cores']


    if verbose :
        print("Building computer info dataframe")
    # concatenate all the extracted dataframes into 
    # one single one from the year 1993 to 2023.
    concat_df = m.pd.concat(
            [u.read_filter_top500_data(y, m, 
                                       range=500, 
                                       fields=top500_fields) 
            for y in range(year_range[0], year_range[1]) 
            for m in ['06', '11']],
            ignore_index=True
        )


    green_concat_df = m.pd.concat(
            [u.read_filter_green500_data(y, m, 
                                         range=500, 
                                         fields=main_green_fields) 
            for y in range(year_range_green[0], year_range_green[1]) 
            for m in ['06', '11']],
            ignore_index=True
        )

    # some info about the concatenated dataframe
    dfs = [concat_df, green_concat_df]

    all_df = u.concat_all_df(dfs)

    all_df = u.reason_of_leaving_list(all_df, maxYear=2024.5)  
    all_df.rename(columns={'list_all_years_in_ranking': 'Rank Year List'}, 
                  inplace=True) 
    all_df['Segment'] = (all_df['Segment']
                        .apply(lambda x: "Unknown" if m.pd.isna(x) else x))

    if verbose :
        print(f"Computer info dataframe built with {len(all_df)} rows and\
              {len(all_df.columns)} columns.") 
    
    if save:
        all_df.to_csv('./data/output_data/all_df.csv', index=False)
    
    return all_df

# chip info
    #formerly cores_cleaned_df
def build_chip_info(save:bool = False, verbose:bool = m.VERBOSE_VAL):
    """
    Read CPU and GPU data, merges it with Computers dataframe 
    based on processor and calculates both annual 
    and hourly power consumption for both CPU and GPU.

    Args:
        save (bool): Whether to save the DataFrame to a CSV file.
        verbose (bool): If True, prints additional information
                             about the process.

    Returns:
        m.pd.DataFrame: The cleaned DataFrame containing chip information, 
        including CPU and GPU power consumption.
    
    Dependencies:
        CPU_TDP_data.csv, GPU_TDP_data.csv
    """
    if verbose :
        print("Building Chip info dataframe")

    # Read in CPU data to add to cores df
    cpu_df = (m.pd.read_csv('./data/CPU_TDP_data.csv')
              .drop(columns={'Unnamed: 9'}))
    gpu_df = m.pd.read_csv('./data/GPU_TDP_data.csv')

    computers_df = buildComputerInfo(save=False)
    cores_df = computers_df.copy(deep=True)

    cores_df['Processor lower'] = cores_df['Processor'].str.lower()
    cores_df['Accelerator/Co-Processor lower'] = (
        cores_df['Accelerator/Co-Processor']
        .str.lower()
        )

    cpu_df['Processor lower'] = cpu_df['Processor'].str.lower()
    gpu_df['GPU Model lower'] = gpu_df['GPU Model'].str.lower()

        # join chip data to computer
    cores_df = m.pd.merge(cores_df, cpu_df,
                          left_on='Processor lower',
                          right_on='Processor lower',
                          how='left')
    cores_df = m.pd.merge(cores_df, gpu_df, 
                          left_on='Accelerator/Co-Processor lower',
                          right_on='GPU Model lower',
                          how='left')
    
    cores_df.drop(columns=['Source',
                           'Accelerator/Co-Processor lower', 
                           'Cores_y','GPU Model lower',
                           'Processor lower', 'Processor_y'
                           ], inplace=True)
    cores_df.rename(columns={'Cores_x': 'Total Cores',
                             'Cores_y' : 'CPU Cores',
                             'Manufacturer_x':'Manufacturer',
                             'Manufacturer_y': 'Core_Manufacturer',
                             'TDP':'CPU_TDP',
                             'Rank_x': 'Rank',
                             'rank_year':'Rank Year',
                             'Model' : 'CPU Model',
                             'Processor_x' : 'Processor'
                             }, inplace=True)

    # make sure Green500 computers that are also
    # in Top500 will have CPU/GPU data
    cores_df['Processor'] = (cores_df
                             .groupby('HashedID')['Processor']
                            .transform(lambda x: x.ffill().bfill()))
    cores_df['Accelerator/Co-Processor'] = (cores_df
                            .groupby('HashedID')['Accelerator/Co-Processor']
                            .transform(lambda x: x.ffill().bfill()))
    cores_df = cores_df.fillna(0)

    # Get number of CPU Cores
    cores_df['CPU Cores'] = (cores_df['Total Cores'] - 
                                   cores_df['Accelerator/Co-Processor Cores'])

    if verbose :
        print(f"Chip info dataframe built with {len(cores_df)} rows and\
               {len(cores_df.columns)} columns.")

    if save:
        cores_df.to_csv('./data/output_data/cores_df.csv', index=False)

    return cores_df

# elec info
    # formerly elec_compare_df
def build_electricity_impact_info(save:bool = False, 
                                  verbose:bool=m.VERBOSE_VAL):
    """
    Build the electricity impact dataframe from Ember and 
        Electricity Maps data.
        Includes a column of the absolute difference between the two

    Args:
        save (bool): If true, saves the dataframe to a csv file.
        verbose (bool): If true, prints progress messages
    Returns:
        m.pd.DataFrame: The dataframe containing 
            combined data and comparison of
            Ember and Electricity Maps data.
    Dependencies:
        Reads:
            ./data/ElectricityMaps_coutry_abbr_list.csv
            ./data/monthly_full_release_long_format.csv
        read_filter_electricity_maps from utilities
    """
    if verbose :
        print("Building electricity info dataframe")

    # Read in all from electricity maps
    # read in list of countries with abreviations
    countries_codes_df = m.pd.read_csv(
        './data/ElectricityMaps_coutry_abbr_list.csv')
    countries_codes_df.fillna(0, inplace=True)

    electricity_maps_df = m.pd.concat([u.read_filter_electricity_maps(
                                        y, abbr, verbose=verbose)
                    for y in range(2021, 2025) 
                    for abbr in countries_codes_df['ZoneKey']],
                    ignore_index=True)
    
    electricity_maps_df['Datetime (UTC)'] = m.pd.to_datetime(
        electricity_maps_df['Datetime (UTC)'], format= 'mixed')
    electricity_maps_df['Year'] = (electricity_maps_df['Datetime (UTC)']
                                   .dt.year)
    electricity_maps_df['Month'] = (electricity_maps_df['Datetime (UTC)']
                                    .dt.month)

    # read in ember long data
    ember_df = m.pd.read_csv('./data/monthly_full_release_long_format.csv')

    ember_df['Date'] = m.pd.to_datetime(ember_df['Date'], format= 'mixed')
    ember_df['Year'] = ember_df['Date'].dt.year
    ember_df['Month'] = ember_df['Date'].dt.month

    ember_df = ember_df[ember_df['Category'] == 'Power sector emissions']
    ember_df = ember_df[ember_df['Unit'] == 'gCO2/kWh']
    ember_df = ember_df[ember_df['Area type'] == 'Country']
    ember_df.fillna(0, inplace=True)
    ember_df['Country'] = ember_df['Area']

    # Standardizing data
    # Ember side
    ember_df['Country'] = ember_df['Area'].str.strip()
    ember_df['Year'] = m.pd.to_datetime(ember_df['Date']).dt.year
    ember_df['Month'] = m.pd.to_datetime(ember_df['Date']).dt.month

    # Electricity-Maps side
    electricity_maps_df['Country'] = electricity_maps_df['Country'].str.strip()
    electricity_maps_df['Year'] = m.pd.to_datetime(
                                electricity_maps_df['Datetime (UTC)']).dt.year
    electricity_maps_df['Month'] = m.pd.to_datetime(
                                electricity_maps_df['Datetime (UTC)']).dt.month

    electricity_maps_df['Country'] = (electricity_maps_df['Country']
                        .str.replace('USA', 'United States of America'))
    electricity_maps_df['Country'] = (electricity_maps_df['Country']
                        .str.replace('Korea, South', 'South Korea'))
    ember_df['Country'] = (ember_df['Country']
                        .str.replace('Russian Federation (the)', 'Russia'))

    # join dfs on country name for comparison
    elec_compare_df = m.pd.merge(ember_df, electricity_maps_df,
                                 on=['Country','Month', 'Year'], how='outer')

    elec_compare_df['Electricity Maps Data Source'] = (
        elec_compare_df["Data Source"])
    elec_compare_df["Ember Carbon Intensity gCO₂eq/kWh"] = (
        elec_compare_df["Value"])

    elec_compare_df["Electricity Maps Carbon Intensity gCO₂eq/kWh"] = (
        elec_compare_df["Carbon Intensity gCO₂eq/kWh (Life cycle)"])
    elec_compare_df["Carbon Factor Difference (Electricity Maps - Ember)"] = (
                elec_compare_df["Electricity Maps Carbon Intensity gCO₂eq/kWh"] 
                - elec_compare_df["Ember Carbon Intensity gCO₂eq/kWh"])
    elec_compare_df.drop(columns=['Country code','Area type', 
                                  'Low Carbon Percentage',
                                  'Area', 'Zone Id', 'Data Source', 'Category', 
                                  'Subcategory', 'Variable','Datetime (UTC)'],
                                   inplace=True)
    if verbose :
        print(f"Electricity impact dataframe built with {len(elec_compare_df)}\
              rows and {len(elec_compare_df.columns)} columns.")

    if save:
        (elec_compare_df
         .to_csv('./data/output_data/ElectricityMaps_Ember_Comparison.csv'))
    
    return elec_compare_df


# Cores + computers + electricity
def connectCoresComputersElectricity(save:bool = False, 
                                     verbose:bool = m.VERBOSE_VAL):
    """
    Connect cores, computers, and electricity DataFrames by merging on 
    HashedID and Rank Year.
    Args:
        save (bool): If true, saves output DataFrame to a CSV file.
        verbose (bool): If True, prints additional information about process.

    Returns:
        m.pd.DataFrame: The merged DataFrame containing cores, 
        computers, and electricity data.

    Dependencies:
        build_chip_info
        build_electricity_impact_info
        deci_yr_col_to_datetime from utilities
    """
    if verbose :
        print("Building computer, cores, electricity dataframe")

    cores_cleaned_df = build_chip_info(save=False)
    elec_compare_df = build_electricity_impact_info(save=False)
    
    cores_cleaned_df['Country'] = (cores_cleaned_df['Country']
                                   .str.replace('United States',
                                                'United States of America'))
    cores_cleaned_df['Country'] = (cores_cleaned_df['Country']
                                   .str.replace('Korea, South',
                                                'South Korea'))
    cores_cleaned_df['country lower'] = (cores_cleaned_df['Country']
                                         .str.strip().str.lower())
    

    elec_compare_df['country lower'] = (elec_compare_df['Country']
                                        .str.strip()
                                        .str.lower())

    # Clean and match the Rank Year
        ## Built a function for this but not implemented yet
    cores_cleaned_df['Year-Month'] = u.deci_yr_col_to_datetime(
                                     cores_cleaned_df['Rank Year'])

    elec_compare_df['Year-Month'] = (elec_compare_df['Year'].astype(str) +
                                      '-' +
                                      elec_compare_df['Month']
                                      .astype(str)).str.strip()
    
    elec_compare_df['Year-Month'] = m.pd.to_datetime(
                                    elec_compare_df['Year-Month'], 
                                    format='%Y-%m')


    # Merge Datatframes
    cores_computers_elec_full = m.pd.merge(cores_cleaned_df, elec_compare_df,
                                            on=['country lower', 'Year-Month'],
                                            how='left')
    
    cores_computers_elec_full.drop(columns=[
        'Carbon Intensity gCO₂eq/kWh (Life cycle)', 
        'Renewable energy percentage (RE%)',
        'Notes',
        'Country_y', 
        'Continent_y', 
        'Year_y'
        ], inplace=True)
    
    cores_computers_elec_full.rename(columns={
        'Year_x': 'Year', 
        'Month_x': 'Month',
        'Country_x': 'Country',
        'Continent_x': 'Continent',
        'base_clock_speed':'CPU Base Clock Speed', 
        'Family': 'CPU Family',
        'Model':'CPU Model', 
        'num_cores':'CPU Num Cores', 
        'rank_year_x':'Years in Ranking (list)'
        }, inplace=True)
    
    if verbose :
        print(f"Computer, cores, electricity dataframe built with\
            {len(cores_computers_elec_full)} rows and\
            {len(cores_computers_elec_full.columns)} columns.")

    if save:
        (cores_computers_elec_full
         .to_csv('./data/output_data/cores_computers_elec_full.csv',
                 index=False))

    return cores_computers_elec_full

############################################################################
#
# Functions to build minmal datasets
#
############################################################################
#  Just computer info
def read_just_computer_df(col_filter_str: str = '[\S\s]+[\S]+',
                          year_range:tuple = (2012, 2025), 
                          year_range_green:tuple = (2014, 2025), 
                          save:bool = False,
                          verbose:bool = m.VERBOSE_VAL):
    return (buildComputerInfo(year_range, year_range_green, save, verbose)
                    .filter(regex=col_filter_str))

# Just CPU
def read_just_CPU_info(verbose:bool = m.VERBOSE_VAL):
    cpu_df = (m.pd.read_csv('./data/CPU_TDP_data.csv')
              .drop(columns = 'Unnamed: 9')
              .rename(columns={'CPU_TDP':'TDP'}))
    if verbose:
        print('cpu dataframe read with columns:', sorted(cpu_df
                                                         .columns
                                                         .to_list()))
    return cpu_df


# Just GPU
def read_just_GPU_info(verbose:bool = m.VERBOSE_VAL):
    gpu_df = m.pd.read_csv('./data/GPU_TDP_data.csv')
    if verbose:
        print('gpu dataframe read with columns:', sorted(gpu_df
                                                         .columns
                                                         .to_list()))
    return gpu_df

# Just cores, not connected to computer info
def read_just_chips_df(save: bool = False, verbose:bool = m.VERBOSE_VAL):
    cpu = read_just_CPU_info(verbose = verbose)
    gpu = read_just_GPU_info(verbose = verbose)

    cpu['Type'] = 'CPU'
    gpu['Type'] = 'GPU'

    just_chips = m.pd.concat([cpu, gpu])

    if verbose:
        print('Chip dataset constructed with columns ',(sorted(just_chips
                                                               .columns
                                                               .to_list())))
    if save:
        just_chips.to_csv('./data/output_data/chip_data.csv')

    return just_chips


# Just electricity data
def read_just_electricity_df(col_filter_str:str = '[\S\s]+[\S]+',
                             save:bool = False, 
                             verbose:bool = m.VERBOSE_VAL):
    return(build_electricity_impact_info(save, verbose)
                .filter(regex = col_filter_str))


############################################################################
#
# Fuzzy Matching
#
############################################################################
# Fuzzy matching --> adapted, NOT ORIGINALLY MY CODE
CPU_specs = read_just_CPU_info(verbose = False)
GPU_specs = read_just_GPU_info(verbose = False)

# For CPUs
def _get_cpu_spec(cpu_model_name: str) -> m.pd.Series | None:
    cpu_spec = (
        CPU_specs[CPU_specs["Processor"] == cpu_model_name].squeeze()
        if cpu_model_name
        else None
    )

    cpu_spec = m.cast(m.pd.Series | None, cpu_spec)

    return cpu_spec

def find_closest_cpu_model_name(name: str) -> m.List[m.Tuple[str, float, int]]:
    """
    Find the closest CPU model name from the database.
    Returns a list of CPU model names that match the fuzzy search.
    """
    results = m.process.extract(name, CPU_specs["Processor"].astype(str)
                                .tolist(), 
                                scorer=m.fuzz.token_ratio, 
                                limit=None, score_cutoff=99)
    if len(results)==0:
        # adaptive cut-off
        top_score = m.process.extract(name, CPU_specs["Processor"].tolist(), 
                                    scorer=m.fuzz.token_ratio, limit=1)[0][1]
        results = m.process.extract(name, CPU_specs["Processor"].tolist(), 
                                  scorer=m.fuzz.token_ratio, limit=None,
                                  score_cutoff=top_score*0.9)
    return results

def auto_avg(cpu_name, prop='TDP'):
    found_cpu_names = find_closest_cpu_model_name(cpu_name)
    sumW = 0
    sumTdp = 0
    sources = []
    exponent = 8
    if len(found_cpu_names)>0:
        for e in found_cpu_names:
            spec = _get_cpu_spec(e[0])
            if m.math.isfinite(spec[prop]):
                w = (e[1]/100)**exponent*100 # give much more weight to best
                                             # matches (close to infinite norm)
                sumTdp += w*spec[prop]
                sumW += w
                sources.append([e[0],round(w)])
    return round(sumTdp/sumW,1),sources


def apply_fuzzy_cpu_match(df:m.pd.DataFrame,
                          verbose:bool = m.VERBOSE_VAL):
    # Sources are a list of the CPU whose values area averaged to build est.TDP
    def fuzz_match_cpu(row):
        cpu_name = str(row['Processor'])
        if (row['CPU_TDP'] == 0) | (m.pd.isna(row['CPU_TDP'])):
            avg, sources = auto_avg(cpu_name)
            if verbose:
                print(f"{cpu_name} estimated TDP: {avg}, from: {sources}")
            return avg
        else:
            return row['CPU_TDP']
    df['CPU_TDP'] = df.apply(fuzz_match_cpu, axis = 1)
    # if no average found, place a TDP of 200 W
    # (this value is an arbitaray estimation)
    df['CPU_TDP'] = (df['CPU_TDP']
                     .mask((df['CPU_TDP'] == 0) | (df['CPU_TDP'].isna()), 200))

# Fuzzy matching for GPUs
    # If no value availiable, but a processor is said to be there, TDP = 200
def _get_gpu_spec(gpu_model_name: str) -> m.pd.Series | None:
    gpu_spec = (
        GPU_specs[GPU_specs["GPU Model"] == gpu_model_name].squeeze()
        if gpu_model_name
        else None
    )

    gpu_spec = m.cast(m.pd.Series | None, gpu_spec)

    return gpu_spec

def find_closest_gpu_model_name(name: str) -> m.List[m.Tuple[str, float, int]]:
    """
    Find the closest GPU model name from the database.
    Returns a list of GPU model names that match the fuzzy search.
    """
    results = m.process.extract(name, GPU_specs["GPU Model"].astype(str)
                                .tolist(), 
                                scorer=m.fuzz.token_ratio, 
                                limit=None, score_cutoff=99)
    if len(results)==0:
        # adaptive cut-off
        top_score = m.process.extract(name, GPU_specs["GPU Model"].tolist(), 
                                    scorer=m.fuzz.token_ratio, limit=1)[0][1]
        results = m.process.extract(name, GPU_specs["GPU Model"].tolist(), 
                                  scorer=m.fuzz.token_ratio, limit=None,
                                  score_cutoff=top_score*0.9)
    return results

def gpu_auto_avg(gpu_name, prop='GPU_TDP'):
    found_gpu_names = find_closest_gpu_model_name(gpu_name)
    sumW = 0
    sumTdp = 0
    sources = []
    exponent = 8
    if len(found_gpu_names)>0:
        for e in found_gpu_names:
            spec = _get_gpu_spec(e[0])
            if m.math.isfinite(spec[prop]):
                w = (e[1]/100)**exponent*100 # give much more weight to best
                                             # matches (close to infinite norm)
                sumTdp += w*spec[prop]
                sumW += w
                sources.append([e[0],round(w)])
    # protect against divide by 0 error
    if sumW == 0:
        return 200, []
    # if not 0, return the avg
    return round(sumTdp / sumW, 1), sources


def apply_fuzzy_gpu_match(df:m.pd.DataFrame,
                          verbose:bool = m.VERBOSE_VAL):
    # Sources are a list of the GPU whose values area averaged to build est.TDP
    df['GPU_TDP'].fillna(0)
    def fuzz_match_gpu(row):
        if ((row['GPU_TDP']==0)&(row['Accelerator/Co-Processor'] != 0)):
            gpu_name = str(row['GPU Model'])
            if (row['GPU_TDP'] == 0) | (m.pd.isna(row['GPU_TDP'])):
                avg, sources = gpu_auto_avg(gpu_name)
                if verbose:
                    print(f"{gpu_name} estimated TDP: {avg}, from: {sources}")
                return avg
            else:
                return row['GPU_TDP']
    df['GPU_TDP'] = df.apply(fuzz_match_gpu, axis = 1)
    # if no average found, place a TDP of 200 W
    # (this value is an arbitaray estimation)
    df['GPU_TDP'] = df['GPU_TDP'].mask((df['Accelerator/Co-Processor'] != 0) & 
                        ((df['GPU_TDP'] == 0) | (df['GPU_TDP'].isna())), 200)

############################################################################
#
# Build the datasets to use
#
############################################################################

# Build full dataframe with all relevant data, and then some
#   Fuzzy matching helps to fill in missing TDP data
cores_computers_elec_full = connectCoresComputersElectricity(save=False)
apply_fuzzy_cpu_match(df=cores_computers_elec_full, 
                      verbose=m.VERBOSE_VAL)
apply_fuzzy_gpu_match(df=cores_computers_elec_full, 
                      verbose=m.VERBOSE_VAL)

# Get number of CPU/GPUs for power calculation
# Number of CPUs depends on the total number of cores divided by 
# the number of cores per cpu
cores_computers_elec_full['num_CPUs'] = m.np.where(
    cores_computers_elec_full['CPU Num Cores'] > 0,
    cores_computers_elec_full['CPU Cores']/
    cores_computers_elec_full['CPU Num Cores'], 0)
cores_computers_elec_full['num_CPUs'] = m.np.where(
    ((cores_computers_elec_full['CPU_TDP'] > 0)
     & (cores_computers_elec_full['num_CPUs']== 0)),1, 
     cores_computers_elec_full['num_CPUs'])

# Similarly with GPUs
cores_computers_elec_full['num_GPUs'] = m.np.where(
    cores_computers_elec_full['n_shaders'] > 0,
    (cores_computers_elec_full['Accelerator/Co-Processor Cores'] / 
    cores_computers_elec_full['n_shaders']), 0)
cores_computers_elec_full['num_GPUs'] = m.np.where(
    ((cores_computers_elec_full['GPU_TDP'] > 0)
     & (cores_computers_elec_full['num_GPUs']== 0)),1, 
     cores_computers_elec_full['num_GPUs'])
# adjust the number of GPUs by making sure the number of cores reflects 
# to the number of streaming multiprocessors
cores_computers_elec_full['num_GPUs'] = (cores_computers_elec_full['num_GPUs']*
                                            cores_computers_elec_full['n_SM'])

# Filter for a smaller dataframe with just the necessary fields
minimal_cores_computers_elec_full = cores_computers_elec_full.copy(deep=True)
minimal_cores_computers_elec_full = (minimal_cores_computers_elec_full
    .groupby('HashedID')
    .first()
    .reset_index()
    .filter(regex='HashedID|Rank Year List|Hourly|Year|_TDP|Country|Intensity')
    .fillna(0))

# Remove duplicates from Rank Year List in the minimal dataframe
for i in range(0, len(minimal_cores_computers_elec_full)):
    (minimal_cores_computers_elec_full
        .at[minimal_cores_computers_elec_full
        .index[i],'Rank Year List']) = list(
        set(minimal_cores_computers_elec_full
            .at[minimal_cores_computers_elec_full
            .index[i],'Rank Year List']))
