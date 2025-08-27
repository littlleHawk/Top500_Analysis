###################################################
#
#              U T I L I T E S
#
###################################################

"""
UTILITIES Module
================

Utility functions for preprocessing and analyzing HPC datasets,
to support downstream tasks.

Main Functionalities:
---------------------

**Top500 / Green500:**
- `read_filter_top500_data`: Load and standardize Top500 data.
- `read_filter_green500_data`: Load and normalize Green500 data.
- `get_headers`: Extract column names from Top500 files.

**Electricity Data Processing:**
- `read_filter_electricity_maps`: Load & clean Electricity Maps data.

**Data Management & Inspection:**
- `concat_all_df`: Concatenate multiple filtered DataFrames.
- `remove_outliers`: Remove statistical outliers based on z-score.
- `accel_present`: Identify presence of accelerators or co-processors.

**Modeling & Trends:**
- `calc_apparition`: Track system presence across ranking periods.
- `exp_funct`, `moore_funct`, `koomey_funct`: Apply growth trend models.

**Lifespan & Drop-Off Analysis:**
- `enumerate_years`: Annotate system lifespans across rankings.
- `reason_of_leaving_list`: Infer drop-off reasons using next-period
                            comparisons.

**Energy Estimation:**
- `compute_power_use`: Estimate 6-month power use by utilization.

**Time Utilities:**
- `deci_yr_col_to_datetime`: Convert decimal years to datetime.


Dependencies:
-------------
- Requires `mod` alias for standard libraries

Data Paths:
-----------
- Top500 data:         ./data/top500/
- Green500 data:       ./data/green500/
- Electricity Maps:    ./data/ElectricityMapsData/
- Outputs:             ./data/output_data/

Notes:
------
- Normalizes fields across years and sources.
- Adds computed fields
"""

import project_lib as mod

# Some useful functions to:
###############################################################################
# Filter data and read into dataframes:
###############################################################################

def read_filter_top500_data(y: int, 
                            m: str, 
                            range: int, 
                            fields: list, 
                            save: bool = False) -> mod.pd.DataFrame:
    """Read and filter the Top500 list for a given year and month.

    Args:
        y (int): Year.
        m (int): Month.
        range (int): Number of entries to keep.
        fields (list): Columns to retain.
        save (bool): If True, saves the result as a CSV.

    Returns:
        mod.pd.DataFrame: Filtered DataFrame.

    Dependencies:
        File path: ./data/top500/TOP500_{y}{m}.xls[x]
        Columns: Processors, Total Cores, Rmax, Rpeak, etc.
    """
    # reading data
    file_path = f"./data/top500/TOP500_{y}{m}.xls{'x'*min(1, max(0, y-2019))}"
    df_f = mod.pd.read_excel(
            file_path, 
            header=1 - min(1, max(0, y - 2007))
        )
    # truncating by range
    df_f = df_f.iloc[:range]

    # some data cleaning + scaling (Tflops to Gflops)
    if y < 2008 or (y == 2008 and m == '06'):
        df_f['Cores'] = df_f['Processors']
    elif y > 2011 or (y == 2011 and m == '11'):
        df_f['Cores'] = df_f['Total Cores']
        if y > 2017:
            df_f['RMax'] = df_f['Rmax [TFlop/s]'] * 1000 
            df_f['RPeak'] = df_f['Rpeak [TFlop/s]'] * 1000
        else:
            df_f['RMax'] = df_f['Rmax'] * (1000 - 999 * 
                                           min(1, max(0, 2017 - y)))
            df_f['RPeak'] = df_f['Rpeak'] * (1000 - 999 * 
                                             min(1, max(0, 2017 - y)))

    df_f['perf_percent'] = df_f['RMax']/df_f['RPeak']
    df_f.loc[df_f['perf_percent'] > 1, 'RPeak'] *= 10
    df_f['perf_percent'] = df_f['RMax']/df_f['RPeak']


    # adding the ranking date
    df_f['rank_year'] = '-'.join([str(y), str(m)])

    ## Adding Hash to ID    
    df_f=df_f.rename(columns={'name':'Name'})
    df_f['HashedID'] = df_f.apply(
        lambda row: mod.hl.md5('-'.join([str(row['Computer']),
                                         str(row['Site']), 
                                         str(row['Name']), 
                                         str(row['Country']), 
                                         str(row['Year'])])
                                         .encode()).hexdigest(),
                                         axis=1
    )
    df_f['Country'] = df_f['Country'].apply(lambda x: "Czechia" if x ==  
                                            "Czech Republic" else x)
    # Add Relative difference between RMax and RPeak
    df_f['RPeak-RMax'] = (df_f['RPeak'] -df_f['RMax'])/df_f['RPeak']
    # add listing info
    df_f['Listing'] = 'Top500'
    # choosing the fields of interest 
    df_f = df_f[fields]

    # saving the newly created df                             
    if save:
        df_f.to_csv(f'./data/output_data/TOP500_{y}{m}.csv')
    
    return df_f

def read_filter_green500_data(y: int, 
                              m: str, 
                              range: int, 
                              fields: list, 
                              save: bool = False) -> mod.pd.DataFrame:
    
    """Load and filter a Green500 list for a given year/month.

    Args:
        y (int): Year.
        m (int): Month.
        range (int): Number of top systems to keep.
        fields (list): Columns to retain.
        save (bool): If True, save the result as CSV.

    Returns:
        mod.pd.DataFrame: Filtered Green500 DataFrame.
    
    Dependencies:
        File: ./data/green500/#green500_{y}_{m}.xlsx
    """
    # reading data
    df_f = mod.pd.read_excel(f"./data/green500/#green500_{y}_{m}.xlsx", 
                             header=0)

    # truncating by range
    df_f = df_f.iloc[:range]

    # homogenising the fields
    if y == 2014 and m == '06':
        df_f['TOP500 Rank'] = df_f['Top500_Rank']
        df_f['Rank'] = df_f['green500_rank']
        df_f['G_eff'] = df_f['mflops_per_watt'] / 1000
    elif y == 2015:
        df_f['TOP500 Rank'] = df_f['top500_rank']
        df_f['Rank'] = df_f['green500_rank']
        df_f['G_eff'] = df_f['mflops_per_watt'] / 1000
        df_f['Power'] = df_f['total_power']
    elif y == 2016 and m == '06' or (y == 2014 and m == '11'):
        df_f['TOP500 Rank'] = df_f['Top500_Rank']
        df_f['G_eff'] = df_f['Mflops/Watt'] / 1000
        df_f['Rank'] = df_f['Green500_Rank']
    elif y == 2016 and m == '11':
        df_f['TOP500 Rank'] = df_f['Rank']
        df_f['Rank'] = df_f['Green500_Rank']
        df_f['G_eff'] = df_f['Mflops/Watt'] / 1000
    elif y < 2020:
        df_f['G_eff'] = df_f['Power Effeciency [GFlops/Watts]']
    elif y < 2022:
        df_f['G_eff'] = df_f['Power Efficiency [GFlops/Watts]']
    else:
        df_f['G_eff'] = df_f['Energy Efficiency [GFlops/Watts]']
    
    if y < 2008 or (y == 2008 and m == '06'):
        df_f['Cores'] = df_f['Processors']
    elif y> 2011 or (y == 2011 and m == '11'):
        df_f['Cores'] = df_f['Total Cores']
        if y >= 2017:
            df_f['RMax'] = df_f['Rmax [TFlop/s]'] * 1000
            df_f['RPeak'] = df_f['Rpeak [TFlop/s]'] * 1000
            df_f['Power'] = df_f['Power (kW)']
        else:
            df_f['RMax'] = df_f['Rmax'] * (1000 - 999 * 
                                           min(1, max(0, 2017 - y)))
            df_f['RPeak'] = df_f['Rpeak'] * (1000 - 999 * 
                                            min(1, max(0, 2017 - y)))
        
    # adding the ranking date
    df_f['rank_year'] = '-'.join([str(y), str(m)])
    
    # performance percentage
    df_f['perf_percent'] = df_f['RMax']/df_f['RPeak']
    df_f.loc[df_f['perf_percent'] > 1, 'RPeak'] *= 10
    df_f['perf_percent'] = df_f['RMax']/df_f['RPeak']

    # HPC type (heterogeneous / homogeneous:
    df_f['Accelerator/Co-Processor'] = list(df_f['Accelerator/Co-Processor']
                                            .notnull()
                                            .astype('int'))

    ## Adding Hash to ID 
    df_f=df_f.rename(columns={'name':'Name'})  
    df_f['HashedID'] = df_f.apply(
        lambda row: mod.hl.md5('-'.join([str(row['Computer']), 
                                         str(row['Site']), 
                                         str(row['Name']), 
                                         str(row['Country']), 
                                         str(row['Year'])])
                                         .encode()).hexdigest(),
                                         axis=1
    )

    df_f['Country'] = df_f['Country'].apply(lambda x: "Czechia" if x == 
                                             "Czech Republic" else x)

    # Add Relative difference between RMax and RPeak
    df_f['RPeak-RMax'] = (df_f['RPeak'] -df_f['RMax'])/df_f['RPeak']

    df_f['Listing'] = 'Green500'

    # choosing the fields of interest
    df_f = df_f[fields]

    # saving the newly created df                             
    if save:
        df_f.to_csv(f'./data/output_data/GREEN500_{y}{m}.csv')

    return df_f

def read_filter_electricity_maps(year:int, 
                                 abbr: str, 
                                 save:bool = False, 
                                 verbose:bool=mod.VERBOSE_VAL)->mod.pd.DataFrame:
    """
    Reads and cleans Electricity Maps data for a specified year and region.

    Args:
        year (int): Year of interest.
        abbr (str): Region abbreviation
        save (bool, optional): Whether to save the cleaned DataFrame. 
                               Defaults to False.
        verbose (bool, optional): Whether to print progress messages. 
                                  Defaults to False.

    Returns:
        pd.DataFrame: The cleaned Electricity Maps data.
    
    Dependencies:
        File: ./data/ElectricityMapsData/{abbr}_{year}_monthly.csv
    """

    if(abbr != 0):
        if verbose:
            print(f"Reading {abbr} data from {year}...")
        #data is from 2021 - 2024 monthly
        file_path = f"./data/ElectricityMapsData/{abbr}_{year}_monthly.csv"

        df_f = mod.pd.read_csv(file_path, sep=',', header=0, 
                               skip_blank_lines=True)
        #df_f = df_f[df_f.columns[:9]]

        if 'Zone name' in df_f.columns:
            df_f.rename(columns={'Zone name': 'Zone Name'}, inplace=True)
        if 'Zone id' in df_f.columns:
            df_f.rename(columns={'Zone id': 'Zone Id'}, inplace=True)
        if 'Carbon intensity gCO₂eq/kWh (direct)' in df_f.columns:
            df_f.rename(columns={'Carbon intensity gCO₂eq/kWh (direct)':
                                 'Carbon Intensity gCO₂eq/kWh (direct)'},
                                  inplace=True)
        if 'Carbon intensity gCO₂eq/kWh (Life cycle)' in df_f.columns:
            df_f.rename(columns={'Carbon intensity gCO₂eq/kWh (Life cycle)': 
                                 'Carbon Intensity gCO₂eq/kWh (Life cycle)'}, 
                                 inplace=True)
        if 'Carbon Intensity gCO₂eq/kWh (LCA)' in df_f.columns:
            df_f.rename(columns={'Carbon Intensity gCO₂eq/kWh (LCA)': 
                                 'Carbon Intensity gCO₂eq/kWh (Life cycle)'}, 
                                 inplace=True)
        if 'Renewable Percentage' in df_f.columns:
            df_f.rename(columns={'Renewable Percentage':
                                 'Renewable energy percentage (RE%)'}, 
                                 inplace=True)
        if 'Data source' in df_f.columns:
            df_f.rename(columns={'Data source':'Data Source'}, inplace=True)
    else:
        #df is empty
        df_f = mod.pd.DataFrame()
    
    if(save):
        df_f.to_csv(f'./data/output_data/ElectricityMaps_{abbr}_{year}.csv')
    
    return df_f

def concat_all_df(dataframes: list, 
                  save: bool = False, 
                  saveTo: str = './data/all_df_hashed_concat.csv')->mod.pd.DataFrame:
    """
    Concatenates a list of DataFrames into one combined DataFrame.

    Args:
        dataframes (list): List of pandas DataFrames.
        save (bool, optional): Whether to save the result to CSV. 
                               Defaults to False.
        saveTo (str, optional): Output file path. 
                                Defaults to './data/all_df_hashed_concat.csv'.

    Returns:
        pd.DataFrame: Concatenated DataFrame.
    """

    all_df = mod.pd.concat(dataframes, ignore_index=True)

    if save:
        all_df.to_csv(saveTo)

    return all_df 

def get_headers(y,m)->mod.pd.DataFrame:
    """
    Reads a Top500 Excel file and returns its column headers.

    Args:
        y (int): Year of the dataset.
        m (str): Month ('06' or '11') of the dataset.

    Returns:
        pd.DataFrame: A DataFrame containing the column headers.
    """

    # reading data
    file_path = f"./data/top500/TOP500_{y}{m}.xls{'x'*min(1, max(0, y-2019))}"
    df_f = mod.pd.read_excel(
            file_path, 
            header=1 - min(1, max(0, y - 2007))
        )

    # some data cleanin
    if y < 2008 or (y == 2008 and m == '06'):
        df_f['Cores'] = df_f['Processors']
    elif y > 2011 or (y == 2011 and m == '11'):
        df_f['Cores'] = df_f['Total Cores']
        if y > 2017:
            df_f['RMax'] = df_f['Rmax [TFlop/s]'] * 1000 
            df_f['RPeak'] = df_f['Rpeak [TFlop/s]'] * 1000

    df_f['perf_percent'] = df_f['RMax']/df_f['RPeak']
    df_f.loc[df_f['perf_percent'] > 1, 'RPeak'] *= 10
    df_f['perf_percent'] = df_f['RMax']/df_f['RPeak']

    df_f['rank_year'] = '-'.join([str(y), str(m)])

    return mod.pd.DataFrame(df_f.columns.values)

###############################################################################
# Perform operations on the data and calculate other values
###############################################################################
  
def exp_funct(x: float, a: float, b: float) -> float:
    """Calculates the exp."""
    return a * mod.np.power(mod.np.exp(x), b)

def moore_funct(x: float, df: mod.pd.DataFrame, metric: str = "RMax") -> float:
    """Calculates moore's prediction."""
    return df.iloc[0][metric] * 2 ** (x/4)

def koomey_funct(x: float, df: mod.pd.DataFrame, 
                 y: float = 0, metric: str = "G_eff") -> float:
    """Calculates koomey's prediction."""
    return df.iloc[y][metric] * 2 ** ((x-y)/3)

###############################################################################
# Funcitons for adding addtitional data to dataframes
###############################################################################
def accel_present(df:mod.pd.DataFrame)-> mod.pd.DataFrame:
    """ Checks if an accelerator or co-processor is present in a system."""
    df['Accelerator_Present'] = (
        mod.np.where(df['Accelerator/Co-Processor']==0, 
            'No',
            'Yes'))
    return df    

def enumerate_years(df: mod.pd.DataFrame)-> mod.pd.DataFrame:
    """Enumerates the years in the dataframe and adds information 
       about lifespan of the computer.

    Args:
        df (mod.pd.DataFrame): Input DataFrame with 'rank_year' and 'HashedID'

    Returns:
        mod.pd.DataFrame: DataFrame with lifespan and ranking periods.
    
    Dependencies:
        Colums: 'HashedID', 'rank_year', 'Year'
    """
    # reaname for clarity
    df['Installation Year'] = df['Year']

    #turn rank_year string into float
        # o,5 is added to indicate second semester ranking (November list)
    for i in range(0, len(df)):
        additive = 0.0
        if df.loc[i, 'rank_year'].rsplit('-', 1)[1] == '11':
            additive = 0.5
        df.loc[i, 'rank_year'] = (float(df.loc[i, 'rank_year']
                                        .rsplit('-', 1)[0]) 
                                        + additive)

    # group by hashedID and add all semesters where the system ranked 
    # to list assocaited with that HashedID
    Hash_list_df = df.groupby(['HashedID'])['rank_year'].apply(list)
    # merged dataframes of Hash_list_df and years_enumerated_df so each 
    # computer also has a list of its appearances
    df = mod.pd.merge(Hash_list_df, df, on='HashedID', how='inner')

    # add final year of appearance
    for i in range(0, len(df)):
        df.loc[i, 'Final Year'] = max(df.loc[i,'rank_year_x'])
    # add lifespan column to df
    for i in range(0, len(df)):
        df.loc[i, 'Lifespan'] = (max(df.loc[i,'rank_year_x']) - 
                                 df.loc[i,'Installation Year'])

    return df

def reason_of_leaving_list(df:mod.pd.DataFrame, 
                           choiceRPeakOrRMax:str = 'RMax', 
                           maxYear:float = 2024.5)-> mod.pd.DataFrame:
    """
    Identifies if a system remains in, was dropped, 
        or was removed from the ranking list.

    Args:
        df (pd.DataFrame): DataFrame of systems with rank and metric.
        choiceRPeakOrRMax (str, optional): Metric to compare for dropoff
                         ('RMax' or 'RPeak'). Defaults to 'RMax'.
        maxYear (float, optional): Evaluation cutoff year. Defaults to 2024.5

    Returns:
        pd.DataFrame: DataFrame with reason labeled.
    
    Dependencies:
        Calls enumerate_years()
        Columns: 'Rank', 'RMax', 'RPeak'
    """
    df = enumerate_years(df)

    # create df with the rmax and rpeak of the 500th position
    # rank_year is the semester before the rpeak rmax
    #  was measured for 500th position   
    df_500 = df[['rank_year_y','Rank', 'RMax', 'RPeak']].copy()
    df_500 = df_500.rename(columns={'rank_year_y':'next_rank_year',
                                    'RPeak':'RPeak_500', 
                                    'RMax':'RMax_500'})
    df_500 = df_500.loc[df_500['Rank'] == 500]
    df_500['next_rank_year'] = df_500['next_rank_year'].apply(lambda x: x-0.5)

    # join df_500 with df to line up 500th rpeak rmax with the 
    # final year of each computer
    df = mod.pd.merge(df, df_500, 
                      left_on='Final Year', 
                      right_on='next_rank_year', 
                      how='left')
    
    df = df.loc[df['Final Year'] < maxYear]
    
    # compare RMax and RPeak of the computer with the 
    # 500th position of the next list
    if choiceRPeakOrRMax == 'RMax':
        df['RMax_500'] = df['RMax_500'].astype(float)
        df['RMax'] = df['RMax'].astype(float)
        df['Reason_Leave_Ranking'] = mod.np.where(df['RMax'] < df['RMax_500'],
                                                  'Dropped', 'Removed')
    else:
        df['RPeak_500'] = df['RPeak_500'].astype(float)
        df['RPeak'] = df['RPeak'].astype(float)
        df['Reason_Leave_Ranking'] = mod.np.where(df['RPeak'] < df['RPeak_500'],
                                                  'Dropped', 'Removed')

    # remove unneeded columns
    #df = df.drop(columns=['Rank_y', 'next_rank_year', 'RMax_500', 'RPeak_500'])
    df = df.rename(columns={'rank_year_x':'list_all_years_in_ranking',
                            'rank_year_y':'rank_year', 
                            'Rank_x':'Rank'})
    return df

# Returns power use BY SEMESTER 
#   multiplication by 6 at the end turns from month to semester
def compute_power_use(tau:float, df:mod.pd.DataFrame, CPUorGPU:str)-> mod.pd.Series:
    p_idle_col_str = 'P_idle_' + CPUorGPU
    tdp_col_str = CPUorGPU + '_TDP_Total'
    return (24 * (365 / 12) * 
            ((1 - tau) * df[p_idle_col_str] + 
             tau * df[tdp_col_str]) / 1000000000) * 6


###############################################################################
# Convert and normalize data, Fuzzy matching for data homogenization
###############################################################################
def deci_yr_col_to_datetime(deci_year_series: mod.pd.Series)-> mod.pd.Series:
    """
    Converts a Series of float-based Years to a date string in
     'YYYY-MM' format.
    """
    years = deci_year_series.astype(int)
    months = (mod.pd.to_numeric(((deci_year_series % 1) * 10))
                               .round()
                               .astype(int) + 1)

    return mod.pd.to_datetime({
        'year': years,
        'month': months,
        'day': 1
    })


def remove_outliers(df: mod.pd.DataFrame, 
                    colName: str, 
                    threshold: int = 3, 
                    verbose: bool = mod.VERBOSE_VAL) -> mod.pd.DataFrame:

    """
    Removes outliers from a column using z-score thresholding.

    Args:
        df (pd.DataFrame): Input DataFrame.
        colName (str): Column to clean.
        threshold (int, optional): Z-score threshold. Defaults to 3.
        verbose (bool, optional): Whether to print summary. Defaults to False.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """

    z_scores = mod.zscore(df[colName], nan_policy='omit')
    outliers = df.index[abs(z_scores) >= threshold]
    df = df.drop(index=outliers)
    
    if verbose:
        print(f"Removed {len(outliers)} outliers from {colName}")
    
    return df