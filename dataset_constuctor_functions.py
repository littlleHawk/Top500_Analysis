###################################################
#
#              DATASET CONSTUCTORS
#                  for widgets
#
###################################################
"""
DATASET CONSTRUCTORS Module
===========================

Builds datasets for analyzing HPC power use and carbon impact, with support
for interactive widget-based filtering.

Main Functions:
---------------
- `filter_df`: Filters a DataFrame based on widget parameters.
- `power_consumption_construct_df`: Computes monthly power consumption per
  supercomputer using core, chip, and electricity data.

Dependencies:
-------------
- `utilities`, `project_lib`, and `widgets` modules  
- Requires `cores_computers_elec_full` from `build_dfs` as input data
"""


import utilities as u
import project_lib as m
from build_dfs import cores_computers_elec_full

# get dataframe to be used by the following functions
in_df = cores_computers_elec_full.copy(deep=True)

# #Filter data
def filter_df(df: m.pd.DataFrame, 
              country_name:str, 
              segment_name:tuple, 
              listing_name:str, 
              carbon_choice:str,
              verbose:bool = m.VERBOSE_VAL):
    """
    Filters the DataFrame based on widget parameters.
    
    Args:
        df (pd.DataFrame): input dataframe to be filtered
        country_name (str): widget value of country to be used to filter data
        segment_name (tuple of strings): widget value of segment (Sector area)
                      to be used to filter data
        listing_name (str): widget value of Listing to be used to filter data
        carbon_choice (str): widget value of choice of carbon impact source 
                             data to be used to filter data
        verbose (bool): If True, prints additional information about 
                        the process.

    Returns:
        m.pd.DataFrame: The filterd DataFrame containing all entries 
            in the input dataframe which match the provided filter values.
    """
    filtered_df = df.copy()
    if 'Country' in df.columns:
        if country_name != 'Somme de Tous':
            filtered_df = filtered_df[filtered_df['Country'] == country_name]
        else:
            filtered_df = filtered_df.drop(columns=['Country'])
    if 'Segment' in df.columns:
            if segment_name != ('Tous',):
                filtered_df = (filtered_df[filtered_df['Segment']
                                           .isin(segment_name)])
    if 'Listing' in df.columns:
        if listing_name != 'Tous':
            filtered_df = filtered_df[filtered_df['Listing'] == listing_name]
        else:
            filtered_df = filtered_df.drop(columns=['Listing'])
    if 'Carbon Intensity' in df.columns:
        if carbon_choice == 'Electricity Maps':
            filtered_df = (filtered_df[filtered_df['Carbon Intensity Source']== 
                            'Electricity Maps Carbon Intensity gCO₂eq/kWh'])
        else:
            filtered_df = (filtered_df[filtered_df['Carbon Intensity Source']== 
                            'Ember Carbon Intensity gCO₂eq/kWh'])
    if verbose:
        print(f"filter_df: Filtered DataFrame for \
              {country_name}, {segment_name}, {listing_name},\
              {carbon_choice} with {filtered_df.shape[0]} rows\
              and {filtered_df.shape[1]} columns.")

    return filtered_df

def construct_energy_and_carbon_impact_df(alpha:float, 
                                   tau:tuple, 
                                   time_after_drop: float, 
                                   carbon_choice:str,
                                   in_df: m.pd.DataFrame = in_df,
                                   time_span:tuple = (2012.0, 2024.5), 
                                   verbose:bool= m.VERBOSE_VAL):
    """
    Calculates and constructs a dataframe of the Monthly energy consumption 
    of each system using estimation values tau and alpha in the formula:
    Power consumption =  24 * (365 / 12) * ((1 - tau) * P_idle + (tau * P_max) 
    where
        tau = rate of use, starting at the lower bound in the first semester 
                and augmenting to the higher bound 12 months later 
        P_idle = estimated proportion of maximum power consumed at idle, 
                given by the TDP * alpha parameter
        P_max = estimated power consumption at maximum use, given by TDP

    Args:
        alpha (float): proportion of max power used at idle
        tau (tuple): tuple containing the lower and upper bounds of a system's
            rate of use
        in_df (pd.Dataframe): dataframe with values to use to create energy
            dataframe. Must have columns:
                            CPU_TDP
                            GPU_TDP
                            Installation Year
                            Lifespan
                            Reason_Leave_Ranking
                        either:
                            Electricity Maps Carbon Intensity gCO₂eq/kWh
                            Ember Carbon Intensity gCO₂eq/kWh

        time_span (tuple of floats): lower and upper bounds of semesters 
            to be included, defaults to (2012.0, 2024.5)
        verbose (bool): if true, prints progress messages

    Returns: A dataframe describing the total Energy consumption for each 
             included semester

    Dependencies:
        compute_power_use
        deci_yr_col_to_datetime
    """
    
    df = in_df.copy(deep=True).groupby('HashedID', as_index=False).first()
    # This system (Gyoukou) was causing the peak in 2017. 
    # Number of cores is now updated to address this
    # df = df[df['HashedID'] != '3b0f1861e44b8180229b470a968d5651']

    # Incorporate lifespan after drop for systems that have 
    # not been explicitly removed but are no longer in the list
    df['Lifespan'] = (m.np.where(df['Reason_Leave_Ranking']=='Dropped', 
                                 df['Lifespan'] + time_after_drop, 
                                 df['Lifespan']))

    df['P_idle_CPU'] = df['CPU_TDP'] * alpha
    df['P_idle_GPU'] = df['GPU_TDP'] * alpha

    df['CPU_TDP_Total'] = df['num_CPUs'] * df['CPU_TDP']
    df['GPU_TDP_Total'] = df['num_GPUs'] * df['GPU_TDP']


    df.fillna(0, inplace=True)
    
    expanded_df = []

    time_values = m.np.arange(time_span[0], time_span[1], 0.5)
    
    # Track system additions and removals
    previous_active_systems = set()

    # Apply time_after_drop
    for i in range(0, len(time_values)):
        temp_df = df.copy()
        temp_df['temp_yr_coeff'] = time_values[i] - temp_df['Installation Year']
        condlist = [
            temp_df['temp_yr_coeff'] == 0,
            temp_df['temp_yr_coeff'] == 0.5,
            ((temp_df['temp_yr_coeff'] >= 1) & 
                    (temp_df['temp_yr_coeff'] < temp_df['Lifespan']))
        ]
        
        # apply power function, also converts to GWh (1 kWH = 1,000,000 GWh)
        CPU_choicelist = [
            u.compute_power_use(tau = tau[0], df = df, CPUorGPU = 'CPU'),
            u.compute_power_use(tau = ((tau[1] + tau[0]) / 2), df = df, 
                                                        CPUorGPU = 'CPU'),
            u.compute_power_use(tau = tau[1], df=df, CPUorGPU = 'CPU')
        ]

        GPU_choicelist = [
            u.compute_power_use(tau = tau[0], df = df, CPUorGPU = 'GPU'),
            u.compute_power_use(tau = ((tau[1] + tau[0]) / 2), df = df, 
                                                        CPUorGPU = 'GPU'),
            u.compute_power_use(tau = tau[1], df=df, CPUorGPU = 'GPU')
        ]

        temp_df['CPU Energy Consumption (Monthly)'] = m.np.select(condlist, 
                                                                 CPU_choicelist, 
                                                                 default=0)
        temp_df['GPU Energy Consumption (Monthly)'] = m.np.select(condlist, 
                                                                 GPU_choicelist, 
                                                                 default=0)
        temp_df['semester'] = time_values[i]
        temp_df['Total Energy Consumption (Monthly)'] = (
            temp_df['CPU Energy Consumption (Monthly)'] +
            temp_df['GPU Energy Consumption (Monthly)']
        )

        # Track active systems (systems with energy consumption > 0)
        current_active_systems = set(
            temp_df[
                (temp_df['CPU Energy Consumption (Monthly)'] > 0) |
                (temp_df['GPU Energy Consumption (Monthly)'] > 0)
            ]['HashedID'].values
        ) if 'HashedID' in temp_df.columns else set()

        # Calculate additions and removals
        systems_added = len(current_active_systems - previous_active_systems)
        systems_removed = len(previous_active_systems - current_active_systems)
        
        # Add system tracking columns to temp_df
        temp_df['Systems Added'] = systems_added
        temp_df['Systems Removed'] = systems_removed
        temp_df['Active Systems'] = len(current_active_systems)

        # Update previous_active_systems for next iteration
        previous_active_systems = current_active_systems

        # Keep only the necessary columns or everything — depending on needs
        expanded_df.append(temp_df)

        if verbose:
            print(f"power_consumption_construct_df: appending year \
        {time_values[i]} to year_month_list for CPU power of \
        {temp_df['CPU Energy Consumption (Monthly)'].sum()} GWh")

    # build result_df
    result_df = m.pd.concat(expanded_df, ignore_index=True)
    
    # Incorporate Carbon impact info
        # gCO2 * GWh = KgCO2 * 1000 = 1 tonne (1 tonne=10^6 g)
    # get carbon factor
    if carbon_choice == 'Electricity Maps':
        source = 'Electricity Maps Carbon Intensity gCO₂eq/kWh'
    else:
        source = 'Ember Carbon Intensity gCO₂eq/kWh'

    # Multiply by energy conso and add to dataframe
    result_df['Carbon Impact CPU (tonnes CO₂eq)'] = (
        result_df['CPU Energy Consumption (Monthly)'] * result_df[source])
        
    result_df['Carbon Impact GPU (tonnes CO₂eq)'] = (
        result_df['GPU Energy Consumption (Monthly)'] * result_df[source])
        
    result_df['Carbon Impact Total (tonnes CO₂eq)'] = (
        result_df['Total Energy Consumption (Monthly)'] * result_df[source])
        
    if verbose:
        print(f'Carbon Impact DataFrame: source {carbon_choice} created')
    
    #Sum by semester
    result_df = (result_df
                 .filter(regex='TDP|Carbon|semester|Energy|HashedID|System'))
    # Force semester to float and round to avoid precision errors
    result_df['semester'] = result_df['semester'].astype(float).round(1)

    #group and sum numeric columns and count number of HPSCs in each semester
    energy_columns = ['CPU Energy Consumption (Monthly)', 
                      'GPU Energy Consumption (Monthly)', 
                      'Total Energy Consumption (Monthly)']
    carbon_columns = ['Carbon Impact CPU (tonnes CO₂eq)', 
                      'Carbon Impact GPU (tonnes CO₂eq)', 
                      'Carbon Impact Total (tonnes CO₂eq)']
    
    # Define aggregation functions for different column types
    agg_dict = {}
    
    # Sum energy and carbon columns
    for col in energy_columns + carbon_columns:
        if col in result_df.columns:
            agg_dict[col] = 'sum'
    
    # Take first value for system tracking 
    # (since they're the same within each semester)
    for col in ['Systems Added', 'Systems Removed', 'Active Systems']:
        if col in result_df.columns:
            agg_dict[col] = 'first'
    
    # Count number of systems (rows) per semester
    if 'HashedID' in result_df.columns:
        agg_dict['HashedID'] = 'count'
        # Rename this column after groupby to be more descriptive
    
    # Group by semester and apply aggregations
    result_df = result_df.groupby('semester', as_index=False).agg(agg_dict)
    
    # Rename the HashedID count column to be more descriptive
    if 'HashedID' in result_df.columns:
        result_df = (result_df
                .rename(columns={'HashedID': 'Total SystemszzContributing'}))
    
    # Add col with Year-Month as a datetime
    result_df['Year-Month'] = u.deci_yr_col_to_datetime(result_df['semester'])

    return result_df
