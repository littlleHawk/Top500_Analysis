# Testbed.py

#imports
#%%
import utilities as u
import project_lib as m
import build_dfs as bdf
import dataset_constuctor_functions as dcf
# import widget_functions as wf
# import widgets as w

#%%
##########################
#  Read in Data
##########################
#Read in basic datasets
cores = bdf.read_just_chips_df()
elec = bdf.read_just_electricity_df()
comp_dataslice = bdf.read_just_computer_df().sample(n=10)


# Get sample of the full dataset to play with
num_vals = 10

big_set = bdf.cores_computers_elec_full.copy(deep=True)
big_set_dataslice = big_set.copy(deep=True).sample(n=num_vals)

# Check out datatset
big_set_dataslice.head().T

# build own sample dataset to play with
data = {'HashedID': ['Comp1', 'Comp2', 'Comp3'],
        'Accelerator/Co-Processor': [0,1,'N/A'],
        'CPU_TDP':[100, 0.0, 'N/A'],
        'GPU_TDP':[100, 0.0, 'N/A'],
        'num_CPUs' : [10, 10, 10],
        'num_GPUs' : [10, 10, 10],
        'P_idle_CPU' : [10, 10, 10],
        'P_idle_GPU' : [10, 10, 10],
        'Processor':['AMD EPYC 7V12 48C 2.45GHz','AMD EPYC7V12 60C 2.45GHz' ,
                     'AMD 7V12'],
        'GPU Model':['Type 1', 'Tupe 2', 0],
        'Ember Carbon Intensity gCO₂eq/kWh': [10,50,10],
        'Installation Year' : [2015, 2018, 2020],
        'Lifespan' : [5, 5, 5],
        'Reason_Leave_Ranking' : ['Dropped', 'Removed', 'Removed']
                     }
playData = m.pd.DataFrame(data = data)
# Filling NA with 0 makes sure all the thing run smooth when 0 is not 
# valid value for the data in the column
playData = playData.replace('N/A', m.np.nan).fillna(0)
##########################
# Testing Functions
##########################
# From dataset constructors

# From Utilities
# %%
# play data
accelPresPlay = u.accel_present(playData)
print(f"accel_present test completed on fake data \
with {len(accelPresPlay[accelPresPlay['Accelerator_Present']==0])}\
 missing values")
print(accelPresPlay.filter(regex='Accelerator'))

# Dataset sample
accelPres = u.accel_present(big_set_dataslice).copy().fillna(0)

print(f"accel_present test completed on data sample \
with {len(accelPres[accelPres['Accelerator_Present']==0])}\
 missing values")

print(accelPres.filter(regex='Accelerator').head())

# %%
#TODO: Take this df from start to end to make sure of all the steps
# Power Conso and Carbon
      # these do provide the same outputs as hand-calculations
tau = (0.5,0.8)
how = 'CPU'
alpha = 0.5

time_after_drop = 2

#Copy datasets to use
pd = playData.copy(deep=True)
ds = big_set_dataslice.copy(deep=True)

#fill in necessary cols where missing
ds['P_idle_CPU'] = ds['CPU_TDP'] * alpha
ds['P_idle_GPU'] = ds['GPU_TDP'] * alpha

# Fill TDP where missing
      #CPUs
# bdf.apply_fuzzy_cpu_match(df=ds)
# bdf.apply_fuzzy_cpu_match(df=pd)
# TODO: Build GPU fuzzy match and fill GPU Data
      #GPUs


# Testing power use calculation
#%%
pow = u.compute_power_use(tau[0], pd, how)
print(pow)

# with dataslice
ds['pow'] = u.compute_power_use(tau[0],ds, how)
print(ds.filter(regex='pow|CPU_TDP'))

#%%
#For pd and ds, run constuctor cell above

#select dataset: 
      # big_set = full data
      # ds = slice of full data
      # pd = play (test) data
df_to_use = big_set
# bdf.apply_fuzzy_cpu_match(df=df_to_use)
# bdf.apply_fuzzy_gpu_match(df=df_to_use)

#%%
#Test power_consumption_construct_df

pow_df = dcf.construct_energy_and_carbon_impact_df(alpha=alpha, tau=tau, 
                                          time_after_drop = time_after_drop,
                                          carbon_choice='Ember',
                                          in_df=df_to_use,
                                          verbose=True)
#pow_df.head().T
#%%
m.px.line(pow_df, x='semester', y='CPU Energy Consumption (Monthly)', hover_data=(
    'Systems Added', 'Systems Removed', 'Active Systems'))
#%%
#  Test carbon_impact
# vals by month -> add together
m.px.line(pow_df, x='Year-Month', y='Carbon Impact CPU (tonnes CO₂eq)')




#%%
##########################
#  Fuzzy Matching
##########################

# Testing with small dataset

# Build and clean dataset
data = {'Name': ['Comp1', 'Comp2', 'Comp3'], 'CPU_TDP':[3, 0.0, 0.0],
        'Processor':['AMD EPYC 7V12 48C 2.45GHz','AMD EPYC7V12 60C 2.45GHz' ,
                     'AMD 7V12']}
playData = m.pd.DataFrame(data = data)
playData.replace('N/A', m.np.nan)

# get values to track if fuzzy match works
outDf = playData.copy(deep=True)
inputLen = len(outDf)
lenMissing = len(outDf[outDf['CPU_TDP']==0.0].copy())

print('Input dataframe length: ', inputLen)
print(f'Currently missing {lenMissing} TDP values')
# show data before fuzzy
playData['Initial TDP'] = playData['CPU_TDP']
print(playData)
# Apply fuzzy
bdf.apply_fuzzy_cpu_match(df = outDf, verbose=True)
# show data after fuzzy

# Check that TDP values filled in properly
outDf_fix = outDf[outDf['Estimated TDP']==0.0].copy()
print(f'Num TDP values missing after match: {len(outDf_fix)} out\
of {inputLen} missing\
({len(outDf_fix)/inputLen*100})%')

outDf

#%%
#Testing with full dataset

# Initial check of missing values, all na have been set to 0.0
missing_CPU_info = big_set[big_set['CPU_TDP']==0.0].copy()
#this is almost half missing (48.82%) 
#   --> join using lowercase drops this to 46.81%
print(f'{len(missing_CPU_info)} out of {len(big_set)} missing\
       ({len(missing_CPU_info)/len(big_set)*100} %)')

# Copy dataset to apply fuzzy match
ds = big_set.copy(deep=True)

# Apply fuzzy match to fill in missing TDPs in new column
      # if no TDP in initial dataset, uses fuzzy to find a suitable
      # one, otherwise keep initial TDP
bdf.apply_fuzzy_cpu_match(df = ds, verbose=True)

# Now none are missing
missing_CPU_info = ds[ds['Estimated TDP']==0.0].copy()
print(f'{len(missing_CPU_info)} out of {len(ds)} missing\
       ({len(missing_CPU_info)/len(ds)*100} %)')

#%%
a = bdf.cores_computers_elec_full.copy()
a.groupby('HashedID').first().filter(regex='CPU|GPU|Accelerator')
#%%
