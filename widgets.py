###################################################
#
#             INITIALIZE WIDGETS
#
###################################################
"""
Module for initializing the interactive widgets used in the dashboard.

Widgets:
    - tau_slider (FloatRangeSlider): Set range of Tau constant to describe 
                                     the proportion of maximum TDP used by the
                                     CPUs and GPUs when computer is in use.
    - alpha_slider (FloatSlider): Adjusts the proportion of power used at idle.
    - carbon_choice (RadioButtons): Allows selection of the source of data used
                                    for electricity impact calculations
    - country_choice (Dropdown): Filters data by country; includes option for 
                                 sum of all countries
    - sector_choice (SelectMultiple): Multiple selection of sectors to be 
                                      included in the visualization
    - listing_choice (Dropdown): Choice of Green500, Top500, or both listings 
                                 to be included in the visualization
    - lifespan estimation (Float Input): Value to use as the lifespan of 
                                         systems which 'fall through' the list

Additional Info Widgets:
    - infos1, infos2, infos3, infos4 (HTML): additional information displays

Usage:
    Import this module to initialize and configure the widgets for use in the 
    dashboard application. The widgets can be integrated into interactive 
    visualizations to provide dynamic data filtering and customization.
"""
import project_lib as mod
from build_dfs import cores_computers_elec_full

# build widgets to appear on dashboard

# #tau: Taux d'utilisation
tau_slider = mod.widgets.FloatRangeSlider(value = [0.5, 0.8], 
                                          min=0, max=1, step = 0.01,
                                          description='Tau', 
                                          readout = True, 
                                          readout_format='.2f',
                                          continuous_update=False)

# #alpha: coefficient de percentage de puissance utilisée à idle
alpha_slider = mod.widgets.FloatSlider(min=0, max=1, value=.5, 
                                       description='Alpha', 
                                       continuous_update=False)

# choice of dataset to use for electricity impact
carbon_choice = mod.widgets.RadioButtons(
    options=['Electricity Maps', 'Ember'],
    value='Electricity Maps',
    description='Carbon Factor Source',
    disabled=False)

# # Country Selection
    # get Country list for dropdown
country_list = cores_computers_elec_full['Country'].unique().tolist()
country_list.sort()

    # Choice to filter by country
country_choice = mod.widgets.Dropdown(
    options=['Somme de Tous'] + country_list,
    value='Somme de Tous',
    description='Pays',
    disabled=False)

# Choice of sector(s) to visualize
  #TODO: add graph which shows them all on the same graph with their own lines
sector_choice = mod.widgets.SelectMultiple(
    options= cores_computers_elec_full['Segment'].unique().tolist(),
    value= tuple(cores_computers_elec_full['Segment'].unique().tolist()),
    description='List',
    disabled=False)

# # Choose listing green or top500 or both
listing_choice = mod.widgets.Dropdown(
    options=['Green500', 'Top500', 'Tous'],
    value='Tous',
    description='Listing',
    disabled=False)

# Value for how long systems are in use after 
# dropping past the bottom of the list
lifespan_estimation = mod.widgets.BoundedFloatText(
    value = 5.0,
    min = 0.0,
    max = 35.0,
    step = 0.5,
    description = "Années après depassant le fond de la liste",
    style={'description_width': 'initial'},
)

# Info widgets
infos1 = mod.widgets.HTML()
infos2 = mod.widgets.HTML()
infos3 = mod.widgets.HTML()
infos4 = mod.widgets.HTML()