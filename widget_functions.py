###################################################
#
#              WIDGET FUNCTIONS
#
###################################################
#TODO: Adjust carbon intensity values to reflect GWh rather than KWh


"""
WIDGET FUNCTIONS FOR DASHBOARD
===========================================================

Defines key functions for visualizing and analyzing the carbon 
impact and electricity consumption of supercomputers by country
using interactive widgets.

Visualizations allow comparison between countries and global averages.

Key Functions:
--------------

**byCountryComparison** : Plots a line chart comparing carbon intensities
    by year using Ember and both life cycle and direct Electricity Maps 
    data for a given country to compare the two sources

**create_update_TDP_graphs** : Constructs a dual-subplot of Energy consumption
    and carbon impact (total/CPU/GPU) over time for a selected country and
    compares it to global averages.

**update_display** : Generates descriptive statistics and comparison metrics
    (text widgets) based on selected filtering and modeling parameters.UNFINISHED

Interactive Outputs:
--------------------
- outTotals:Binds widget inputs to the `create_update_TDP_graphs` 
            visualization function.
- outText:  Binds widget inputs to the `update_display` function
            for displaying text summaries.

Dependencies:
-------------
- utilities
- project_lib
- widgets
- dataset_constuctor_functions
- build_dfs

Main DataFrames Used:
---------------------
- cores_computers_complete_df: Full dataset of systems with 
    electricity and carbon data.
- cores_computers_elec_minimal_df: A reduced version of the above for 
    lightweight operations.

Notes:
------
- The functions assume all inputs come from predefined interactive widgets.
"""
import project_lib as m
import utilities as u
import widgets as w
import dataset_constuctor_functions as dcf
import build_dfs as builddfs

cores_computers_complete_df = (builddfs.cores_computers_elec_full
                                        .copy(deep=True))
cores_computers_elec_minimal_df = (builddfs.minimal_cores_computers_elec_full
                                            .copy(deep=True))

def byCountryComparison(Country:str, 
                        in_df:m.pd.DataFrame = cores_computers_complete_df):
    """
    Creates a line graph comparing annual average carbon intensity values across 
    three data sources 
        (Ember, Electricity Maps - Direct, Electricity Maps - Life Cycle)
    for a given country.

    Args:
        Country (str) :The name of the country for which to visualize the data.
        in_df (pd.DataFrame) :
            Input dataframe containing carbon intensity data. Defaults to 
            cores_computers_complete_df. Must have columns:
                Ember Carbon Intensity gCO₂eq/kWh,
                Electricity Maps Carbon Intensity gCO₂eq/kWh, 
                Carbon Intensity gCO₂eq/kWh (direct),
                Country,
                Year

    Returns:
        plotly.graph_objs._figure.Figure displaying the carbon impact factor 
        for each source over time.
    """
    #Filter country and set up data
    df = in_df.copy(deep=True)
    df = df[df["Country"] == Country]
    df["Ember_year_average"]= (df.groupby('Year')
                               ['Ember Carbon Intensity gCO₂eq/kWh']
                                 .transform('mean'))
    df["ElectricityMaps_year_average_lifeCycle"]= (df.groupby('Year')
                        ["Electricity Maps Carbon Intensity gCO₂eq/kWh"]
                                                     .transform('mean'))
    df["ElectricityMaps_year_average_direct"]= (df.groupby('Year')
                                    ["Carbon Intensity gCO₂eq/kWh (direct)"]
                                                  .transform('mean'))
    df = df.drop_duplicates(subset="Year")
    df = df[df["Year"]>=2021]
    df = df.sort_values('Year')

    #Create base figure to add traces
    fig = m.go.Figure()
    
    fig.add_trace(m.go.Scatter(x=df["Year"], y = df['Ember_year_average'],
                    mode='lines+markers', name='Ember',
                    hovertemplate='%{x:.0f} : %{y:.2f} gCO₂/GWh'))
    fig.add_trace(m.go.Scatter(x=df['Year'], 
                    y = df['ElectricityMaps_year_average_lifeCycle'],
                    mode='lines+markers', name='Electricity Maps Life Cycle',
                    hovertemplate='%{x:.0f} : %{y:.2f} gCO₂/GWh'))
    fig.add_trace(m.go.Scatter(x=df['Year'], 
                    y = df['ElectricityMaps_year_average_direct'],
                    mode='lines+markers', name='Electricity Maps Direct',
                    hovertemplate='%{x:.0f} : %{y:.2f} gCO₂/GWh'))
    fig.update_layout(
        title=f"Carbon Intensity Comparison en {Country} (Moyenne Annuelle)",
        xaxis_title="Year",
        yaxis_title="gCO₂/GWh",
        template='plotly'
    )
    return fig

def create_update_TDP_graphs(alpha: float, tau: tuple, country: str, 
                        source: str, sector:tuple, 
                        listing:str, time_after_drop:float,
                        in_df: m.pd.DataFrame = cores_computers_complete_df):
    """
    Generates a dual subplot graph showing 1) monthly Energy consumption and 2) 
    carbon impact for CPU, GPU, and total usage across time. 
    Compares country-specific data to global averages.

    Args:
        alpha (float) : Proportion of max Energy used when system is at idle
        tau (tuple) : A tuple of the lower and upper
                      bounds of the rate of utilisation 
        country (str) : selected country
        source (str) : carbon intensity data source 
                       ("Ember" or "Electricity Maps")
        sector (str) : selected sector filter
        listing (str) : Listing category filter
        in_df (pd.DataFrame) : Input dataset of hardware and 
                               carbon intensity data.
                               Defaults to cores_computers_complete_df.

    Returns:
        None

    Dependencies:
        filter_df
        construct_energy_and_carbon_impact_df
        deci_yr_col_to_datetime
        cores_computers_complete_df dataframe from build_dfs
    """
    working_df = dcf.filter_df(df=in_df, country_name=country, 
                               segment_name=sector,
                               listing_name=listing, 
                               carbon_choice=source,
                               verbose=m.VERBOSE_VAL)
    
    if working_df.empty:
        fig = m.go.Figure()
        fig.add_annotation(
            text="No data available for the selected parameters.",
            font=dict(size=20)
        )
        return fig
    #Create dataframes for global averages
    global_working_df = dcf.filter_df(df=cores_computers_complete_df, 
                                      country_name='Somme de Tous', 
                                      segment_name= sector,
                                      listing_name=listing, 
                                      carbon_choice=source, 
                                      verbose=m.VERBOSE_VAL)

    energy_country = dcf.construct_energy_and_carbon_impact_df(alpha=alpha, 
                                                               tau=tau,
                                            time_after_drop = time_after_drop,
                                            carbon_choice = source,
                                            in_df=working_df,
                                            verbose=m.VERBOSE_VAL)
    energy_country['Year-Month'] = u.deci_yr_col_to_datetime(
                                            energy_country['semester'])

    #Compute global average to graph as a point of comparison
    energy_global = dcf.construct_energy_and_carbon_impact_df(alpha=alpha, 
                                            tau=tau, 
                                            time_after_drop = time_after_drop,
                                            carbon_choice=source,
                                            in_df=global_working_df, 
                                            verbose=m.VERBOSE_VAL)
    
    # Get factors and set up for global average
    avg_factor = len(in_df['Country'].unique())
    energy_global['Year-Month'] = u.deci_yr_col_to_datetime(
        energy_global['semester'])
    energy_global = energy_global.filter(
        regex="Energy|tonnes|semester|Year-Month")

    # average columns
    energy_cols = [col for col in energy_global.columns 
                   if 'Energy' in col or 'tonnes' in col]
    energy_global[energy_cols] = (energy_global[energy_cols]
                                  .apply(lambda x: x / avg_factor))
    # #################################################################3
    #     # 1. Label sources
    # energy_country['Source'] = country
    # energy_global['Source'] = 'Global Average'

    # # 2. Concatenate both
    # combined_df = m.pd.concat([energy_country, energy_global])

    # # 3. Melt into long format
    # long_df = m.pd.melt(
    #     combined_df,
    #     id_vars=['Year-Month', 'Source', 'Systems Added', 
    #              'Systems Removed', 'Active Systems'],
    #     value_vars=[
    #         'Total Energy Consumption (Monthly)',
    #         'CPU Energy Consumption (Monthly)',
    #         'GPU Energy Consumption (Monthly)',
    #         'Carbon Impact Total (tonnes CO₂eq)',
    #         'Carbon Impact CPU (tonnes CO₂eq)',
    #         'Carbon Impact GPU (tonnes CO₂eq)',
    #     ],
    #     var_name='Metric',
    #     value_name='Value'
    # )

    # # 4. Add 'Type' and 'Component' columns for plotting logic
    # long_df['Type'] = long_df['Metric'].apply(
    #     lambda x: 'Energy Consumption (GWh)' if 'Energy' in x
    #                else 'Carbon Impact (tonnes CO₂eq)'
    # )
    # long_df['Component'] = long_df['Metric'].apply(
    #     lambda x: 'Total' if 'Total' in x else ('CPU' if 'CPU' in x else 'GPU')
    # )

    # # 5. Plot with Plotly Express
    # fig = m.px.line(
    #     long_df,
    #     x='Year-Month',
    #     y='Value',
    #     line_dash='Source',  # Solid for country, dashed for global
    #     color='Component',
    #     color_discrete_sequence= m.px.colors.qualitative.Pastel,
    #     hover_data=['Systems Added', 'Systems Removed', 'Active Systems'],
    #     title='CPU + GPU Energy Consumption and Carbon Impact over Time'
    # )

    # # 6. Make it look like your original plot
    # fig.update_traces(mode='lines+markers')

    # # Optional: adjust dash opacity
    # for trace in fig.data:
    #     if 'Global Average' in trace.name:
    #         trace.update(opacity=0.55)

    # # 7. Clean up layout
    # fig.update_layout(
    #     title_x=0.5,
    #     title_y=0.9,
    #     margin=dict(t=80, b=50, l=50, r=50),
    #     legend_title_text=''
    # )
    # fig.for_each_annotation(lambda a: a.update(text=a.text.replace("Type=", "")))
    # fig.update_yaxes(matches=None)
    # fig.update_xaxes(title='Month')

    # fig.show()  
    # #################################################################3
    
    # Base plot to visualize data
    fig = m.make_subplots(rows = 1, cols = 2)

    #Add traces for Total data
    fig.add_trace(m.go.Scatter(x=energy_country['Year-Month'], 
                        y=energy_country['Total Energy Consumption (Monthly)'],
                        mode='lines+markers', 
                        name=f'{country} Total Energy Consumption (GWh)',
                        line_color = 'blue', 
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>')),
                        row=1, col=1)
    
    fig.add_trace(m.go.Scatter(x=energy_country['Year-Month'], 
                        y=energy_country['Carbon Impact Total (tonnes CO₂eq)'],
                        mode='lines+markers', 
                        name=f'{country} Total Carbon Impact (tonnes CO₂eq)', 
                        line_color = 'blue',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>')), 
                        row=1, col=2)
    
    #Add traces for CPUs
    fig.add_trace(m.go.Scatter(x=energy_country['Year-Month'], 
                        y=energy_country['CPU Energy Consumption (Monthly)'],
                        mode='lines+markers', 
                        name=f'{country} CPU Energy Consumption (GWh)',
                        line_color = 'green',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>')),
                        row=1, col=1)
    fig.add_trace(m.go.Scatter(x=energy_country['Year-Month'], 
                        y=energy_country['Carbon Impact CPU (tonnes CO₂eq)'],
                        mode='lines+markers', 
                        name=f'{country} CPU Carbon Impact (tonnes CO₂eq)',
                        line_color = 'green',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>')), 
                        row=1, col=2)
    

    #Add traces for GPUs
    fig.add_trace(m.go.Scatter(x=energy_country['Year-Month'], 
                        y=energy_country['GPU Energy Consumption (Monthly)'],
                        mode='lines+markers', 
                        name=f'{country} GPU Energy Consumption (GWh)',
                        line_color = 'orange',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>')), 
                        row=1, col=1)
    fig.add_trace(m.go.Scatter(x=energy_country['Year-Month'], 
                        y=energy_country['Carbon Impact GPU (tonnes CO₂eq)'],
                        mode='lines+markers', 
                        name=f'{country} GPU Carbon Impact (tonnes CO₂eq)',
                        line_color = 'orange',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>')), 
                        row=1, col=2)
    

    # Global values
    fig.add_trace(m.go.Scatter(x=energy_global['Year-Month'],
                        y=energy_global['Carbon Impact Total (tonnes CO₂eq)'],
                        mode='lines', 
                        name='Global Average Total Carbon Impact (tonnes CO₂eq)',
                        line_color = 'blue',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>'), 
                        line_dash = 'dash',  
                        opacity = 0.55), 
                        row=1, col=2)
    
    fig.add_trace(m.go.Scatter(x=energy_global['Year-Month'],
                        y=energy_global['CPU Energy Consumption (Monthly)'],
                        mode='lines', 
                        name='Global Average CPU Energy Consumption (GWh)',
                        line_color = 'green',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>'), 
                        line_dash = 'dash',  
                        opacity = 0.55), 
                        row=1, col=1)
    
    fig.add_trace(m.go.Scatter(x=energy_global['Year-Month'], 
                        y=energy_global['Carbon Impact CPU (tonnes CO₂eq)'],
                        mode='lines', 
                        name='Global Average CPU Carbon Impact (tonnes CO₂eq)',
                        line_color = 'green',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>'), 
                        line_dash = 'dash',  
                        opacity = 0.55), 
                        row=1, col=2)
    
    fig.add_trace(m.go.Scatter(x= energy_global['Year-Month'], 
                        y=energy_global['Total Energy Consumption (Monthly)'], 
                        mode='lines', 
                        name='Global Average Total Energy Consumption (GWh)', 
                        line_color = 'blue',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>'), 
                        line_dash = 'dash',  
                        opacity = 0.55), 
                        row=1, col=1)
    
    fig.add_trace(m.go.Scatter(x=energy_global['Year-Month'], 
                        y=energy_global['GPU Energy Consumption (Monthly)'],
                        mode='lines', 
                        name='Global Average GPU Energy Consumption (GWh)',
                        line_color = 'orange',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>'), 
                        line_dash = 'dash',  
                        opacity = 0.55), 
                        row=1, col=1)
    
    fig.add_trace(m.go.Scatter(x=energy_global['Year-Month'], 
                        y=energy_global['Carbon Impact GPU (tonnes CO₂eq)'],
                        mode='lines', 
                        name='Global Average GPU Carbon Impact (tonnes CO₂eq)',
                        line_color = 'orange',
                        customdata=energy_country[['Systems Added', 
                                                   'Systems Removed', 
                                                   'Active Systems']].values,    
                        hovertemplate=(
                            '%{x}<br>' +
                            'Energy: %{y:.2f} GWh<br>' +
                            'Systems Added: %{customdata[0]}<br>' +
                            'Systems Removed: %{customdata[1]}<br>' +
                            'Active Systems: %{customdata[2]}<br>' +
                            '<extra></extra>'), 
                        line_dash = 'dash',  
                        opacity = 0.55),
                        row=1, col=2)
    

    fig.update_layout(
        title_text="CPU + GPU Energy Consumption and Carbon Impact over Time",
        title_x=0.5,
        title_y=0.9,
        xaxis_title="Semester",
        yaxis_title="Energy Consumption (GWh)")

    fig.update_xaxes(title_text = 'Month', row=1, col=2)
    fig.update_yaxes(title_text = 'Carbon Impact tonnes CO₂eq', row=1, col=2)

    fig.show()
    return

# TODO: Revisit this?

# def update_display (country:str, source:str, alpha:float, tau:tuple, 
#                     listing:str, sector:tuple, time_after_drop:float, 
#                     in_df:m.pd.DataFrame = cores_computers_complete_df):

#     """
#     Updates text widgets to display summary data
#     and compares to global averages.

#     Args:
#       country (str) : The selected country for display
#       source (str) : The carbon intensity data source ("Electricity Maps" or "Ember")
#       alpha (float) : Proportion of max Energy used when system is at idle
#       tau (tuple) : A tuple of the lower and upper bounds of the rate of utilisation 
#       listing (str) : Listing category filter
#       sector (str) : Sector filter
#       in_df (pd.DataFrame) : Input dataset to filter and analyze. 
#                                Defaults to cores_computers_complete_df

#     Returns
#         None after updating widgets.
#     """
#     # save source (source choice for carbon impact data) as the correct
#     #    string column name
#     if source == 'Electricity Maps':
#         source = 'Electricity Maps Carbon Intensity gCO₂eq/kWh'
#     else:
#         source = 'Ember Carbon Intensity gCO₂eq/kWh'

#     # Filter dataframe to be used to calculate carbon impact and electricity consumption
#     working_df = dcf.filter_df(df=in_df, 
#                                country_name=country, 
#                                segment_name=sector, 
#                                listing_name=listing, 
#                                carbon_choice=source, 
#                                verbose=m.VERBOSE_VAL)
    
#     if working_df.empty:
#         w.infos1.value = "No data available with the selected parameters."
#         return 0

#     # Build electricity consumption and carbon impact dataframes for the specified country
#     power_country = dcf.power_consumption_construct_df(alpha=alpha,
#                                                        tau=tau,
#                                                        time_after_drop = time_after_drop,
#                                                        in_df=working_df, 
#                                                        verbose=m.VERBOSE_VAL)
#     carbon_impact_country = dcf.carbon_impact(pow_df=power_country, 
#                                               carbon_df=working_df, 
#                                               carbon_choice=source, 
#                                               verbose=m.VERBOSE_VAL)

#     power_country = power_country[power_country['Total Power Consumption (Monthly)'] > 0]
#     carbon_impact_country = (carbon_impact_country[
#                              carbon_impact_country['Total Power Consumption (Monthly)'] > 0])


#     #Create dataframes to obtain global averages
#     global_working_df = dcf.filter_df(df=cores_computers_complete_df, 
#                                       country_name='Somme de Tous', 
#                                       segment_name=sector,
#                                       listing_name=listing, 
#                                       carbon_choice=source, 
#                                       verbose=m.VERBOSE_VAL)
#     # Build electricity consumption and carbon impact dataframes for all countries
#     power_global = dcf.power_consumption_construct_df(alpha=alpha, tau=tau,
#                                                       time_after_drop = time_after_drop,
#                                                       in_df=global_working_df, 
#                                                       verbose=m.VERBOSE_VAL)
#     carbon_global = dcf.carbon_impact(pow_df=power_global, 
#                                       carbon_df=global_working_df, 
#                                       carbon_choice=source, 
#                                       verbose=m.VERBOSE_VAL)
    
#     # Scale the summed values in the global dataframe to reflect the average power use and carbon impact
#         # allows for a metric agianst which to compare that country's consumption
#     power_cols = ['CPU Power Consumption (Monthly)', 
#                   'GPU Power Consumption (Monthly)', 
#                   'Total Power Consumption (Monthly)']
#     carbon_cols = ['Carbon Impact CPU (tonnes CO₂eq)', 
#                    'Carbon Impact GPU (tonnes CO₂eq)', 
#                    'Carbon Impact Total (tonnes CO₂eq)']
#     # num calcs x num semester   
#     avg_factor = len(global_working_df)

#     global_avg = carbon_global.groupby('Year-Month')[
#         power_cols + carbon_cols
#     ].sum().reset_index().apply(lambda x: x / avg_factor if x.name in power_cols + carbon_cols else x)
#     #  if x.name in power_cols + carbon_cols else x --> probably not necessary

#     # Reassign global variables
#     power_global = global_avg[['Year-Month'] + power_cols]
#     carbon_global = global_avg[['Year-Month'] + carbon_cols]
#     # Create date limitations for a more descriptive comparison metric 
#         #Values for date limitations:
#     min_year = m.pd.to_datetime(power_country['Year-Month'].min())
#     max_year = m.pd.to_datetime(power_country['Year-Month'].max())

#     power_global = power_global[(power_global['Year-Month'] >= min_year) & (power_global['Year-Month'] <= max_year)]
#     carbon_global = carbon_global[(carbon_global['Year-Month'] >= min_year) & (carbon_global['Year-Month'] <= max_year)]

#     elecVal = power_country['Total Power Consumption (Monthly)'].sum()
#     carbonVal = carbon_impact_country['Carbon Impact Total (tonnes CO₂eq)'].sum()
    
#     # TODO: Calculate time period
#     time = (max_year.year - min_year.year) + 1

#     elecVal_global_avg = (power_global['Total Power Consumption (Monthly)']
#                           .sum()) * time * 12
#     carbonVal_global_avg = (carbon_global['Carbon Impact Total (tonnes CO₂eq)']
#                             .sum()) * time * 12

#     if country == 'Somme de Tous':
#         number_computers = len(cores_computers_complete_df)

#         w.infos1.value = f"Ces données représentent <b>{number_computers} supercalculs</b> qu'ont utilisé <b>{elecVal:,.2E} GW</b>\
#                         avec un inpact carbone de <b>{carbonVal:,.2E} tonnes CO₂eq</b> chaque mois"

#         w.infos2.value = f"Sur {time} ans, ces {number_computers} supercalculs ont utilisé {(elecVal * 12 * time):,.2E} \
#                         GWh avec un impact carbone de {carbonVal * 12 * time:,.2E} tonnes CO₂eq"
#         w.infos3.value = " "
#         w.infos4.value = " "
#     else:
#         number_computers = len(cores_computers_complete_df[cores_computers_complete_df['Country'] == country])
   
#         w.infos1.value = f"Ces données représentent <b>{number_computers} supercalculs</b> qu'ont utilisé <b>{elecVal:,.2E} GW</b>\
#                         avec un inpact carbone de <b>{carbonVal:,.2E} tonnes CO₂eq</b> chaque mois"

#         w.infos2.value = f"Sur {time} ans, ces {number_computers} supercalculs ont utilisé {(elecVal * 12 * time):,.2E} \
#                         GWh avec un impact carbone de {carbonVal * 12 * time:,.2E} tonnes CO₂eq"
        
#         w.infos3.value =  f"Globalement, les supercalculs utilisent en moyenne \
#                         {elecVal_global_avg:,.2E}\
#                         GWh pendant les mêmes {time} années avec un impact carbone de \
#                         {carbonVal_global_avg:,.2E} tonnes CO₂eq"
        
#         #TODO: Verify that figures here are correct --> The outputs feel wrong
#         w.infos4.value = f"{country} utilise {(elecVal/elecVal_global_avg):,.2f} fois \
#                         le consommation de puissance globale moyenne totale avec un impact de {(carbonVal/carbonVal_global_avg):,.2f}\
#                             fois le moyenne globale"
#     return None
      
# Create interactive bits, connecting widgets to the functions 
# and preparing functions to be used in the display
outTotals = m.widgets.interactive_output(create_update_TDP_graphs,
    {'tau': w.tau_slider, 
     'alpha': w.alpha_slider, 
     'source': w.carbon_choice, 
     'country': w.country_choice,
     'sector': w.sector_choice,
     'listing': w.listing_choice,
     'time_after_drop' : w.lifespan_estimation})

# outText = m.widgets.interactive_output(update_display,
#      {'country': w.country_choice,
#       'source': w.carbon_choice,
#       'alpha': w.alpha_slider,
#       'tau': w.tau_slider,
#       'listing': w.listing_choice,
#       'sector': w.sector_choice,
#       'time_after_drop' : w.lifespan_estimation})
