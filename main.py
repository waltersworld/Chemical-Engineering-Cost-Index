import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from fredapi import Fred
import time
import fredseries

fred_key = '46f9c73428a0be4b7d3fd21f548d2444'
fred = Fred(api_key=fred_key)

# Get timeseries data from FRED
def getFred(series):
    series_results = []

    for series_name in series:
        series_data = fred.get_series(series_name)
        series_data = series_data.to_frame(name=series_name)
        series_results.append(series_data)
        time.sleep(0.25)
        
    series_df = pd.concat(series_results, axis=1)
    series_df = series_df.mean(axis=1)
    series_df = series_df.divide(series_df.iloc[0])*100
    
    return series_df

# Clean component indices
def generateComponents(df):
    df = df.dropna()
    df = df.sort_index()
    df = df.iloc[-120:]
    df = df.divide(df.iloc[0])*100

    return df

# Calculate the weighted average
def generateSubindex(dataframes, weights):
    dfs = {key: getFred(value) for key, value in dataframes.items()}
    concatenated_df = pd.concat(dfs, axis=1)
    processed_df = generateComponents(concatenated_df)
    weighted_sum = (processed_df * weights).sum(axis=1)
    index_df = pd.DataFrame(weighted_sum.iloc[-120:] / weighted_sum.iloc[0] * 100)
    
    return index_df
    
# Loop categories and weights through generateSubindex from dict
def processSubindex(categories):
    indexes = {}
    for category, values in categories.items():
        index = generateSubindex(values['dataframes'], values['weights'])
        indexes[category] = index
    
    return indexes

# Define subindex categories and weights
categories = {
    'Heat Exchangers': {'dataframes': {
        'Plates': fredseries.he_plates, 
        'Tubing': fredseries.he_tubing, 
        'Nonferrous': fredseries.he_nonferrous, 
        'Alloy_plates': fredseries.he_alloy_plates, 
        'Hot_rolled_shapes': fredseries.he_hot_rolled_shapes, 
        'Tanks': fredseries.he_tanks}, 'weights': [0.25, 0.086, 0.043, 0.11, 0.11, 0.175]},
    'Process Machinery': {'dataframes': {
        'Plates': fredseries.pm_plates, 
        'Hot Rolled': fredseries.pm_hr_sheets, 
        'Cold Rolled': fredseries.pm_cr_sheets, 
        'Foundry Products': fredseries.pm_foundry_forge, 
        'Machine Handling Equipment': fredseries.pm_mh_equipment, 
        'Fans and Blowers': fredseries.pm_fans_blowers, 
        'Motors and Generators': fredseries.pm_motors, 
        'Crushers and Pulverizers': fredseries.pm_crushers, 
        'Concrete Ingredients': fredseries.pm_concrete}, 'weights': [0.105, 0.030, 0.010, 0.060, 0.075, 0.025, 0.035, 0.150, 0.030]},
    'Pipes and Valves': {'dataframes': {
        'Plastic': fredseries.pv_plastic, 
        'Pipe': fredseries.pv_pipe, 
        'Tubing': fredseries.pv_tubing, 
        'Copper and Brass': fredseries.pv_copper, 
        'Concrete Ingredients': fredseries.pv_concrete, 
        'Valves': fredseries.pv_valves, 
        'Fittings': fredseries.pv_fittings}, 'weights': [0.050, 0.400, 0.100, 0.100, 0.050, 0.200, 0.100]},
    'Process Instruments': {'dataframes': {
        'Hot Rolled': fredseries.pi_hot_rolled, 
        'Cold Rolled': fredseries.pi_cold_rolled, 
        'Foundry Products': fredseries.pi_foundry, 
        'Nonferrous': fredseries.pi_nonferrous, 
        'Copper and Brass': fredseries.pi_copper, 
        'Motors and Generators': fredseries.pi_motors, 
        'Electrical Components': fredseries.pi_elec, 
        'Valves': fredseries.pi_valves}, 'weights': [0.057, 0.014, 0.077, 0.060, 0.053, 0.036, 0.400, 0.053]},
    'Pumps and Compressors': {'dataframes': {
        'Pumps': fredseries.pc_pumps, 
        'Air Compressors': fredseries.pc_air_compressors, 
        'Gas Compressors': fredseries.pc_gas_compressors, 
        'Vacuum Pumps': fredseries.pc_vacuum}, 'weights': [0.900, 0.050, 0.025, 0.025]},
    'Electrical Equipment': {'dataframes': {
        'Wire': fredseries.ee_wire, 
        'Lighting': fredseries.ee_lighting, 
        'Motors': fredseries.ee_motors, 
        'Transformers': fredseries.ee_transformers, 
        'Switchgear': fredseries.ee_switchgear}, 'weights': [0.057, 0.188, 0.349, 0.146, 0.260]},
    'Miscellaneous': {'dataframes': {
        'Paint': fredseries.misc_paint, 
        'Hot Rolled': fredseries.misc_hot_rolled, 
        'Rebar': fredseries.misc_rebar, 
        'Concrete': fredseries.misc_concrete, 
        'Insulation': fredseries.misc_insulation}, 'weights': [0.024, 0.406, 0.089, 0.129, 0.353]},
    'Buildings': {'dataframes': {
        'Construction Materials': fredseries.construction_materials, 
        'General Building Contractors': fredseries.general_building_contractors}, 'weights': [0.53, 0.47]},
    'Supervision': {'dataframes': {
        'Administrative': fredseries.admin, 
        'Engineering': fredseries.engineering, 
        'Design and Drafting': fredseries.design, 
        'Managerial Support': fredseries.managerial}, 'weights': [0.06, 0.325, 0.395, 0.220]},
    'Construction Labor': {'dataframes': {
        'General Contractors': fredseries.general_contractors, 
        'Heavy Contractors': fredseries.heavy_contractors, 
        'Specialty Contractors': fredseries.specialty_contractors}, 'weights': [0.467, 0.317, 0.217]}
}

all_indexes = processSubindex(categories)

all_df = pd.concat(all_indexes.values(), axis=1, keys=all_indexes.keys())

# Add date column
all_df['Date'] = all_df.index
all_df.reset_index(drop=True, inplace=True)

# Load dataframes into MySQL
engine = create_engine('mysql+pymysql://root:password@localhost:3306/CE_INDEX')
conn = engine.connect()

all_df.to_sql('Chemical Engineering Cost Index', conn, if_exists='replace', index=False)
