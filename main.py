import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
from sqlalchemy import create_engine
from fredapi import Fred
import time

# Import individual index components
import fredseries

plt.style.use('fivethirtyeight')
pd.options.display.max_columns = 500
color_pal = plt.rcParams["axes.prop_cycle"].by_key()["color"]

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

# Generate subindex
def generateSubIndex(df, weights):
    df = pd.DataFrame((df * weights).sum(axis=1))
    df = df.divide(df.iloc[0])*100

    return df

# Heat Exchangers
plates_df = getFred(fredseries.he_plates)
tubing_df = getFred(fredseries.he_tubing)
nonferrous_df = getFred(fredseries.he_nonferrous)
alloy_plates_df = getFred(fredseries.he_alloy_plates)
hot_rolled_shapes_df = getFred(fredseries.he_hot_rolled_shapes)
tanks_df = getFred(fredseries.he_tanks)

heatx_df = pd.concat({
    'Plates': plates_df,
    'Tubing': tubing_df,
    'Nonferrous': nonferrous_df,
    'Alloy_plates': alloy_plates_df,
    'Hot_rolled_shapes': hot_rolled_shapes_df,
    'Tanks': tanks_df
}, axis=1)

heatx_df = generateComponents(heatx_df)

weights = [0.25, 0.086, 0.043, 0.11, 0.11, 0.175]
heatx_index = pd.DataFrame((heatx_df * weights).sum(axis=1))
heatx_index = heatx_index.iloc[-120:]
heatx_index = heatx_index.divide(heatx_index.iloc[0])*100

# Process Machinery
pm_plates_df = getFred(fredseries.pm_plates)
pm_hot_rolled_df = getFred(fredseries.pm_hr_sheets)
pm_cold_rolled_df = getFred(fredseries.pm_cr_sheets)
pm_foundry_df = getFred(fredseries.pm_foundry_forge)
pm_mh_equip_df = getFred(fredseries.pm_mh_equipment)
pm_fans_df = getFred(fredseries.pm_fans_blowers)
pm_motors_df = getFred(fredseries.pm_motors)
pm_crushers_df = getFred(fredseries.pm_crushers)
pm_concrete_df = getFred(fredseries.pm_concrete)

process_machinery_df = pd.concat({
    'Plates': pm_plates_df,
    'Hot Rolled': pm_hot_rolled_df,
    'Cold Rolled': pm_cold_rolled_df,
    'Foundry Products': pm_foundry_df,
    'Machine Handling Equipment': pm_mh_equip_df,
    'Fans and Blowers': pm_fans_df,
    'Motors and Generators': pm_motors_df,
    'Crushers and Pulverizers': pm_crushers_df,
    'Concrete Ingredients': pm_concrete_df
}, axis=1)

process_machinery_df = generateComponents(process_machinery_df)

pm_weights = [0.105, 0.030, 0.010, 0.060, 0.075, 0.025, 0.035, 0.150, 0.030]

process_machinery_index = pd.DataFrame((process_machinery_df * pm_weights).sum(axis=1))
process_machinery_index = process_machinery_index.iloc[-120:]
process_machinery_index = process_machinery_index.divide(process_machinery_index.iloc[0])*100

# pipes, valves and fittings
pv_plastic_df = getFred(fredseries.pv_plastic)
pv_pipe_df = getFred(fredseries.pv_pipe)
pv_tubing_df = getFred(fredseries.pv_tubing)
pv_copper_df = getFred(fredseries.pv_copper)
pv_concrete_df = getFred(fredseries.pv_concrete)
pv_valves_df = getFred(fredseries.pv_valves)
pv_fittings_df = getFred(fredseries.pv_fittings)

pv_df = pd.concat({
    'Plates': pv_plastic_df,
    'Pipe': pv_pipe_df,
    'Tubing': pv_tubing_df,
    'Copper and Brass': pv_copper_df,
    'Concrete Ingredients': pv_concrete_df,
    'Valves': pv_valves_df,
    'Fittings': pv_fittings_df,
}, axis=1)

pv_df = generateComponents(pv_df)

pv_weights = [0.050, 0.400, 0.100, 0.100, 0.050, 0.200, 0.100]

pv_index = pd.DataFrame((pv_df * pv_weights).sum(axis=1))
pv_index = pv_index.iloc[-120:]
pv_index = pv_index.divide(pv_index.iloc[0])*100

# Process instruments
pi_hot_rolled_df = getFred(fredseries.pi_hot_rolled)
pi_cold_rolled_df = getFred(fredseries.pi_cold_rolled)
pi_foundry_df = getFred(fredseries.pi_foundry)
pi_nonferrous_df = getFred(fredseries.pi_nonferrous)
pi_copper_df = getFred(fredseries.pi_copper)
pi_motors_df = getFred(fredseries.pi_motors)
pi_elec_df = getFred(fredseries.pi_elec)
pi_valves_df = getFred(fredseries.pi_valves)

pi_df = pd.concat({
    'Hot Rolled': pi_hot_rolled_df,
    'Cold Rolled': pi_cold_rolled_df,
    'Foundry Products': pi_foundry_df,
    'Nonferrous': pi_nonferrous_df,
    'Copper and Brass': pi_copper_df,
    'Motors and Generators': pi_motors_df,
    'Electrical Components': pi_elec_df,
    'Valves': pi_valves_df,
}, axis=1)

pi_df = generateComponents(pi_df)

pi_weights = [0.057, 0.014, 0.077, 0.060, 0.053, 0.036, 0.400, 0.053]

pi_index = pd.DataFrame((pi_df * pi_weights).sum(axis=1))
pi_index = pi_index.iloc[-120:]
pi_index = pi_index.divide(pi_index.iloc[0])*100

# Pumps and Compressors
pc_pumps_df = getFred(fredseries.pc_pumps)
pc_air_compressors_df = getFred(fredseries.pc_air_compressors)
pc_gas_compressors_df = getFred(fredseries.pc_gas_compressors)
pc_vacuum_df = getFred(fredseries.pc_vacuum)

pc_df = pd.concat({
    'Pumps': pc_pumps_df,
    'Air Compressors': pc_air_compressors_df,
    'Gas Compressors': pc_gas_compressors_df,
    'Vacuum Pumps': pc_vacuum_df,
}, axis=1)

pc_df = generateComponents(pc_df)

pc_weights = [0.900, 0.050, 0.025, 0.025]

pc_index = pd.DataFrame((pc_df * pc_weights).sum(axis=1))
pc_index = pc_index.iloc[-120:]
pc_index = pc_index.divide(pc_index.iloc[0])*100

# Electrical Equipment
ee_wire_df = getFred(fredseries.ee_wire)
ee_lighting_df = getFred(fredseries.ee_lighting)
ee_motors_df = getFred(fredseries.ee_motors)
ee_transformers_df = getFred(fredseries.ee_transformers)
ee_switchgear_df = getFred(fredseries.ee_switchgear)

ee_df = pd.concat({
    'Wire': ee_wire_df,
    'Lighting': ee_lighting_df,
    'Motors': ee_motors_df,
    'Transformers': ee_transformers_df,
    'Switchgear': ee_switchgear_df,
}, axis=1)

ee_df = generateComponents(ee_df)

ee_weights = [0.057, 0.188, 0.349, 0.146, 0.260]

ee_index = pd.DataFrame((ee_df * ee_weights).sum(axis=1))
ee_index = ee_index.iloc[-120:]
ee_index = ee_index.divide(ee_index.iloc[0])*100

# Misc
misc_paint_df = getFred(fredseries.misc_paint)
misc_hot_rolled_df = getFred(fredseries.misc_hot_rolled)
misc_rebar_df = getFred(fredseries.misc_rebar)
misc_concrete_df = getFred(fredseries.misc_concrete)
misc_insulation_df = getFred(fredseries.misc_insulation)

misc_df = pd.concat({
    'Paint': misc_paint_df,
    'Hot Rolled': misc_hot_rolled_df,
    'Rebar': misc_rebar_df,
    'Concrete': misc_concrete_df,
    'Insulation': misc_insulation_df,
}, axis=1)

misc_df = generateComponents(misc_df)

misc_weights = [0.024, 0.406, 0.089, 0.129, 0.353]

misc_index = pd.DataFrame((misc_df * misc_weights).sum(axis=1))
misc_index = misc_index.iloc[-120:]
misc_index = misc_index.divide(misc_index.iloc[0])*100

# Buildings
construction_materials_df = getFred(fredseries.construction_materials)
general_building_contractors_df = getFred(fredseries.general_building_contractors)

buildings_df = pd.concat({
    'Construction Materials': construction_materials_df,
    'General Building Contractors': general_building_contractors_df}, axis=1)

buildings_df = generateComponents(buildings_df)

buildings_weights = [0.53, 0.47]

buildings_index = pd.DataFrame((buildings_df*buildings_weights).sum(axis=1))
buildings_index = buildings_index.iloc[-120:]
buildings_index = buildings_index.divide(buildings_index.iloc[0])*100

# Supervision
admin_df = getFred(fredseries.admin)
engineering_df = getFred(fredseries.engineering)
design_df = getFred(fredseries.design)
managerial_df = getFred(fredseries.managerial)

supervision_df = pd.concat({
    'Administrative': admin_df,
    'Engineering': engineering_df,
    'Design and Drafting': design_df,
    'Managerial Support': managerial_df}, axis=1)

supervision_df = generateComponents(supervision_df)

supervision_weights = [0.06, 0.325, 0.395, 0.220]

supervision_index = pd.DataFrame((supervision_df*supervision_weights).sum(axis=1))
supervision_index = supervision_index.iloc[-120:]
supervision_index = supervision_index.divide(supervision_index.iloc[0])*100

# Construction Labor
general_df = getFred(fredseries.general_contractors)
heavy_df = getFred(fredseries.heavy_contractors)
special_df = getFred(fredseries.specialty_contractors)

const_df = pd.concat({
    'General Contractors': general_df,
    'Heavy Contractors': heavy_df,
    'Specialty Contractors': special_df}, axis=1)

const_df = generateComponents(const_df)

const_weights = [0.467, 0.317, 0.217]

const_index = pd.DataFrame((const_df*const_weights).sum(axis=1))
const_index = const_index.iloc[-120:]
const_index = const_index.divide(const_index.iloc[0])*100

equipment_df = pd.concat({
    'Heat Exchangers': heatx_index,
    'Process Machinery': process_machinery_index,
    'Pipes and Valves': pv_index,
    'Process Instruments': pi_index,
    'Pumps and Compressors': pc_index,
    'Electrical Equipment':ee_index,
    'Miscelaneous': misc_index}, axis=1)
equipment_df = equipment_df.sort_index()

equipment_weights = [0.338, 0.128, 0.190, 0.105, 0.064, 0.070, 0.105]

equipment_index = pd.DataFrame((equipment_df*equipment_weights).sum(axis=1))

equipment_df['Date'] = equipment_df.index
equipment_index['Date'] = equipment_index.index

# Load dataframes intto MySQL
engine = create_engine('mysql+pymysql://root:password@localhost:3306/CE_INDEX')
conn = engine.connect()

heatx_df.to_sql('heatx_df', conn, if_exists='replace', index=False)
heatx_index.to_sql('heatx_index', conn, if_exists='replace', index=False)
process_machinery_df.to_sql('process_machinery_df', conn, if_exists='replace', index=False)
process_machinery_index.to_sql('process_machinery_index', conn, if_exists='replace', index=False)
pv_df.to_sql('pv_df', conn, if_exists='replace', index=False)
pv_index.to_sql('pv_index', conn, if_exists='replace', index=False)
pi_df.to_sql('pi_df', conn, if_exists='replace', index=False)
pi_index.to_sql('pi_index', conn, if_exists='replace', index=False)
pc_df.to_sql('pc_df', conn, if_exists='replace', index=False)
pc_index.to_sql('pc_index', conn, if_exists='replace', index=False)
ee_df.to_sql('ee_df', conn, if_exists='replace', index=False)
ee_index.to_sql('ee_index', conn, if_exists='replace', index=False)
misc_df.to_sql('misc_df', conn, if_exists='replace', index=False)
misc_index.to_sql('misc_index', conn, if_exists='replace', index=False)
equipment_df.to_sql('equipment_df', conn, if_exists='replace', index=False)
equipment_index.to_sql('equipment_index', conn, if_exists='replace', index=False)
