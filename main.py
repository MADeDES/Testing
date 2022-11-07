import pandas as pd
import numpy as np
import pymongo
import PHM


# "C:/Users/61435/OneDrive - PHM Technology/PHM Tech/Research/Testability/PHM_Module"
sensor_path = "C:/Users/61435/OneDrive - PHM Technology/PHM Tech/Research/Testability/PHM_Module/"
sensor_filename = "Excel_Calculations.xlsx"

# Create open_pyxl doc

# Pymongo SetUp Database
client = pymongo.MongoClient(host="localhost", port=27017)
MADe_db = client.MADe
sensor_library_collection = MADe_db.Sensor_Library
sensor_location_collection = MADe_db.Sensor_Location
failure_mode_collection = MADe_db.Failure_Mode

# Read in excel files
PHM.SensorFactory(sensor_path+sensor_filename, "sensor_library", sensor_library_collection)
PHM.S_LocFactory(sensor_path+sensor_filename, "sense_locations", sensor_location_collection)
PHM.FmFactory(sensor_path+sensor_filename, "failure_modes", failure_mode_collection)

# # Calc metrics from sensor set
df = pd.read_excel(sensor_path+sensor_filename, "Proptable")
pt = PHM.PropagationTable("Test", df.copy().replace(np.nan, 0))
df_sense_loc = pd.read_excel(sensor_path+sensor_filename, "sense_locations", index_col=0)

# Create Custom propagation_tables
S1 = ['s_loc20', 's_loc17', 's_loc6',  's_loc4', 's_loc5', 's_loc8', 's_loc16', 's_loc13', 's_loc14','s_loc2']
S2 = ['s_loc20', 's_loc17', 's_loc6', 's_loc4', 's_loc5']

df_s1 = df[PHM.FM_LABELS + S1].copy()
pt_s1 = PHM.PropagationTable("Maurice_SS1", df_s1.replace(np.nan, 0))

df_s2 = df[PHM.FM_LABELS + S2].copy()
pt_s2 = PHM.PropagationTable("Maurice_SS2", df_s2.replace(np.nan, 0))

# add attributes
# pt.add_sensors(sensor_library_collection, df)
pt.add_sense_locations(sensor_location_collection)
pt.add_failure_modes(failure_mode_collection)
pt_s1.add_failure_modes(failure_mode_collection)
pt_s2.add_failure_modes(failure_mode_collection)

# Save propagation_tables
with pd.ExcelWriter(sensor_path+sensor_filename+"_out"+".xlsx") as writer:
    df_sense_loc.to_excel(writer, "_sense_locations")
    pt.calc_stats()
    pt.save_pt(writer)
    pt.save_stats(writer)

    pt_s1.calc_stats()
    pt_s1.save_pt(writer)
    pt_s1.save_stats(writer)

    pt_s2.calc_stats()
    pt_s2.save_pt(writer)
    pt_s2.save_stats(writer)
