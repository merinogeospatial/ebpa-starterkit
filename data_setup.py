#############
## MODULES ##
#############
import arcpy
import json
import os
import shutil
import sys
import time
import urllib

####################################
## SOME GLOBAL SETTINGS/VARIABLES ##
####################################
start = time.time()
project_dir = os.getcwd()
arcpy.env.workspace = project_dir
arcpy.env.overwriteOutput = True

####################################################
## CREATE INTERMEDIATE GDB FOR SETTING UP PROJECT ##
####################################################
arcpy.CreateFileGDB_management(project_dir, 'SETUP')
setup_gdb = os.path.join(project_dir, 'SETUP.gdb')

#####################################
## DOWNLOAD DATA FROM WEB SERVICES ##
#####################################
data_sources = {
    'parks': {
        'url': 'https://maps.raleighnc.gov/arcgis/rest/services/Parks/Greenway/MapServer/5',
        'abbreviation': 'p',
        'tableType': 'feature'
    },
    'access_points': {
        'url': 'https://services.arcgis.com/v400IkDOw1ad7Yad/arcgis/rest/services/Park_Access_Points/FeatureServer/0',
        'abbreviation': 'ap',
        'tableType': 'feature'
    },
    'analysis_tiers': {
        'url': 'https://services.arcgis.com/v400IkDOw1ad7Yad/arcgis/rest/services/Park_Analysis_Tiers/FeatureServer/0',
        'abbreviation': 'at',
        'tableType': 'record' 
    }
}
# Logic for this section courtest of SoCalGIS.org
# Source: https://socalgis.org/2018/03/28/extracting-more-features-from-map-services/
for d in data_sources.items():
    remote_data_name = d[0]
    baseURL = d[1]['url']
    table_type = d[1]['tableType']
    fields = "*"
    out_data = os.path.join(project_dir, 'SETUP.gdb', d[1]['abbreviation'])
    
    # G
    urlstring = baseURL + "?f=json"
    j = urllib.request.urlopen(urlstring).read().decode('UTF-8')
    js = json.loads(j)
    maxrc = int(js["maxRecordCount"])
    print("Record extract limit: %s" % maxrc)
    
    # Get object ids of features
    where = "1=1"
    urlstring = baseURL + "/query?where={}&returnIdsOnly=true&f=json".format(where)
    j = urllib.request.urlopen(urlstring).read().decode('UTF-8')
    js = json.loads(j)
    idfield = js["objectIdFieldName"]
    idlist = js["objectIds"]
    idlist.sort()
    numrec = len(idlist)
    print("Number of target records: %s" % numrec)
    
    # Gather features
    print("Gathering records...")
    fs = dict()
    for i in range(0, numrec, maxrc):
      torec = i + (maxrc - 1)
      if torec > numrec:
        torec = numrec - 1
      fromid = idlist[i]
      toid = idlist[torec]
      where = "{} >= {} and {} <= {}".format(idfield, fromid, idfield, toid)
      print("  {}".format(where))
      urlstring = baseURL + "/query?where={}&returnGeometry=true&outFields={}&f=json".format(where,fields)
      if table_type == 'feature':
        fs[i] = arcpy.FeatureSet()
      else:
        fs[i] = arcpy.RecordSet()  
      fs[i].load(urlstring)
    
    # Save features
    print("Saving features...")
    fslist = []
    for key,value in fs.items():
      fslist.append(value)
    arcpy.Merge_management(fslist, out_data)
    print("Done!")


######################################################################
## CREATE PARKS AND ACCESS POINTS FEATURE CLASSES FOR EACH SCENARIO ##
###################################################################### 
queries = {
    'los': {
        'baseline': "LEVEL_OF_SERVICE = 1 AND EBPAYEAR = 2013",
        'current': "LEVEL_OF_SERVICE = 1"
    },
    'la': {
        'baseline': "LAND_ACQUISITION = 1 AND EBPAYEAR = 2013",
        'current': "LAND_ACQUISITION = 1"
    }
}
lyr_name_list = ['p', 'ap']
analysis_tiers_table = os.path.join(setup_gdb, 'at')
scenarios = ["baseline", "current"]

for q in queries.items():
    for lyr_name in lyr_name_list:
        if lyr_name == 'p':
            print('Working on parks...')
            keep_fields = ['Shape',
                           'PARKID',
                           'NAME',
                           'MAP_ACRES']
        else:
            print('Working on access points...')
            keep_fields = ['PARKID',
                           'AP_CODE',
                           'TYPE',
                           'STATUS',
                           'ENTRANCE', 
                           'PARK_NAME',
                           'NETWORK',
                           'AP_ID']
            
        feature_lyr = '{}_lyr'.format(lyr_name)
        
        # Create feature layer
        arcpy.MakeFeatureLayer_management(os.path.join(setup_gdb, lyr_name), feature_lyr)
        
        # Join to layer to analysis tiers table
        arcpy.AddJoin_management(feature_lyr, 'PARKID', analysis_tiers_table, 'PARKID')
                
        # Create Field Map to limit the fields exported
        field_mappings = arcpy.FieldMappings()
        field_mappings.addTable(feature_lyr)
        feature_lyr_fields = arcpy.ListFields(feature_lyr)
        for feature_lyr_field in feature_lyr_fields:
            feature_lyr_field_name = feature_lyr_field.name
            feature_lyr_field_split = feature_lyr_field_name.split('.')
            feature_lyr_field_stem = feature_lyr_field_split[1]
        
            if feature_lyr_field_stem not in keep_fields:
                feature_lyr_field_field_map_index = field_mappings.findFieldMapIndex(feature_lyr_field_stem)
                if feature_lyr_field_field_map_index > -1:
                    field_mappings.removeFieldMap(feature_lyr_field_field_map_index)
        
        
        
        for scenario in scenarios:
            scenario_fc_name = '{}_{}_{}'.format(lyr_name, q[0], scenario)
            scenario_query = q[1][scenario]
            print('Creating {} based on the condition(s) where {}...'.format(scenario_fc_name, scenario_query))
            arcpy.FeatureClassToFeatureClass_conversion(in_features = feature_lyr,
                                                        out_path = setup_gdb,
                                                        out_name = scenario_fc_name,
                                                        where_clause = scenario_query,
                                                        field_mapping = field_mappings)
            scenario_fc = os.path.join(setup_gdb, scenario_fc_name)
            scenario_out_fields = arcpy.ListFields(scenario_fc)
            for scenario_out_field in scenario_out_fields:
                if scenario_out_field.name[-2:] == "_1":
                    arcpy.DeleteField_management(scenario_fc, scenario_out_field.name)
            print('Success!')

        # Remove join 
        arcpy.RemoveJoin_management(feature_lyr)

################################################################
## DISTRIBUTE DATA FROM SETUP.GDB TO INDIVIDUAL SCENARIO GDBS ##
################################################################
# NOTE: When running the model, the network dataset and census block centroids can be pulled directly from EBPA_NETWORK.gdb and EBPA_CENSUS.gdb, respectively.
scenario_gdb_names = ['LOS_BASELINE',
                      'LOS_CURRENT',
                      'LA_BASELINE',
                      'LA_CURRENT']
census_gdb = os.path.join(project_dir,
                          'EBPA_CENSUS.gdb')

for scenario_gdb_name in scenario_gdb_names:
    arcpy.CreateFileGDB_management(project_dir,
                                   scenario_gdb_name)
    scenario_gdb_path = os.path.join(project_dir,
                                     scenario_gdb_name + ".gdb")
    # add data from EBPA_CENSUS.gdb and SETUP.gb    
    if scenario_gdb_name == 'LOS_BASELINE':
        census_scenario_fcs = ['BLOCKS_2013',
                               'BLOCKGROUP_2013']
        scenario_fcs = ['ap_los_baseline',
                        'p_los_baseline']
    elif scenario_gdb_name == 'LOS_CURRENT':
        census_scenario_fcs = ['BLOCKS_2013',
                      'BLOCKS_2017',
                      'BLOCKGROUP_2017']
        scenario_fcs = ['ap_los_current',
                        'p_los_current']
    elif scenario_gdb_name == 'LA_BASELINE':
        census_scenario_fcs = ['BLOCKS_2013',
                      'BLOCKGROUP_2013']
        scenario_fcs = ['ap_la_baseline',
                        'p_la_baseline']
    elif scenario_gdb_name == 'LA_CURRENT':
        census_scenario_fcs = ['BLOCKS_2013',
                      'BLOCKS_2017',
                      'BLOCKGROUP_2017']
        scenario_fcs = ['ap_la_current',
                        'p_la_current']
    
    for census_scenario_fc in census_scenario_fcs:
        print("Adding {} to {}...".format(census_scenario_fc, scenario_gdb_name))
        arcpy.Copy_management(os.path.join(census_gdb, census_scenario_fc),
                              os.path.join(scenario_gdb_path, census_scenario_fc))
        print("Success!")
        
    for scenario_fc in scenario_fcs:
        print("Adding {} to {}...".format(scenario_fc, scenario_gdb_name))
        arcpy.Copy_management(os.path.join(setup_gdb, scenario_fc),
                              os.path.join(scenario_gdb_path, scenario_fc))
        print("Success!")

try:
    shutil.rmtree(setup_gdb)
except OSError as e:
    print("Error {} - {}. Delete SETUP.gdb manually.".format(e.filename, e.strerror))
duration = (time.time() - start)/60
print("Time Elapsed: {} minutes".format(duration))