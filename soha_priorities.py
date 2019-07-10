import sql_helpers
import pandas as pd
from datetime import datetime

def pull_well_specific_data():
    
    def pull_well_metadata():
        """
        Pull well metadata, including area, route, API, wellflac, and corpID. Source is various tables in EDH
        """
        query=('well_metadata.sql')
        #Pull SQL data from future state server.
        df=sql_helpers.pull_data_from_sql_query_future_state(query, 'EnterpriseDataHub')
        return df
    
    def pull_most_recent_well_codes():
        """
        Pull most recent coding for each well in Enbase. Source is Enbase.ChokeStatusAction in ODS
        """
        query=('most_recent_well_coding.sql')
        #Pull SQL data from future state server.
        df=sql_helpers.pull_data_from_sql_query_future_state(query, 'ODS')
        return df
    
    def pull_yday_gas_production():
        """
        Pull yesterday's gas production in SoHa. Source table is bpx_field.daily_gas_snapshot in EDH (allocated daily gas volumes)
        """
        query=('yday_production_soha.sql')
        #Pull SQL data from future state server.
        df=sql_helpers.pull_data_from_sql_query_future_state(query, 'EnterpriseDataHub')
        df['Production_Date'] = pd.to_datetime(df['production_date_utc'])
        df['Gas_Production']=df['wellhead_extrapolated_24_hr_gas'].astype('float64')
        return df
    
    def pull_clean_average():
        """
        Pull clean average from EDH. Source table is clean 365 table in EDH
        """
        query=('clean_average.sql')
        #Pull SQL data from future state server.
        df=sql_helpers.pull_data_from_sql_query_arrow_future_state(query)
        return df 
    
    try:
        #Pull metadata and most recent chokestatusaction
        well_metadata=pull_well_metadata()
        #Make APINumber in well_metadata API10
        well_metadata['API'] = well_metadata['API'].astype(str).str[0:10]
        well_codes=pull_most_recent_well_codes()
        #Merge well code with well metadata
        merged_df=pd.merge(well_metadata, well_codes[['apinumber', 'chokeStatusCreatedBy', 'chokeStatusDate', 'chokeStatusType', 
                                                      'chokeStatusAction', 'chokeStatusComments']], how='inner', left_on='API', right_on='apinumber')
        #Pull yesterday's well production
        yday_gas_production=pull_yday_gas_production()
        #Pull clean average data
        clean_average=pull_clean_average()
        #Merge yesterday's production into the main dataframe 
        merged_df=pd.merge(merged_df, yday_gas_production[['Corp_ID', 'Gas_Production']], how='inner', on='Corp_ID')
        #erge clean average into the main dataframe
        merged_df=pd.merge(merged_df, clean_average[['Corp_ID', 'CleanAvgGas', 'CleanAvgLowerBoundGas']], how='inner', on='Corp_ID')
        return merged_df
    except:
        return 'Metadata pull failure'

def format_priorities(df):
    """
    Format the work management priorities for insertion into the SQL table.
    """
    df=df[['WellName','Corp_ID','Facility_ID', 'Area', 'Route','Latitude', 'Longitude', 'Priority', 'Priority_Level', 
           'Description', 'Assigned_To', 'chokeStatusCreatedBy', 'chokeStatusDate', 'chokeStatusType','chokeStatusAction', 
           'chokeStatusComments', 'Gas_Production', 'CleanAvgGas']].drop_duplicates()
    return df

def gas_deferment_priorities(Assigned_To, well_metadata):
    
    def detect_if_well_is_deferring(df):
        """
        Detect if gas volume is lower than the clean average lower bound, and create a boolean column for when the condition is met.
        """
        df['Deferring']=(df.Gas_Production<df.CleanAvgLowerBoundGas)
        return df
    
    def calculate_deferment(df):
        """
        Subtract yesterday's production from the clean average
        """
        df['Deferment']=df['CleanAvgGas']-df['Gas_Production']
        return df
    
    def set_priority(df):
        """
        Set priorities based on deferment amount (top 25% deferring wells are priority 1, 
        25-50% are priority 2, etc.)
        """
        #Declare priority type as deferment
        df['Priority']='Deferment'
        #Get deferment quantiles to rank deferment by amount
        df['DefermentRanking']=df['Deferment'].rank(ascending=0)
        df=df.sort_values(by='Deferment')
        df['DefermentQuantile']=1-(df.shape[0]-df['DefermentRanking'])/df.shape[0]
        #Set up priority logic based on quantiles
        df.loc[df['DefermentQuantile']<.25, 'Priority_Level'] = 2
        df.loc[df['DefermentQuantile']>=.25, 'Priority_Level'] = 3
        df.loc[df['DefermentQuantile']>=.50, 'Priority_Level'] = 4
        df.loc[df['DefermentQuantile']>=.75, 'Priority_Level'] = 5
        #Set up description logic based on quantiles
        df.loc[df['DefermentQuantile']<.25, 'Description'] = 'Top 25% deferring wells: '
        df.loc[df['DefermentQuantile']>=.25, 'Description'] = 'Top 25%-50% wells deferring: '
        df.loc[df['DefermentQuantile']>=.5, 'Description'] = 'Top 50%-75% wells deferring: '
        df.loc[df['DefermentQuantile']>=.75, 'Description'] = 'Bottom 25% wells deferring: '
        #Add description details, including current production, and amount deferment
        df['Description']=df['Description']+'Yesterday well produced '+df.Gas_Production.values.astype(str)+' MCFE, and deferred '+df.Deferment.values.astype(str)+' MCFE.'
        return df
    
    #Use try-except statement to build and format deferment priorities, if there are any
    try:
        #Detect if a well is deferring or not (is it below lowerboundgas?)
        well_metadata=detect_if_well_is_deferring(well_metadata)
        #Remove any wells that aren't deferring
        well_metadata=well_metadata[well_metadata.Deferring==True]
        #Calculate deferment
        well_metadata=calculate_deferment(well_metadata)
        #Prioritize based on amount of deferment
        well_metadata=set_priority(well_metadata)
        #Add a blank Assigned_To column
        well_metadata['Assigned_To']=Assigned_To
        #Reformat priorities for SQL table insertion
        well_metadata=format_priorities(well_metadata)
        return well_metadata
    except:
        return 'No Deferment Priorities'

def work_management_priorities(well_metadata):
    
    def pull_open_work_management_entries():
        """
        Pull any work management entries that are currently in progress and are from the past week. 
        Source is Enbase.WorkManagement in ODS
        """
        query=('work_management_entries.sql')
        #Pull SQL data from future state server.
        df=sql_helpers.pull_data_from_sql_query_future_state(query, 'ODS')
        return df
    
    def fill_blank_priorities(df):
        """
        Autofill any priorities without a declared level as a 5.
        """
        df.loc[pd.isna(df['workOrderPriorityLevel']),'workOrderPriorityLevel']=5
        return df
    
    #Use a try-except statement to build and return work management priorities, if there are any
    try:
        #Pull work management priorities
        work_management=pull_open_work_management_entries()
        #Merge work management entries with well metadata
        work_management_merged=pd.merge(work_management, well_metadata.drop(columns=['Route']), left_on='APINumber', right_on='API', how='inner')
        #Fill any blank priorities with a default of 5.
        work_management_merged=fill_blank_priorities(work_management_merged)
        #Declare priority type as Work Management
        work_management_merged['Priority']='Enbase Work Management'
        #Rename the Priority_Level, Description, Assigned_To, and WellName columns
        work_management_merged.rename(columns={'workOrderDescription': 'Description', 'workOrderPriorityLevel': 'Priority_Level',
                       'workOrderRequester': 'Assigned_To'}, inplace=True)
        #Reformat for SQL insertion
        work_management_final=format_priorities(work_management_merged)
        #Return finalized dataframe
        return work_management_final
    except:
        return 'No Work Management Priorities'

def flood_priorities(Assigned_To, well_metadata):
    
    def pull_flood_data():
        """
        Pull flood predictions from SQL
        """
        query=('Flood_Priorities_Prediction.sql')
        df=sql_helpers.pull_data_from_sql_query_arrow_future_state(query)
        return df  
    
    def detect_if_well_is_already_shut_in_due_to_weather(merged_df):
        """
        Check the lastchokestatus and determine if the well is down due to weather
        """
        merged_df['Already_Down_For_Weather']=(merged_df['chokeStatusType']=='Down - Weather')
        return merged_df
    
    def set_priority(df):
        """
        Sets the priority of shutting the well in, based on how soon it's going to flood.
        """
        #Declare priority type
        df['Priority']='Flood Alert'
        #Conditionally set Priority column based on when well is predicted to flood
        df.loc[df['HoursUntilFlood']<24, 'Priority_Level'] = 1
        df.loc[df['HoursUntilFlood']>=24, 'Priority_Level'] = 2
        df.loc[df['HoursUntilFlood']>=48, 'Priority_Level'] = 3
        df.loc[df['HoursUntilFlood']>=72, 'Priority_Level'] = 4
        df.loc[df['HoursUntilFlood']>=96, 'Priority_Level'] = 5
        #Conditionally set Description based on when well is predicted to flood
        df.loc[df['HoursUntilFlood']<24, 'Description'] = 'Well is already flooding or is predicted to flood within the next 24 hours. '
        df.loc[df['HoursUntilFlood']>=24, 'Description'] = 'Well is predicted to flood between 1 to 2 days. '
        df.loc[df['HoursUntilFlood']>=48, 'Description'] = 'Well is predicted to flood between 2 and 3 days. '
        df.loc[df['HoursUntilFlood']>=72, 'Description'] = 'Well is predicted to flood between 3 and 4 days. '
        df.loc[df['HoursUntilFlood']>=96, 'Description'] = 'Well is predicted to flood between 4 and 5 days. '
        #Add description details
        df['Description']=df['Description']+'Affected flood height of the site is '+df.AffectedFloodHeight.values.astype(str)+' ft. Next predicted flood date is '+ df.EarliestPredictedFloodDate.values.astype(str)+'.'
        #Return datframe with added Priority and Description columns
        return df

    #Use a try-except statement to build and return flood priorities, if there are any
    try:
        #Pull the flood data and store to pandas df
        flood_data=pull_flood_data()
        flood_data['API']=flood_data.API.str[:10]
        #Merge flood data with well metadata
        merged_df=pd.merge(flood_data.drop(columns=['WellName']), well_metadata, on='API', how='inner')
        #Determine if the wells already shut in due to flooding
        merged_df=detect_if_well_is_already_shut_in_due_to_weather(merged_df)
        #Remove cases where the well is already shut in due to weather (it's already been actioned)
        merged_df=merged_df[merged_df.Already_Down_For_Weather==False]
        #Set priorities based on when the well is supposed to flood
        merged_df=set_priority(merged_df)
        #Create Assigned_To Column and assign to James Walker
        merged_df['Assigned_To']=Assigned_To
        #Reformat the priorities
        merged_df=format_priorities(merged_df)
        return merged_df
    except:
        return 'No Flood Priorities'

def cumulative_deferment_priorities(well_metadata):
    
    def pull_cumulative_deferment_for_each_well():
        """
        This function pulls the cumulative deferment for all of the SoHa wells from current state architecture.
        """
        query=('cumulative_deferment.sql')
        df=sql_helpers.pull_data_from_sql_query_current_state(query)
        return df
    
    def set_priority(df):
        """
        Sets the priority for cumulative deferment wells, based on logic.
        """
        #Declare priority type
        df['Priority']='Cumulative Deferment'
        #Set priorities based on total amount of cumulative deferment
        df.loc[(df['CumulativeDeferment']>=1000) & (df['CumulativeDeferment']<2000), 'Priority_Level'] = 5
        df.loc[(df['CumulativeDeferment']>=2000) & (df['CumulativeDeferment']<3000), 'Priority_Level'] = 4
        df.loc[(df['CumulativeDeferment']>=3000) & (df['CumulativeDeferment']<4000), 'Priority_Level'] = 3
        df.loc[(df['CumulativeDeferment']>=4000) & (df['CumulativeDeferment']<5000), 'Priority_Level'] = 2
        #Add description
        df['Description']='Well has a cumulative deferment of '+df.CumulativeDeferment.astype(str)+' MCFE, and has been deferring for '+df.ConsecutiveDaysDeferring.values.astype(str)+' days.'
        #Return datframe with added Priority and Description columns
        return df

    #Run a try-except statement to pull cumulative deferment priorities, if there are any
    try:
        #Pull cumulative deferment data.
        cumulative_deferment_data=pull_cumulative_deferment_for_each_well()    
        #Merge the two data frames.
        cumulative_deferral_merged=pd.merge(well_metadata, cumulative_deferment_data, how='inner', left_on='Corp_ID', right_on='CorpID')
        #Subset the data frame to include well that have been deferring for five or more days, and more than 1000 MCFE
        cumulative_deferral_merged=cumulative_deferral_merged[(cumulative_deferral_merged['ConsecutiveDaysDeferring']>5) & (cumulative_deferral_merged['CumulativeDeferment']>=1000)]
        #Set priorities based on conditions
        cumulative_deferral_merged=set_priority(cumulative_deferral_merged)
        cumulative_deferral_merged['Assigned_To']=None
        #Reformat to fit the current priorities structure
        cumulative_deferral_reformatted=format_priorities(cumulative_deferral_merged)
        #Return the formatted dataset, ready for insertion
        return cumulative_deferral_reformatted
    except: 
        return 'No Cumulative Deferment Priorities'

def site_inspection_priorities(well_metadata):
    
    def pull_site_inspections():
        """
        This function pulls the last date of site inspection for each well from Enbase.
        """
        query=('site_inspections.sql')
        #Pull SQL data from future state server.
        df=sql_helpers.pull_data_from_sql_query_future_state(query, 'ODS')
        return df
    
    def set_priority(df):
        """
        Sets site inspection priority, based on when the well was last visited.
        """
        #Declare priority type
        df['Priority']='Site Inspection'
        #Set priorities based on last site visit
        df.loc[(df['DaysSinceLastInspection']>=60) & (df['DaysSinceLastInspection']<75), 'Priority_Level'] = 5
        df.loc[(df['DaysSinceLastInspection']>=75) & (df['DaysSinceLastInspection']<90), 'Priority_Level'] = 4
        df.loc[df['DaysSinceLastInspection']>=90, 'Priority_Level'] = 3
        #Add description details
        df['Description']='Site Inspection Due: Last recorded site inspection was '+df.DaysSinceLastInspection.astype(str)+' days ago.'
        #Return datframe with added Priority and Description columns
        return df
    
    #Run a try-except statement to pull site inspection priorities, if there are any
    try:
        #Pull site inspection data.
        site_inspection_data=pull_site_inspections()   
        #Merge the two data frames.
        site_inspection_merged=pd.merge(site_inspection_data, well_metadata, left_on='APINumber', right_on='API', how='inner')
        #Remove any wells that have been inspected in the past 60 days.
        site_inspection_merged=site_inspection_merged[site_inspection_merged['DaysSinceLastInspection']>60]
        #Subset the data frame to include well that have been deferring for five or more days, and more than 1000 MCFE
        #Set priorities based on conditions
        site_inspection_merged=set_priority(site_inspection_merged)
        site_inspection_merged['Assigned_To']=None
        #Reformat to fit the current priorities structure
        site_inspection_reformatted=format_priorities(site_inspection_merged)
        #Return the formatted dataset, ready for insertion
        return site_inspection_reformatted
    except: 
        return 'No Site Inspection Priorities'
    
def RTU_comms_priorities(well_metadata):
    
    def pull_most_recent_battery_voltage():
        """
        This function pulls the most recent battery voltages recorded for each RTU
        """
        query=('rtu_battery_voltages.sql')
        #Pull SQL data from future state server.
        df=sql_helpers.pull_data_from_sql_query_future_state(query, 'EnterpriseDataHub')
        return df

    def pull_most_recent_hourly_percent_successful_comms():
        """
        This function pulls the most recent percentage successful comms (last hour) for each RTU
        """
        query=('percent_successful_comms.sql')
        #Pull SQL data from future state server.
        df=sql_helpers.pull_data_from_sql_query_future_state(query, 'EnterpriseDataHub')
        return df

    def set_priority(df):
        """
        Sets automation priority, based on most recent percent successful comms and current battery voltage.
        """
        #Declare priority type
        df['Priority']='Automation-RTU Issue'
        #Set priorities based on last site visit
        df.loc[(df['PercentSuccessfulComms'].astype('float')<=50) | (df['BatteryVoltage'].astype('float')<=11), 'Priority_Level'] = 3
        df.loc[(df['PercentSuccessfulComms'].astype('float')>50) & (df['PercentSuccessfulComms'].astype('float')<=60), 'Priority_Level'] = 4
        df.loc[(df['PercentSuccessfulComms'].astype('float')>60) & (df['PercentSuccessfulComms'].astype('float')<=75), 'Priority_Level'] = 5
        #Conditionally set Description based on last site visit
        df.loc[:, 'Description'] = 'RTU Comms Issue Detected: '
        #Add description details
        df['Description']=df['Description']+'Percent successful comms in the past hour is '+df.PercentSuccessfulComms.values.astype(str)+'%, and current battery voltage is '+df.BatteryVoltage.values.astype(str)+'.'
        #Return datframe with added Priority and Description columns
        return df
    #Run a try-except statement to pull automation priorities, if there are any
    try:
        #Pull the most recent battery voltages for the RTU
        battery_voltages=pull_most_recent_battery_voltage()
        #Pull the most recent hourly successful comms percentage
        hourly_percent_successful_comms=pull_most_recent_hourly_percent_successful_comms()
        #Merge all of the data sets together to create a master data set to build automation priorities off of
        comms_anomaly_df=pd.merge(battery_voltages[['Corp_ID', 'Meter', 'LastBatteryVoltageReading', 'BatteryVoltage']], 
                                  hourly_percent_successful_comms[['Corp_ID', 'Meter', 'LastPercentSuccessfulCommsReading', 
                                                               'PercentSuccessfulComms']], on=['Corp_ID', 'Meter'], how='outer')
        #Add metadata to the comms_anomaly_df dataframe
        comms_anomaly_df=pd.merge(comms_anomaly_df, well_metadata, on=['Corp_ID'], how='inner')
        #Run the dataframe through the set_priorities(), determining if there are any RTU's with poor performance
        comms_anomaly_df=set_priority(comms_anomaly_df)
        #Remove any rows without priorities associated with them (filter out null Priority_Level columns)
        comms_anomaly_df=comms_anomaly_df[comms_anomaly_df['Priority_Level'].notnull()]
        #Created Assigned_To column
        comms_anomaly_df['Assigned_To']=None
        #Format to fit the main priorities dataframe
        comms_anomaly_df=format_priorities(comms_anomaly_df)
        #return the list of automation priorities
        return comms_anomaly_df
    except: 
        return 'No Automation Priorities'

def classify_priority_types_to_groups(priority_df):
    """
    This function assigns priorities to different groups--FSS's, engineers, site managers, automation, optimizers--
    based on well coding and other logic.
    """
    #Set wells coded to engineering to the engineering bucket
    priority_df.loc[(priority_df.chokeStatusAction.str.contains("Engineering")), 'Grouper'] = 'Engineering'
    #Set wells coded to operations in operations bucket
    priority_df.loc[(priority_df.chokeStatusAction.str.contains("Operations")), 'Grouper'] = 'Operations'
    #Set wells coded to 'No Action' in the Operations bucket
    priority_df.loc[(priority_df.chokeStatusAction.str.contains("No Action")), 'Grouper'] = 'Operations'
    #Set wells coded to maintenance in the operations bucket
    priority_df.loc[(priority_df.chokeStatusAction.str.contains("Maintenance")), 'Grouper'] = 'Operations'
    #Set wells coded to Natural Decline into the operations bucket
    priority_df.loc[(priority_df.chokeStatusAction.str.contains("Natural Decline")), 'Grouper'] = 'Operations'
    #Set wells coded to midstream in the operations bucket
    priority_df.loc[(priority_df.chokeStatusAction.str.contains("Midstream")), 'Grouper'] = 'Operations'
    #Set wells coded as work complete to operations
    priority_df.loc[(priority_df.chokeStatusAction.str.contains("Work Complete")), 'Grouper'] = 'Operations'
    #Set wells coded as Waiting on Optimization to operations
    priority_df.loc[(priority_df.chokeStatusAction.str.contains("Optimization")), 'Grouper'] = 'Operations'
    #Set wells coded as Waiting on Construction to Construction
    priority_df.loc[(priority_df.chokeStatusAction.str.contains("Construction")), 'Grouper'] = 'Construction'
    #Set automation priorities to automation
    priority_df.loc[(priority_df.Priority=='Automation'), 'Grouper'] = 'Automation'
    #Set flood priorities to site manager
    priority_df.loc[priority_df.Priority=='Flood Alert', 'Grouper'] = 'Site Manager'
    #Set site inspection priorities to Operations
    priority_df.loc[priority_df.Priority=='Site Inspection', 'Grouper'] = 'Operations'
    return priority_df
    
def main():
    """
    Find priorities and write them to a SQL table
    """
    #Declare the site manager so he can be assigned specific 'site manager' priorities
    site_manager='name'
    #Pull the well metadata that will used as a basis for priorities
    well_metadata=pull_well_specific_data()
    #Get flood priorities
    flood_priorities_df=flood_priorities(site_manager, well_metadata)
    #Get work management priorities
    work_management_priorities_df=work_management_priorities(well_metadata)
    #Get deferment priorities
    deferment_priorities_df=gas_deferment_priorities(None, well_metadata)
    #Get the cumulative deferment priorities
    #cumulative_deferment_priorities_df=cumulative_deferment_priorities()
    #Get all the site inspection priorities
    site_inspection_priorities_df=site_inspection_priorities(well_metadata)
    #Get all the automation priorities
    automation_priorities_df=RTU_comms_priorities(well_metadata)
    #Create a master dataframe to append priorities to
    priority_df=pd.DataFrame()
    #Add deferment priorities if there are any 
    if str(type(deferment_priorities_df))=="<class 'pandas.core.frame.DataFrame'>":
        priority_df=priority_df.append(deferment_priorities_df)
    #Add work management priorities if there any
    if str(type(work_management_priorities_df))=="<class 'pandas.core.frame.DataFrame'>":
        priority_df=priority_df.append(work_management_priorities_df)
    #Add flood priorities if there are any
    if str(type(flood_priorities_df))=="<class 'pandas.core.frame.DataFrame'>":
        priority_df=priority_df.append(flood_priorities_df)
    """
    #Add cumulative deferment priorities if there are any
    if str(type(cumulative_deferment_priorities_df))=="<class 'pandas.core.frame.DataFrame'>":
        priority_df=priority_df.append(cumulative_deferment_priorities_df)
    """
    #Add site inspection priorities if there are any 
    if str(type(site_inspection_priorities_df))=="<class 'pandas.core.frame.DataFrame'>":
        priority_df=priority_df.append(site_inspection_priorities_df)
    #Add automation priorities if there any
    if str(type(automation_priorities_df))=="<class 'pandas.core.frame.DataFrame'>":
        priority_df=priority_df.append(automation_priorities_df)
    #Write the priorities to a table
    priority_df=classify_priority_types_to_groups(priority_df)
    #Remove any T&A candidates
    priority_df=priority_df[priority_df.chokeStatusAction!='TA - P&A Candidate']
    #add calculation date column
    priority_df['Calc_Date']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #Rename columns for SQL insertion
    priority_df.rename(columns={'WellName': 'Well_Name', 'chokeStatusDate': 'Choke_Status_Date',
                       'chokeStatusType': 'Choke_Status_Type', 'chokeStatusAction': 'Choke_Status_Action', 
                       'chokeStatusCreatedBy': 'Choke_Status_Created_By',
                       'chokeStatusComments': 'Choke_Status_Comments', 'Gas_Production':'Yesterday_Gas_Production',
                       'CleanAvgGas':'Clean_Average_Gas'}, inplace=True)
    #Insert into ArrowAppTest table, under SoHa.Priorities_Test
    sql_helpers.sql_push_future_state_arrow_test(priority_df, table='Priorities_Test', schema='SoHa', if_exists='replace', database='ArrowtestDB')
    #Insert formatted priorities into the VRP_Details.SoHa_Priorities table
    #Reformat priorities for insertion into the approved Arrow table
    priority_df.rename(columns={'Well_Name':'SiteName',
                          'Facility_ID':'FacilityKey', 
                          'Corp_ID':'LocationID',
                          'Priority_Level':'PriorityLevel',
                          'Description':'Reason',
                          'Priority':'PriorityType',
                          'Facility_ID':'FacilityKey',
                          'Calc_Date': 'CalcDate',
                          'Assigned_To': 'Person_assigned'}, inplace=True)
    #Add in any missing columns for final insertion
    priority_df['Supporting_info']=None
    priority_df['Job_Rank']=None
    priority_df['JobTime']=None
    priority_df['DefermentGas']=priority_df['Clean_Average_Gas']-priority_df['Yesterday_Gas_Production']
    #Subset the dataframe for final insertion
    priority_df=priority_df[['FacilityKey', 'SiteName', 'LocationID', 'Latitude', 'Longitude', 'PriorityLevel','Grouper','JobTime',
                             'Reason','Supporting_info','PriorityType','DefermentGas','Person_assigned','Job_Rank','CalcDate']]
    sql_helpers.sql_push_future_state_arrow_test(priority_df, table='SoHa_Priorities', schema='VRP_Details', if_exists='replace', database='ArrowtestDB')

#Execute the main script and run the priorities
main()
    
    
