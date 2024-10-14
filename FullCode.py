from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from bng_latlon import WGS84toOSGB36
import itertools

###############################################################################
                                 #GET DATA#
###############################################################################
def get_data():
	engine = create_engine('mssql+pyodbc://@SDMartDataLive2/InfoDB?'\
						   'trusted_connection=yes&driver=ODBC+Driver+17'\
						   '+for+SQL+Server')
	#Get the patient information too
	pat_sql = """--Get outpatients who are news or follow ups, who are not due to be seen via telephone or video, or at a virtual clinic
	SELECT * FROM
	--Query to retrieve the OP waiting list data for both news and follow ups
	(SELECT op_new.[pasid], op_new.[pat_pcode] pcode, [local_spec_desc],
	        op_new.[clinic_code], [wl_clinic_code], [Appointment_Type],
			[See_by_date], op_new.[Cons_yn], op_new.[Diag_yn],
			op_new.[planned_yn], wlist_type = 'New', pri.[description],
			op_new.list_name, op_new.LocalCategory
	FROM [InfoDB].[dbo].[vw_outpatient_ptl] op_new
	LEFT JOIN [InfoDB].[dbo].[vw_cset_specialties] spec
	ON spec.[local_spec] = op_new.[local_spec]
	LEFT JOIN [InfoDB].[dbo].[vw_wlist_op] new_wl
	ON new_wl.[wlist_refno] = op_new.[wlist_refno]
	LEFT JOIN [PiMSMarts].[dbo].[cset_prityr] pri
	ON pri.[identifier] = op_new.[Priority]
	
	UNION ALL

	--FUs
	SELECT op_fu.[pasid], op_fu.[postcode] pcode, [local_spec_desc],
	       op_fu.[clinic_code], [wl_clinic_code], [Appointment_Type],
		   op_fu.[See_by_date], op_fu.[Cons_yn], op_fu.[Diag_yn],
		   op_fu.[planned_yn], wlist_type = 'FU', pri.[description],
		   op_fu.list_name, op_fu.Local_Category
	FROM [InfoDB].[dbo].[vw_outpatient_fup_ptl] op_fu
	LEFT JOIN [InfoDB].[dbo].[vw_cset_specialties] spec
	ON spec.[local_spec] = op_fu.[local_spec]
	LEFT JOIN [InfoDB].[dbo].[wlist_op_fu_daily_snapshot] fu_wl
	ON fu_wl.[wlist_refno] = op_fu.[wlist_refno]
	LEFT JOIN [PiMSMarts].[dbo].[cset_prityr] pri
	ON pri.[identifier] = op_fu.[Priority]) wlist
	
	--Join all of the virtual clinics
	/*LEFT JOIN (SELECT	ClinicCode
	             FROM pimsmarts.dbo.MasterClinicList
	             WHERE ClinicLocation = 'Telephone and Video Contact'
	             OR is_tele = 1
	             OR is_virtual = 1
	             OR is_video_clinic = 1) virtual_clinics
	ON virtual_clinics.ClinicCode = wlist.clinic_code*/
	
	--select only those who don't have a ClinicCode value (those who are seen at an in-person clinic)
	WHERE --ClinicCode IS NULL
	--AND 
	((Appointment_Type <> 'TEL' AND Appointment_Type <> 'VDO' 
	  AND Appointment_Type <> 'VRT') OR Appointment_Type IS NULL)"""

	opats = pd.read_sql_query(pat_sql, engine)
	engine.dispose()
	#Replace double spaces between postcode sections with single spaces for consistency
	opats['pcode'] = opats['pcode'].str.replace('  ', ' ')
	#Replace Urology - Urodynamics with just Urology as requested by GH 26/04/22
	opats['local_spec_desc'] = opats['local_spec_desc'].replace({'Urology - Urodynamics':'Urology'})
	#Update the Appointment_Type column. If NULL, replace with 'Unknown'
	opats['Appointment_Type'] = opats['Appointment_Type'].fillna('Unknown')
	return opats

###############################################################################
                            #DATA ANALYSIS#
###############################################################################
#Make a function to replace specialty names for peripheral and Derriford 
#clinics, so that they match with local_spec_descs
def specMatch(df):
    df['ClinicSpecialty'] = df['ClinicSpecialty'].replace({
        'Ear Nose Throat': 'ENT',
        'Arterial/Vascular': 'Arterial Vascular',
        'Anaesthetics / Epidural': 'Anaesthetics',
        'Andrology (Urology)': 'Andrology',
        'Cardiology Non-Cons': 'Cardiology Non Cons',
        'Diag Lung Function': 'Diagnostic Lung Function',
        'Diag Cardiology': 'Diagnostic Cardiology',
        'Emergency Medicine': 'Accident & Emergency',
        'Fracture Clinic':'Trauma',
        'Health Care For The Elderly':'Geriatric Medicine',
        'Oncology Non-Cons': 'Clinical Oncology',
        'Diag Neurophysiology': 'Diagnostic Neurophysiology',
        'Diag Ophthalmology': 'Diagnostic Ophthalmology',
        'Diag Urology': 'Diagnostic Urology',
        'Hepatobiliary and Pancreatic Surgery': 'Hepatobiliary & Pancreatic Surgery',
        'Maxillo-Facial Surgery': 'Maxillo Facial Surgery',
        'Ophthalmology A and E': 'Ophthalmology A&E',
        'Paed Palliative Care':'Paed Palliative',
        'Paediatric Community Dentistry': 'Community Dentistry',
        'Paediatric Diabetic Medicine':'Paediatric Medicine',
        'Pain Management': 'Pain Clinic',
        'Pain Mgmt Prog Non-Cons': 'Pain Management Clinic',
        'Plastic Surgery (Hand)': 'Plastic Surgery (Hands)',
        'Psychology (Child)': 'Psychology - Child Health',
        'Psychology (General)': 'Psychology(Gen)',
        'Speech and Language Therapy': 'Speech & Language Therapy',
        'Uro-Gynae (Gynae)': 'Uro - Gynae',
        'Uro-Inf.(Gynae)': 'Uro - Inf',
        'Orthopaedic' : 'Orthopaedics'})
    return df

def rescuePcode(df, df_tomatch):
    counter = 1
    while df.shape[0] > 0:
        #Add in a break clause here where the postcode can't be shorter than the regional bit (the bit before the space) 
        if counter > 3:
            break
        #Remove one character from the string
        df['reduced_pcode'] = df['pcode'].str[:-counter]
        #Check if this matches with any of the postcodes in pcode_latlon df
        #First, remove the last character from the conversion df
        df_tomatch['reduced_pcode'] = df_tomatch['pcds'].str[:-counter]
        df['pcode_matched'] = df['reduced_pcode'].isin(df_tomatch['reduced_pcode'])
        #If the postcodes match, update the original dataframe with the postcodes and the lat-long
        df_match = df.loc[df['pcode_matched'] == True].copy()
        #To merge the new dataframe, we need to first drop the duplicated values
        #This will only keep the first instance of the reduced postcode
        df_tomatch_nodups = df_tomatch.drop_duplicates(subset='reduced_pcode')
        #Merge in the new postcode and lat/long as well as LSOA code
        df_matches = df_match.merge(df_tomatch_nodups, on='reduced_pcode', how='left')
        #Now update the returned df
        if counter == 1:
            df_match_all = df_matches
        else:
            df_match_all = df_match_all._append(df_matches, ignore_index = True)
        #Now, remove the matched values from the original df
        df.drop(index = df_match.index, inplace = True)
        counter = counter +1
    return df_match_all
    
def run_main_analysis(opats):

    engine = create_engine('mssql+pyodbc://@SDMartDataLive2/InfoDB?'\
						   'trusted_connection=yes&driver=ODBC+Driver+17'\
						   '+for+SQL+Server')
    
    #Read in perripherals data and tidy up specialties
    query = """SELECT [ClinicCode], [ClinicLocation], [ClinicSpecialty] 
      FROM [PiMSMarts].[dbo].[MasterClinicList]
      WHERE clinicEndDateTime is NULL
      AND ClinicLocation NOT LIKE '%Derriford Hospital%'
      AND ClinicLocation NOT LIKE '%Telephone and Video%'
      AND ClinicLocation NOT LIKE '%Telephone%'
      AND ClinicLocation NOT LIKE '%Virtual%'
      AND ClinicLocation NOT LIKE '%Home (PHNT)%'
      AND ClinicLocation NOT LIKE '%Various Educational Establishments%'
      AND ClinicLocationCode NOT LIKE 'RK950%'
      AND ClinicLocationCode NOT LIKE 'RH8DF03' -- Mustard tree centre on DH site, but different Location code
      AND ClinicLocationCode NOT LIKE 'RK9OTH98' -- Centre for Health and wellbeing on DH site
      AND ClinicLocationCode NOT LIKE 'RH8DF06' -- Freedom Unit on DH site
      AND is_virtual = 0
      AND is_tele = 0
      ORDER BY ClinicLocation"""
    peripherals = pd.read_sql(query, engine) 
    peripherals = specMatch(peripherals)
    #Make the df have one row for each clinic and a list of all specialties
    #and codes available in the ClinicSpecialty column   
    per_uni = (peripherals.groupby('ClinicLocation', as_index=False)
                [['ClinicSpecialty', 'ClinicCode']].agg(lambda x: list(x)))
    #Get the postcode for each clinic, if there is white space at the start or
    #a comma, remove it
    per_uni['pcode'] = (per_uni['ClinicLocation'].str[-8:]
                        .apply(lambda x: (x.strip().replace(',','')).upper()
                                if isinstance(x, str) else x))
    #Manually redefine the postcode for the clinics that don't have a postcode
    per_uni['pcode'] = per_uni['pcode'].replace(
                                  {"WR (RH8)":"PL6 5WR",
                                   "E SCHOOL":"PL6 8UN",
                                   "LEVEL 2":"EX31 4JB",
                                   "LD (RK9)":"PL6 8BG",
                                   "SALTASH":"PL12 6DL",
                                   "TQ2 4FE":"TQ2 7FF",
                                   "PLYMOUTH":"PL4 7PY",
                                   "H PL52LN":"PL5 2LN",
                                   "K DEVON":"PL19 8LD"})
    #group by postcode and location
    per_uni = ((per_uni.groupby([per_uni.pcode, per_uni.ClinicLocation.str[:4]],
                               as_index=False).agg({'ClinicLocation':'first',
                                                    'ClinicSpecialty':'sum',
                                                    'ClinicCode':'sum'}))
                        .sort_values(by='ClinicLocation', ignore_index=True))
    #Make the lists into long strings so that they are searchable
    per_uni['ClinicCode'] = per_uni['ClinicCode'].apply(' '.join)
    per_uni['ClinicSpecialty'] = per_uni['ClinicSpecialty'].apply(' '.join) 
   
    #Read in derriford clinics and tidy up specialties
    d_query = """SELECT [ClinicCode], [ClinicLocation], [ClinicSpecialty]
                FROM [PiMSMarts].[dbo].[MasterClinicList]
                where clinicEndDateTime is NULL
                and (ClinicLocation like '%Derriford Hospital%'
                    or ClinicLocationCode = 'RH8DF03' -- Mustard tree centre on DH site, but different Location code
                    or ClinicLocationCode = 'RK9OTH98' -- Centre for Health and wellbeing on DH site
                    or ClinicLocationCode = 'RH8DF06' -- Freedom Unit on DH site
                    or ClinicLocationCode like 'RK950%')
                and ClinicLocation not like '%Telephone and Video%'
                and ClinicLocation not like '%Telephone%'
                and ClinicLocation not like '%Virtual%'
                and ClinicLocation not like '%Home (PHNT)%'
                and ClinicLocation not like '%Various Educational Establishments%'
                and is_virtual = 0
                and is_tele = 0
                order by ClinicLocation"""
    derrifords = pd.read_sql(d_query, engine) 
    derrifords = specMatch(derrifords)
    #For each patient, find whether they are booked into Derriford, or due to be
    #If patient has a clinic code then use that, or use the due to be clinic if
    #not, then check if this is derriford or not.
    opats['Appt_Booked'] = ~opats['clinic_code'].isna()
    opats['CurrentClinic'] = np.where(opats['Appt_Booked'], opats['clinic_code'],
                                      opats['wl_clinic_code'])
    #Find patients due to be seen at Derriford
    opats['Derriford(Y/N)'] = opats['CurrentClinic'].isin(derrifords['ClinicCode'])
    
    #Import the required LSOA Data to join on postcode
    #Get the postcode to LSOA conversion
    #Import the newer (May 2021) postcode, LSOA and lat-lon data from the ONS file
    pcode_LSOA_LL = pd.read_csv("G:/PerfInfo/Performance Management/PIT Adhocs/2021-2022/Hannah/Maps/pcode_LSOA_latlong.csv",
                                usecols = ['pcds','lsoa11', 'lat', 'long'])
    #Merge each df onto the LSOA data and record the postcodes that don't match.
    #use rescuePcode to fix
    df_list = [opats, per_uni]
    missing = []
    for i, df in enumerate(df_list):
        df = df.merge(pcode_LSOA_LL, left_on='pcode', right_on='pcds', how='left')
        mask_missing = df['lsoa11'].isnull() | df['lat'].isnull()
        no_data = df[mask_missing]
        missing += no_data['pcode'].unique().tolist()
        df_list[i] = df
    matches = rescuePcode(pd.DataFrame({'pcode':list(set(missing))}),
                          pcode_LSOA_LL)

    #From these rescued postcodes, update the dataframes with the new LSOA data
    for i, df in enumerate(df_list):
        df = df.merge(matches[['lsoa11', 'lat', 'long', 'pcode']],
                              on='pcode', how='left')
        df['lsoa11'] = df['lsoa11_x'].fillna(df['lsoa11_y'])
        df['latitude'] = df['lat_x'].fillna(df['lat_y'])
        df['longitude'] = df['long_x'].fillna(df['long_y'])
        #Drop excess columns
        df = df.drop(['lsoa11_x', 'lsoa11_y', 'lat_x', 'lat_y', 'long_x',
                      'long_y', 'pcds'], axis = 1)
        #Also make columns with northing and easting
        BNG_E, BNG_N = ([] for i in range(2))
        #Loop through the rows as WGS84toOSGB36 only accepts single values
        for index, row in df.iterrows():
            #Change from lat-lon to northing and easting
            BNG_E_temp, BNG_N_temp = WGS84toOSGB36(row['latitude'], row['longitude'])
            BNG_E.append([BNG_E_temp])
            BNG_N.append([BNG_N_temp])
        #Make new rows in the dataframe for these easting and northing values
        df['BNG_E'] = BNG_E
        df['BNG_N'] = BNG_N
        df_list[i] = df

    opatsFull, per_uniFull = df_list
    #Find whether or not the postcode has been found in the latlong file
    opatsFull['pcode_missing'] = opatsFull['pcode'].isin(missing) 

    return derrifords, peripherals, opatsFull, per_uniFull, per_uni

###############################################################################
                                #TRAVEL TIMES#
###############################################################################

def currentClinicDetails(all_travel_times, derr_travel_times, per_uni, clinics,
                         opatsFull):
    #Function to get the current clinic name and travel time for each patient.
    #turn relevant columns into a list of lists to iterate over and create
    #empty output list
    curr_clinic_input = opatsFull[['CurrentClinic', 'Derriford(Y/N)', 'pcode',
                                'pcode_missing', 'lsoa11']].values.tolist()
    curr_clinic_output = []
    #Iterate over each patient and find the clinic and the travel time.
    for row in curr_clinic_input:
        clinic_code, derr_bool, pcode, pcode_missing, lsoa = row
        #if no clinic or invalid postcode
        if ((pd.isnull(clinic_code)) or (pd.isna(lsoa)) or (pcode_missing)
            or (pcode[0:2] not in ['PL', 'EX', 'TR','TQ'])):
            curr_clinic_output.append((None, None))
        #If derriford, return that
        elif derr_bool:
            curr_clinic_output.append(('Derriford',
                                       derr_travel_times.loc[pcode]))
        else:
            #First, get the clinic location
            locat = per_uni.loc[per_uni['ClinicCode'].str
                                .contains(fr'\b{clinic_code}\b'),
                                'ClinicLocation']
            locat = [item.split(",")[0] for item in locat]
            #if the clinic isn't in Derriford or the list of peripheral clinics, 
            #assign None
            if len(locat) == 0:
                curr_clinic_output.append((None, None))
            #If the clinic exists in the new dataset, but not the old one, return 
            #the name but not the travel time
            elif locat[0] not in clinics:
                curr_clinic_output.append((locat[0], None))
            else:
                curr_clinic_output.append((locat[0],
                                        float(all_travel_times
                                                .loc[pcode, locat].values[0])))
    return curr_clinic_output

def closestSpecClinicDetails(all_travel_times, derr_travel_times, opatsFull, per_dh_uni):
    #Function to get a list of the closest spec name and travel time for each
    #patient
    closest_spec_input = opatsFull[['local_spec_desc', 'pcode',
                                    'pcode_missing']].values.tolist()
    closest_spec_output = []
    for row in closest_spec_input:
        spec_desc, pcode, pcode_missing = row
        #If the postcode doesn't exist in the travel times df, return nothing
        if (pcode_missing) or (pcode[0:2] not in ['PL', 'EX', 'TR','TQ']):
            closest_spec_output.append((None, None))
        else:
            per_spec_clinics = (per_dh_uni.loc[per_dh_uni['ClinicSpecialty'].str
                                               .contains(spec_desc, regex=False),
                                               'ClinicLocation'])
            #If no peripheral clinics or if only per clinic is REI, return DH
            if ((len(per_spec_clinics) == 0)
                or ((len(per_spec_clinics) == 1) and 
                    (per_spec_clinics.values[0] ==
                    'Royal Eye Infirmary, 3 Alpha Way, Derriford, Plymouth, PL6 5ZF'))):
                closest_spec_output.append(('Derriford',
                                            derr_travel_times.loc[pcode]))
            else:
                #get the short names for the clinics that offer this specialty
                per_spec_clinics = [item.split(",")[0]
                                    for item in per_spec_clinics]
                try:
                    #Make a smaller version of the travel times df, containing
                    #only these specs
                    spec_tt = all_travel_times.loc[pcode]
                    spec_tt = spec_tt[[i for i in per_spec_clinics
                                       if i in spec_tt.index]]
                    #append lowest travel time result
                    closest_spec_output.append((spec_tt.idxmin(), spec_tt.min()))
                except:
                    closest_spec_output.append((None, None))
    return closest_spec_output

def createIdealandFutureIdealPatients(opatsFull, clinicCode, clinic, clinicSpecialty):
    #Function to find the number of patients who are closest to a given clinic and
    #waiting for a specific specialty, and are not already booked into the given 
    #clinic (iterating over list of lists did not speed this up, the .loc is
    #slow)for all clinics on then only for clinics that do not currently
    # offer this spec
    if pd.isnull(clinicCode):
        #if no clinic code, return number of patients closest to clinic
        #that do not currentlyoffer this specialty
        pats = opatsFull.loc[(opatsFull['Clinic'] != clinic)
                            & (opatsFull['closest_clinic'] == clinic)
                            & (opatsFull['local_spec_desc'] == clinicSpecialty)
                            & (pd.isnull(clinicCode)), 'time_diff_closest']
        return None, None, None, len(pats), pats.sum(), pats.mean()
    else:
        #return number of patients closest to the clinic and not booked in.
        pats = opatsFull.loc[(opatsFull['Clinic'] != clinic)
                            & (opatsFull['closest_spec_clinic'] == clinic)
                            & (opatsFull['local_spec_desc'] == clinicSpecialty),
                            'time_diff_spec']
        #return number of patients, total time saved, mean time saved
        return len(pats), pats.sum(), pats.mean(), None, None, None

def run_travel_times(derrifords, peripherals, opatsFull, per_uniFull, per_uni):
    #Bring in the travel times between patient postcodes and peripheral clinics
    travel_times = pd.read_csv('G:/PerfInfo/Performance Management/PIT Adhocs/'\
                               '2021-2022/Hannah/PeripheralClinic/travel_times_'\
                                   'raw.csv', index_col = 0)
     #Get a list of the clinics and only keep what's before the comma, remove
    #duplicates
    clinics_used = per_uniFull['ClinicLocation'].tolist()
    clinics_used = list(set([item.split(",")[0] for item in clinics_used]))

    #Use this list to assign the column names. Adjust for old clinics that have
    #since stopped, and new clinics that have recently begun. First, find
    #clinics that are not longer running load original clinics
    orig_clinics = pd.read_csv(r"G:/PerfInfo/Performance Management/PIT Adhocs/"\
                       "2021-2022/Hannah/PeripheralClinic/OriginalClinics.csv")
    #Get a list of all of the clinics
    clinics = orig_clinics['Clinics'].to_list()
    #Add postcode and Derriford to the list
    clinics.insert(0, 'pcode')
    clinics.insert(len(clinics)+1, 'Derriford')
    #Reassign column names
    travel_times.columns = clinics
    #find and remove the clinics that are no longer running
    rem_clins = orig_clinics[~orig_clinics.isin(clinics_used)].dropna()
    rem_cols = rem_clins['Clinics'].tolist()
    travel_times = travel_times.drop(columns=rem_cols)
    #pick the rows that are needed based on patient postcodes.  Find unique
    #patient postcodes where lat and long exist
    opats_pcodes = opatsFull.loc[~opatsFull['latitude'].isnull(),
                                 'pcode'].unique().tolist()
    travel_times = travel_times.loc[travel_times['pcode']
                                    .isin(opats_pcodes)].copy()
    
    #For each patient's postcode, find the closest clinic, and the travel time 
    #to that clinic
    travel_times['closest_clinic'] = (travel_times.iloc[:,1:].astype(float)
                                      .idxmin(axis = "columns"))
    travel_times['closest_clinic_time'] = travel_times.min(axis=1,
                                                           numeric_only=True)
    #Merge Derriford travel time, closest clinic and time for closest clinic to
    #opats_full on patient postcode. Keep all patient details so left merge
    opatsFull = opatsFull.merge(travel_times[['pcode', 'Derriford',
                                              'closest_clinic',
                                              'closest_clinic_time']],
                                              on='pcode', how='left')
    #set index of travel times to postcode and create derriford version
    #to speed up lookups in below functions.
    all_travel_times = travel_times.set_index('pcode').copy()
    derr_travel_times = all_travel_times['Derriford'].copy()

    #Get the current clinic name and time for each patient
    opatsFull[['current_clinic_name',
                'current_clinic_time']] = currentClinicDetails(all_travel_times,
                                                               derr_travel_times,
                                                               per_uni, clinics,
                                                               opatsFull)
    
    #Make a second version of per_uni, with Derriford also included.
    #First, get the derrifords all in one line
    derrifords['Clinic'] = 'Derriford'
    dh_clinics = (derrifords.groupby('Clinic')['ClinicSpecialty']
                  .apply(list).reset_index()
                  .rename(columns={'Clinic':'ClinicLocation'}))
    #Make a list of the clinic specialties separated by a space
    dh_clinics['ClinicSpecialty'] = dh_clinics['ClinicSpecialty'].apply(' '.join)
    #Concatenate this onto the end of per_uni
    per_dh_uni = pd.concat([per_uni, dh_clinics], ignore_index = True)
    #set index of travel times to postcode and create derriford version
    #to speed up lookups in below functions.
    all_travel_times = travel_times.set_index('pcode').copy()
    derr_travel_times = all_travel_times['Derriford'].copy()

    #Get the closest specalty for each patient and the travel time
    opatsFull[['closest_spec_clinic',
               'closest_spec_clinic_time']] = closestSpecClinicDetails(
                                              all_travel_times,
                                              derr_travel_times, opatsFull,
                                              per_dh_uni)
    #Make a df to show the results in. One row per clinic and specialty  
    #First find all of the specialties offerred by either the peripheral or 
    #derriford clinics
    results = pd.DataFrame(list(
              itertools.product(clinics[1:],
                    (derrifords['ClinicSpecialty'].unique().tolist()
                    + list(set(peripherals['ClinicSpecialty'].unique().tolist())
                           - set(derrifords['ClinicSpecialty'].unique().tolist())))
                           )),
               columns=['Clinic', 'ClinicSpecialty']) 
    #Before merging, update names of locations with similar duplicates
    for idx, loc in enumerate(peripherals['ClinicLocation']):
        if loc[-9:] == "5WR (RH8)":
            peripherals.iloc[idx, 1] = peripherals.iloc[idx, 1][:-6]
        elif loc[:22] =="Child Development Ctre":
            peripherals.iloc[idx, 1] = "Child Development Centre, Scott Business Park, Beacon Park Road, Plymouth, Devon PL2 2PQ"
        elif loc[:23] == "Liskeard Community Hosp":
            peripherals.iloc[idx, 1] = "Liskeard Community Hosp, Outpatient (Community), Clemo Road, Liskeard, Cornwall, PL14 3XA"
        elif loc[:12] == "Mount Gould,":
            peripherals.iloc[idx ,1] = "Mount Gould Local Care Centre, 200 Mount Gould Road, Plymouth, Devon PL4 7PY"
    #Add in a column to peripherals so that the clinic name is split by a comma
    #and add clinic column to the derriford data
    peripherals['Clinic'] = peripherals.apply(lambda x: x['ClinicLocation']
                                              .split(",")[0], axis=1)  
    derrifords['Clinic'] = 'Derriford'
    #Merge in the peripheral clinic details
    results = results.merge((pd.concat([peripherals, derrifords])
                             [['ClinicCode', 'ClinicSpecialty', 'Clinic']]),
                            on = ['Clinic','ClinicSpecialty'], how = 'left')
    #Find the number of patients currently booked into each clinic and merge
    #onto results
    pats_booked = (opatsFull.groupby(['clinic_code'])['clinic_code'].count()
                   .rename('Booked').reset_index()
                   .rename(columns={'clinic_code':'ClinicCode'}))
    results = results.merge(pats_booked, on='ClinicCode', how='left')
    #Find the patients due, but not yet booked in and merge onto results
    pats_due = (opatsFull[~opatsFull['Appt_Booked']].groupby(['wl_clinic_code'])
                ['wl_clinic_code'].count().rename('Due to Attend').reset_index()
                .rename(columns={'wl_clinic_code':'ClinicCode'}))
    results = results.merge(pats_due, on='ClinicCode', how='left')
    #Find the time saved if the patient were to be switched to the closest clinic
    #currently offerring the required specialty. Also find the time difference
    #if the patient were moved to their overall closest clinic
    opatsFull['time_diff_spec'] = (opatsFull['current_clinic_time']
                                   - opatsFull['closest_spec_clinic_time'])
    opatsFull['time_diff_closest'] = (opatsFull['current_clinic_time']
                                      - opatsFull['closest_clinic_time'])
    #Finally, find the clinic name of the clinic the patient is booked into (if any)
    #so that the ideal patients can be calculated based on whether or not they are
    #currently at this location for this spec
    opatsFull = opatsFull.merge((pd.concat([peripherals, derrifords])
                                 [['ClinicCode','Clinic']]
                                 .reset_index(drop=True)),
                                left_on='clinic_code', right_on='ClinicCode',
                                how='left').drop('ClinicCode', axis=1)
    #add columns for numbers of current and future ideal patients
    results[['Current Ideal Patients','Total Time Saved Current',
             'Mean Time Saved Current', 'Future Ideal Patients',
             'Total Time Saved Future',
             'Mean Time Saved Future']] = results.apply(lambda x:
                               createIdealandFutureIdealPatients(opatsFull,
                                                                 x['ClinicCode'],
                                                                 x['Clinic'],
                                                                 x['ClinicSpecialty']),
                                                    axis=1, result_type='expand')
    #Drop collumns to tidy up final output
    opatsFull = opatsFull.drop(['Appt_Booked', 'CurrentClinic',
                                'Derriford(Y/N)', 'lsoa11','latitude',
                                'longitude', 'BNG_E', 'BNG_N', 'pcode_missing',
                                'Derriford','Clinic'], axis = 1)
    #Find the number of patients within 15 minutes' drive from each clinic
    #Make an empty list to store each df in
    pats_15 = []
    for clinic in travel_times.columns[1:-2]:
        #Find all of the postcodes within 15 mins drive
        tt_pcodes = travel_times.loc[travel_times[clinic]<15, 'pcode']
        #Find the number of patients with these postocdes and group by spec
        pats_15_temp = (opatsFull.loc[opatsFull['pcode'].isin(tt_pcodes)]
                        .groupby(['local_spec_desc'])['pasid'].count()
                        .reset_index().rename(columns={
                            'local_spec_desc':'ClinicSpecialty',
                            'pasid':'Pateints Within 15'}))
        #Make a column with the name of the clinic
        pats_15_temp['Clinic'] = clinic
        #Append the new data to the lsit
        pats_15 += pats_15_temp.values.tolist()
        
    #Create dataframe of patients 15 mins away and merge onto results
    pats_15 = pd.DataFrame(pats_15,columns=['ClinicSpecialty',
                                            'Patients Within 15', 'Clinic'])
    results = results.merge(pats_15, how='left',
                            on=['Clinic', 'ClinicSpecialty'])

    return results, opatsFull

###############################################################################
                                #Mapping#
###############################################################################
def run_specialty_maps(results, per_uniFull, derrifords, peripherals):
    #Make an overall ideal patients column
    results['ideal_pats'] = (results['Current Ideal Patients'].fillna(0) 
                            + results['Future Ideal Patients'].fillna(0))
    #Drop all of the unnecessary columns and merge in shapefiles
    results.drop(results.columns[5:10], axis=1, inplace=True)
    #First, get the short name for each clinic in per_uniFull
    #so that this short name can be used to merge the BNG data into results
    per_uniFull['Clinic'] = per_uniFull.apply(lambda x: x['ClinicLocation']
                                              .split(",")[0], axis = 1)
    results = results.merge(per_uniFull[['BNG_E','BNG_N', 'lsoa11','Clinic',
                                         'latitude', 'longitude']], how='left',
                                         on='Clinic')
    #Remove the square brackets around the BNG data and convert to floats
    results['BNG_E'] = results['BNG_E'].apply(lambda x: str(x)[1:-1])
    results['BNG_N'] = results['BNG_N'].apply(lambda x: str(x)[1:-1])
    
    #Add in the missing Derriford data
    results.loc[results['Clinic'] == 'Derriford', 'lsoa11'] = 'E40000006'
    results.loc[results['Clinic'] == 'Derriford', 'BNG_E'] = '249586'
    results.loc[results['Clinic'] == 'Derriford', 'BNG_N'] = '59526'
    results.loc[results['Clinic'] == 'Derriford', 'latitude'] = 50.4163
    results.loc[results['Clinic'] == 'Derriford', 'longitude'] = -4.11849

    #Filter results to the correct specialties
    all_specs = (derrifords['ClinicSpecialty'].unique().tolist()
                 + list(set(peripherals['ClinicSpecialty'].unique().tolist())
                        - set(derrifords['ClinicSpecialty'].unique().tolist())))
    results = results.loc[(results['ClinicSpecialty'].isin(all_specs))]
    #Add in column to differentiate current and future patients, filter out those
    #who are neither.
    results['Current/Future'] = np.select(
                                #Current have clinic code
                                [~pd.isnull(results['ClinicCode']),
                                #Future has no clinic code, but has ideal pats
                                 ((pd.isnull(results['ClinicCode']))
                                  & (results['ideal_pats'] > 0))],
                                ['Current', 'Future'])
    #Filter to only required specialties, and current/future rows only.
    results = results.loc[(results['ClinicSpecialty'].isin(all_specs))
                          & (results['Current/Future'] != '0')].copy()
    #groupby clinic to get aggregate figures.
    results = (results.groupby(['Clinic', 'ClinicSpecialty', 'Current/Future'],
                               as_index=False).agg({'ideal_pats':'first',
                                                    'Booked':'sum',
                                                    'Due to Attend':'sum',
                                                    'latitude':'first',
                                                    'longitude':'first'}))
    results['Bubble Size'] = results['ideal_pats'].fillna(results['Booked']
                                                  + results['Due to Attend'])
    return results


###############################################################################
                                 #RUN CODE#
###############################################################################
#Run analysis in main function so only the required dataframes are returned.
def main():
    opatsFull = get_data()
    derrifords, peripherals, opatsFull, per_uniFull, per_uni = run_main_analysis(opatsFull)
    results, opatsFull = run_travel_times(derrifords, peripherals, opatsFull, per_uniFull, per_uni)
    results_mapping = run_specialty_maps(results.copy(), per_uniFull, derrifords, peripherals)
    opatsFull.columns = ['pasid', 'Patient Postcode', 'Specialty', 'ClinicCode', 'WL ClinicCode', 
    'Appointment Type', 'See by Date', 'Cons', 'Diag', 'Planned', 'OP Type', 'Priority', 'Waiting List Name',
    'Local Category', 'Closest Location', 'Time to Closest Location (mins)', 'Currrent Location',
    'Time to Current Location (mins)', 'Closest Specialty Location', 'Time to Closest Specialty Location (mins)',
    'Time Saved if Booked into Closest Specialty Location (mins)', 'Time Saved if Booked into Closest Location (mins)']
    return results, opatsFull, results_mapping

#results, opatsFull, results_mapping = main()