# ---------- A majority is implemented with pandas library ------------

from genericpath import exists
import psycopg2
from psycopg2 import Error, OperationalError
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from pathlib import Path
# Must install package matplotlib
import matplotlib.pyplot as pl
import datetime as dt

from create_database import *


# *******************************************************************************
# *  Create new dataframe containing ssno, gender, dob, symptom, diagnosisdate  *
# *******************************************************************************

df_patients_symptoms = pd.merge(df_patients, df_diagnosis, how='outer', left_on='ssNo', right_on='ssNo')
df_patients_symptoms.rename(columns={'date':'diagnosisDate'},inplace=True)
df_patients_symptoms.drop(columns=['name','pk'], axis=1,inplace=True)
# Overview of the dataframe
df_patients_symptoms
df_patients_symptoms.to_sql("PatientSymptoms", psql_conn, index = True, if_exists = "replace")


# *************************************************************************************
# *  Create new dataframe containing ssno, date1, date2, vaccinetype1, vaccinetype 2  *
# *  Record patients' vaccination date and type, NULL if missing                      *
# *************************************************************************************

df=pd.merge(df_vaccinations, df_vaccine_batch1, how='inner', left_on='batchID',right_on='batchID')
df.drop(columns=['manufID', 'manufDate','expiration','amount','hospitalName_y'], axis=1, inplace=True)
df.rename(columns={'hospitalName_x':'hospitalName'}, inplace=True)
df1 = pd.merge(df_vaccine_patients, df, how='left', on=['date','hospitalName'])
df_patients_vaccines = df_patients.merge(df1, how='left', on='ssNo')
df_patients_vaccines.drop(columns=['name','birthday','gender','hospitalName','batchID'], inplace=True)
df_patients_vaccines.insert(1,"duplicate",df_patients_vaccines['ssNo'].duplicated())
df_patients_vaccines = df_patients_vaccines.pivot(index='ssNo', columns=['duplicate'], values=['date','type']).reset_index()
df_patients_vaccines.columns=['ssNo','date1','date2','vaccinetype1','vaccinetype2']
# Overview of the dataframe
df_patients_vaccines
df_patients_vaccines.to_sql("PatientVaccineInfo", psql_conn, index = True, if_exists = "replace")

# *******************************************************************************
# *  Create new dataframes (one for male, one for female) from PatientSymptoms  *
# *  Figure out the most common symptoms for each gender  *
# *******************************************************************************

sql_ = """
    SELECT *
    FROM PatientSymptoms 
    """
tt_ = pd.read_sql_query(sql_,psql_conn)
m = tt_
f = tt_

m = m.loc[m['gender']=='M']
f = f.loc[f['gender']=='F']

# The top three most common symptoms for males and females
m['symptom'].value_counts().head(3)
f['symptom'].value_counts().head(3)

# *************************************************************
# *  Create new datafrom from "Patient", add column ageGroup  *
# *************************************************************

df_patients_plus = df_patients.copy()
df_patients_plus["age"] = pd.to_datetime("today").year - df_patients_plus["birthday"].dt.year
bi_labels = ["0-9", "10-19","20-39","40-59","60+"] 
bi_cut_bins = [0,9,19,39,59,150]
df_patients_plus["ageGroup"] = pd.cut(df_patients_plus["age"], bins=bi_cut_bins, labels=bi_labels, include_lowest=True)
df_patients_plus.drop(columns=['age'], axis=1, inplace=True)
df_patients_plus

# ******************************************************************
# *  Add column for vaccination status for the previous dataframe  *
# ******************************************************************

df_patients_plus = df_patients_plus.merge(df_patients_vaccines, how='left', on='ssNo')
conditions = [
    pd.notna(df_patients_plus['vaccinetype1']) & pd.notna(df_patients_plus['vaccinetype2']),
    pd.notna(df_patients_plus['vaccinetype1']) & df_patients_plus['vaccinetype2'].isnull(),
    df_patients_plus['vaccinetype1'].isnull()]
choices = [2,1,0]
df_patients_plus["vaccinationStatus"] = np.select(conditions, choices, default='black')
df_patients_plus.drop(columns=['date1','date2','vaccinetype1','vaccinetype2'], inplace=True)
df_patients_plus

# ******************************************************************************
# *  Calculate the percentage of patients with 0,1,2 doses for each age group  *
# ******************************************************************************

agegroup_vaccination_percentage = df_patients_plus.groupby(['ageGroup','vaccinationStatus'])['ssNo'].count().reset_index()
agegroup_vaccination_percentage = agegroup_vaccination_percentage.pivot(index="vaccinationStatus", columns=['ageGroup'])
agegroup_vaccination_percentage = 100 * agegroup_vaccination_percentage['ssNo'] / agegroup_vaccination_percentage['ssNo'].sum()
agegroup_vaccination_percentage

# ************************************************************************
# *  Create new dataframe with columns of relative frequency of symptoms *
# ************************************************************************

df=pd.merge(df_vaccinations, df_vaccine_batch1, how='inner', left_on='batchID',right_on='batchID')
df.drop(columns=['manufID', 'manufDate','expiration','amount','hospitalName_y'], axis=1, inplace=True)
df.rename(columns={'hospitalName_x':'hospitalName'}, inplace=True)
df1 = pd.merge(df_vaccine_patients, df, how='left', on=['date','hospitalName'])
df_patients_vaccines2 = df_patients.merge(df1, how='left', on='ssNo')
df_patients_vaccines2.drop(columns=['date','name','birthday','gender','hospitalName','batchID'], inplace=True)

sql_ = """
    SELECT *
    FROM patientsymptoms 
    """
tt_ = pd.read_sql_query(sql_,psql_conn)
a = tt_
print(a)
a.drop(['birthday', 'gender','diagnosisDate'], axis=1, inplace=True)
a = pd.merge(a,df_patients_vaccines2,how='outer',left_on='ssNo',right_on='ssNo')
a.drop(['ssNo','index'], axis=1,inplace=True)
a=a.dropna(subset=['type'])
v1 = a.loc[a['type']=='V01']
v2 = a.loc[a['type']=='V02']
v3 = a.loc[a['type']=='V03']
c = pd.DataFrame()
v1 = v1['symptom'].value_counts(normalize=True)
v2 = v2['symptom'].value_counts(normalize=True)
v3 = v3['symptom'].value_counts(normalize=True)
for v in [v1,v2,v3]:
    for index, value in v.items():
        fre = "-"          
        if(value>=0.1): fre = 'very common'
        elif(value>=0.05): fre = 'common'
        elif(value>0): fre = 'rare'
        else: fre = "-"
        v[index] = fre    
c=c.assign(V01=v1)
c=c.assign(V02=v2)
c=c.assign(V03=v3)
c.fillna('-')

# *****************************
# * Minimize waste of vaccine *
# *****************************

sql_ = """
    SELECT *
    FROM VaccinePatients 
    """
tt_ = pd.read_sql_query(sql_,psql_conn)

df=pd.merge(df_vaccinations, df_vaccine_batch1, how='inner', left_on='batchID',right_on='batchID')
df.drop(columns=['manufID', 'manufDate','expiration','hospitalName_y'], axis=1, inplace=True)
df.rename(columns={'hospitalName_x':'hospitalName'}, inplace=True)
df1 = pd.merge(df_vaccine_patients, df, how='left', on=['date','hospitalName'])
df_patients_vaccines2 = df_patients.merge(df1, how='left', on='ssNo')
df_patients_vaccines2.drop(columns=['ssNo','batchID','type','name','birthday','gender'], inplace=True)
df_patients_vaccines2.dropna(subset=['date'],inplace = True)

count_series = df_patients_vaccines2.groupby(['date', 'hospitalName']).size()
#create to dict:
patients = {}
#new df:
patientsWithAmount = pd.DataFrame()

#save to dict
for i,v in count_series.items():
    patients[i] = v
df_patients_vaccines2=df_patients_vaccines2.reset_index(drop=True)

#save to df
for i,v in df_patients_vaccines2.iterrows():
    if((v['date'],v['hospitalName']) in patients.keys()):   
        p = patients[(v['date'],v['hospitalName'])]
        new_row = {'percentage':(p/v['amount']),'date':v['date'],
                   'hospitalName':v['hospitalName'], 'patients':p,
                   'amount':v['amount']}
        patientsWithAmount = patientsWithAmount.append(new_row, ignore_index=True)        

patientsWithAmount=patientsWithAmount.drop_duplicates().reset_index(drop=True)
std = patientsWithAmount['percentage'].std()

total = 0
count = 0
for i,v in patientsWithAmount.iterrows():
    total += v['percentage']
    count += 1
result = total/count
final_result = result + std

print("The expected percentage of patients that will attend: ", result)
print("The standard deviation of the percentage of attending patients: ", std)
print("The result as percentage:",final_result)
patientsWithAmount

# ********************************************************************************************
# * Plot total number of vaccinated patients and total number of 2-dose patients w.r.t date  *
# ********************************************************************************************

df_vaccine_patients_group = df_vaccine_patients.copy().drop_duplicates(subset=['ssNo'])
df_vaccine_patients_group = df_vaccine_patients_group.groupby(["date"])['ssNo'].count()
df_vaccine_patients_group = df_vaccine_patients_group.cumsum()
pl.figure()
plot1 = df_vaccine_patients_group.plot(label="no of vac. patients", title = "total number of vaccinated patients")
df_vaccine_patients_group2 = df_vaccine_patients.copy()
df_vaccine_patients_group2 = df_vaccine_patients_group2[df_vaccine_patients_group2.duplicated(['ssNo'], keep = 'first')]
df_vaccine_patients_group2 = df_vaccine_patients_group2.groupby(['date'])['ssNo'].count()
df_vaccine_patients_group2 = df_vaccine_patients_group2.cumsum()
df_vaccine_patients_group2.plot(ax=plot1, label="no of fully vac. patients")
pl.legend()
plot1

# **********************************************************
# * nurse "19740919-7140" diagnosed positive, find all F1s *
# **********************************************************

# Find how many days a week the staff with ssNo = 19740919-7140 works
weekday_shift = df_shifts[df_shifts['ssNo']=='19740919-7140']
weekday_shift = weekday_shift['weekday']
print(weekday_shift)
# Find the workplacce of the staff with ssNo = 19740919-7140
workplace = df_staff_members[df_staff_members['ssNo']=='19740919-7140']
# Get the actual string name of the workpalce
workplace = workplace['hospitalName'].iloc[0] 
# filter all the staffs who works in same workplace. The schedule is weekly so any staff working will in the checking period of 10 days
staff_ssno = df_shifts[df_shifts['hospitalName']==workplace] 
# remove duplicates
staff_ssno = staff_ssno.drop_duplicates(['ssNo']) 
# merge the list of staff's ssNo with the list of all staff to get their name
staff_list = df_staff_members.merge(staff_ssno, how='inner', on='ssNo')
# the list contains only their ssNo and name
staff_list = staff_list[['ssNo','name']]
print(staff_list)

# mark the first date of the checking period
starting_checkpoint = pd.to_datetime('2021-05-15') - pd.Timedelta("10 days")
# filter all patients who were at the same hospital, any duplicates are also dropped
patient_ssno = df_vaccine_patients[df_vaccine_patients['hospitalName']==workplace].drop_duplicates(['ssNo']) 
# filter all the patients who were within 10 days of 2021-05-15
patient_ssno = patient_ssno[patient_ssno['date']>=starting_checkpoint] 
# making sure that the patients were before 2021-05-15 (even though the excel doesn't have any exceeding this point)
patient_ssno = patient_ssno[patient_ssno['date']<='2021-05-15']  
# merge the list of patients' ssNo with the list of all patients to get their name
patient_list = patient_ssno.merge(df_patients, how='inner', on='ssNo')
# the list contains only their ssNo and name
patient_list = patient_list[['ssNo','name']]
print(patient_list)

 
