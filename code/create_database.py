
from genericpath import exists
import psycopg2
from psycopg2 import Error, OperationalError
from sqlalchemy import create_engine, text 
from sqlalchemy.exc import ResourceClosedError
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as pl
import datetime as dt


database=""  # TO BE REPLACED
user=''    # TO BE REPLACED
password='' # TO BE REPLACED
host=''
port= ''

# ***********************************
# .Create a database for next step  *
# ***********************************
connection = psycopg2.connect(
                                    database=database,              
                                    user=user,       
                                    password=password,   
                                    host=host
                                )
connection.autocommit = True
# Create a cursor to perform database operations
cursor = connection.cursor()
# Print PostgreSQL details
print("PostgreSQL server information")
print(connection.get_dsn_parameters(), "\n")
# Executing a SQL query
cursor.execute("SELECT version();")
# Fetch result
record = cursor.fetchone()
print("You are connected to - ", record, "\n")
cursor.close()

DIALECT = 'postgresql+psycopg2://'
database ='grp13_vaccinedist'
db_uri = "%s:%s@%s/%s" % (user, password, host, database)

print(DIALECT+db_uri)
engine = create_engine(DIALECT + db_uri)
psql_conn  = engine.connect()
if not psql_conn:
    print("DB connection is not OK!")
    exit()
else:
    print("DB connection is OK.")

# ***********************************
# *  Create and clean dataframes  *
# ***********************************

file_name = "vaccine-distribution-data.xlsx"
df_vaccine_type     = pd.read_excel(file_name, sheet_name="VaccineType")
df_vaccine_type.rename(columns={'ID':'vaccineid'}, inplace=True)

df_manufacturer    = pd.read_excel(file_name, sheet_name="Manufacturer")
df_manufacturer.rename(columns={'ID':'manufid', 'vaccine':'vaccineid'}, inplace=True)

df_vaccine_batch    = pd.read_excel(file_name, sheet_name="VaccineBatch")
df_vaccine_batch.rename(columns={'manufacturer':'manufid', 'location':'hospitalName'}, inplace=True)
df_vaccine_batch1 = df_vaccine_batch.copy(deep=True)
df_vaccine_batch.drop('type', axis=1, inplace=True)
df_vaccine_batch['manufDate'] = pd.to_datetime(df_vaccine_batch['manufDate'], errors='coerce')
df_vaccine_batch = df_vaccine_batch.dropna(subset=['manufDate'])

df_vaccination_stations     = pd.read_excel(file_name, sheet_name="VaccinationStations")

df_transportation_log    = pd.read_excel(file_name, sheet_name="Transportation log")
df_transportation_log['dateArr'] = pd.to_datetime(df_transportation_log['dateArr'], errors='coerce')
df_transportation_log = df_transportation_log.dropna(subset=['dateArr'])
df_transportation_log['dateDep'] = pd.to_datetime(df_transportation_log['dateDep'], errors='coerce')
df_transportation_log = df_transportation_log.dropna(subset=['dateDep'])

df_staff_members    = pd.read_excel(file_name, sheet_name="StaffMembers")
df_staff_members.rename(columns={'social security number':'ssNo', 'date of birth':'birthday', 'vaccination status':'vaccinationStatus', 'hospital':'hospitalName'}, inplace=True)
df_staff_members['birthday'] = pd.to_datetime(df_staff_members['birthday'], errors='coerce')
df_staff_members = df_staff_members.dropna(subset=['birthday'])

df_shifts     = pd.read_excel(file_name, sheet_name="Shifts")
df_shifts.rename(columns={'station':'hospitalName', 'worker':'ssNo'}, inplace=True)

df_vaccinations    = pd.read_excel(file_name, sheet_name="Vaccinations")
df_vaccinations.rename(columns={'location ':'hospitalName'}, inplace=True)

df_patients    = pd.read_excel(file_name, sheet_name="Patients")
df_patients.rename(columns={'date of birth':'birthday'}, inplace=True)
df_patients['birthday'] = pd.to_datetime(df_patients['birthday'], errors='coerce')
df_patients = df_patients.dropna(subset=['birthday'])

df_vaccine_patients     = pd.read_excel(file_name, sheet_name="VaccinePatients")
df_vaccine_patients.rename(columns={'patientSsNo':'ssNo', 'location':'hospitalName'},inplace=True)

df_symptoms    = pd.read_excel(file_name, sheet_name="Symptoms")

df_diagnosis    = pd.read_excel(file_name, sheet_name="Diagnosis") 
df_diagnosis.rename(columns={'patient':'ssNo'}, inplace=True)
df_diagnosis.insert(loc = 0,column = 'pk',value = range(len(df_diagnosis))) 
df_diagnosis['date'] = pd.to_datetime(df_diagnosis['date'], errors='coerce')
df_diagnosis = df_diagnosis.dropna(subset=['date'])

# *********************************
# *  Column's names to lowercase  *
# *********************************

df_list = [df_vaccine_type, df_manufacturer, df_vaccine_batch, df_vaccination_stations, df_staff_members, df_shifts, 
            df_vaccinations, df_patients, df_vaccine_patients, df_symptoms, df_diagnosis, df_transportation_log]

for df in df_list:
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.rstrip()
    df.columns = df.columns.str.lstrip()
    df = df.drop_duplicates()

# ***************************
# *  From dataframe to sql  *
# ***************************

df_vaccine_type.to_sql("vaccine", psql_conn, if_exists = 'replace', index=False)
df_manufacturer.to_sql("manufacturer", psql_conn, if_exists = 'replace', index=False)
df_vaccine_batch.to_sql("vaccinebatch", psql_conn, if_exists = 'replace', index=False)
df_vaccination_stations.to_sql("hospitalclinic", psql_conn, if_exists = 'replace', index=False)
df_transportation_log.to_sql("delivery", psql_conn, if_exists = 'replace', index=False)
df_staff_members.to_sql("staffmembers", psql_conn, if_exists = 'replace', index=False)
df_shifts.to_sql("shifts", psql_conn, if_exists = 'replace', index=False)
df_vaccinations.to_sql("vaccinationevents", psql_conn, if_exists = 'replace', index=False)
df_patients.to_sql("patients", psql_conn, if_exists = 'replace', index=False)
df_vaccine_patients.to_sql("vaccinepatients", psql_conn, if_exists = 'replace', index=False)
df_symptoms.to_sql("symptoms", psql_conn, if_exists = 'replace', index=False)
df_diagnosis.to_sql("diagnosis", psql_conn, if_exists = 'replace', index=False)

# ************************
# *  Read from sql file  *
# ************************

fd = open('create_database.sql', 'r')
sqlFile = fd.read()
fd.close()
sqlCommands = sqlFile.split(';')
for command in sqlCommands:
    try:
        tx_ = pd.read_sql_query(command, psql_conn)
    except OperationalError:
        print("Command skipped")
    except ResourceClosedError: 
        print("Skip to avoid errors because ALTER TABLE doesn't return rows")