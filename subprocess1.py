#Load txt data file into postgres database using subprocess.run() and "COPY FROM" querry with returncode, stdout and stderr
import subprocess


# Define the psql copy command
command = [
    "psql",
    "-h",
    "localhost",
    "-U",
    "postgres",
    "-d",
    "processed_third_party",
    "-c",
    "\COPY third_party(MEDICAL_RECORD_NUMBER,PERSON_CODE,THIRD_PARTY_PAYER_ID,THIRD_PARTY_NAME,THIRD_PARTY_TYPE,GROUP_NUMBER,GROUP_NAME,INSURANCE_ID,INSURANCE_NAME) FROM 'processed_third_party.txt' DELIMITER '|' csv header"
   ]

# Execute the psql copy command using subprocess
#subprocess.run(command)
result = subprocess.run(command, text=False, shell=False, capture_output=True)
print(result)
print("File copied successfully")




