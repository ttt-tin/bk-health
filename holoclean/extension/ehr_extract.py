from datetime import datetime
import numpy as np
import pandas as pd
import json
import os
from tqdm import tqdm
import shutil
from upload import upload_file_to_nestjs_api
from dotenv import load_dotenv
from mapping import map_all_tables_from_folder

load_dotenv()

def getFirst(array):
    return array[0] if len(array) > 0 else pd.NA

def processOneFile(sample_df,
                    patient_df,
                    careplan_df,
                    condition_df,
                    diagnostic_report_df,
                    encounter_df,
                    immunization_df,
                    observation_df,
                    procedure_df):


    for index, row in sample_df.iterrows():
        try:
            tempdf=pd.json_normalize(row.entry)
            tempdf.columns = tempdf.columns.str.replace("resource.", "", regex=False)

            if str(tempdf['resourceType'][0])=="Patient":
                tempdf = processPatient(tempdf)
                frames = [patient_df, tempdf]
                patient_df = pd.concat(frames)

            elif str(tempdf['resourceType'][0])=="CarePlan":
                tempdf = processCareplan(tempdf)
                frames = [careplan_df, tempdf]
                careplan_df = pd.concat(frames)

            elif str(tempdf['resourceType'][0])=="Condition":
                tempdf = processCondition(tempdf)
                frames = [condition_df, tempdf]
                condition_df = pd.concat(frames)

            elif str(tempdf['resourceType'][0])=="DiagnosticReport":
                tempdf = processDiagnosticReport(tempdf)
                frames = [diagnostic_report_df, tempdf]
                diagnostic_report_df = pd.concat(frames)

            elif str(tempdf['resourceType'][0])=="Encounter":
                tempdf = processEncounter(tempdf)
                frames = [encounter_df, tempdf]
                encounter_df = pd.concat(frames)

            elif str(tempdf['resourceType'][0])=="Immunization":
                tempdf = processImmunization(tempdf)
                frames = [immunization_df, tempdf]
                immunization_df = pd.concat(frames)

            elif str(tempdf['resourceType'][0])=="Observation":
                tempdf = processObservation(tempdf)
                frames = [observation_df, tempdf]
                observation_df = pd.concat(frames)

            elif str(tempdf['resourceType'][0])=="Procedure":
                tempdf = processProcedure(tempdf)
                frames = [procedure_df, tempdf]
                procedure_df = pd.concat(frames)
        except Exception as e:
            print(f"Error processing row {index}: {e}")
            continue

    return patient_df,\
                    careplan_df,\
                    condition_df,\
                    diagnostic_report_df,\
                    encounter_df,\
                    immunization_df,\
                    observation_df,\
                    procedure_df

def cleanAndRename(patient_df,
                    careplan_df,
                    condition_df,
                    diagnostic_report_df,
                    encounter_df,
                    immunization_df,
                    observation_df,
                    procedure_df):
    try:
        for df in [patient_df, careplan_df, condition_df, diagnostic_report_df,
                    encounter_df, immunization_df, observation_df, procedure_df]:
            df.columns = df.columns.str.replace("resource.", "", regex=False)
            df.columns = df.columns.str.replace(".", "_", regex=False)
            df.drop(columns=['resourceType'], inplace=True)
        
        for df in [patient_df, condition_df, diagnostic_report_df, observation_df, encounter_df]:
            df['fullUrl']= df['fullUrl'].str.replace('urn:uuid:', '')
            
        for df in [encounter_df, immunization_df]:
            df['patient_reference'] = df['patient_reference'].str.replace('urn:uuid:', '')
            
        for df in [immunization_df, diagnostic_report_df, observation_df, procedure_df]:
            df['encounter_reference'] = df['encounter_reference'].str.replace('urn:uuid:', '')
            
        for df in [observation_df, procedure_df, careplan_df, condition_df, diagnostic_report_df]:
            df['subject_reference'] = df['subject_reference'].str.replace('urn:uuid:', '')

        for df in [careplan_df, condition_df]:
            df['context_reference'] = df['context_reference'].str.replace('urn:uuid:', '')
    except Exception as e:
        print(f"Error processing: {e}")
        
    return patient_df,\
                    careplan_df,\
                    condition_df,\
                    diagnostic_report_df,\
                    encounter_df,\
                    immunization_df,\
                    observation_df,\
                    procedure_df

def extractSubgroup(path):
    return path.split("/")[-1]

def extractGroup(path):
    return path.split("/")[-2]

def processPatient(patient_df):
    try:
        patient_df.drop(columns=['text.status', 'text.div', 'extension', 'photo'], inplace=True)
        patient_df['identifier'] = next((item['value'] for item in patient_df['identifier'] if 'type' in item and 
                                        any(coding.get('code') == 'SB' for coding in item['type']['coding'])), None)
        patient_df = pd.concat([pd.json_normalize({'name': next((item for item in patient_df['name'][0] if item.get("use") == "official"), None)}), patient_df.drop(columns=['name'])], axis=1)
        patient_df = pd.concat([pd.json_normalize({'telecom': next((item for item in patient_df['telecom'][0] if 'use' in item and item.get('use') == 'home'), None)}), patient_df.drop(columns=['telecom'])], axis=1)
        patient_df = pd.concat([pd.json_normalize({'address': next(item for item in patient_df['address'][0])}), patient_df.drop(columns=['address'])], axis=1)
        patient_df['maritalStatus.coding'] = patient_df.get('maritalStatus.coding', pd.NA).apply(lambda x: x[0]['code'] if isinstance(x, list) and len(x) > 0 else None) if 'maritalStatus.coding' in patient_df else pd.NA
        patient_df = patient_df.rename(columns={'maritalStatus.coding': 'maritalStatus'}).drop(columns=['telecom.extension', 'address.extension'])
        patient_df['name.given'] = patient_df['name.given'][0] if 'name.given' in patient_df.columns.tolist() and len(patient_df['name.given']) > 0 else pd.NA
        patient_df['name.prefix'] = patient_df['name.prefix'][0] if 'name.prefix' in patient_df.columns.tolist() and len(patient_df['name.prefix']) > 0 else pd.NA
        patient_df['address.line'] = ", ".join(patient_df['address.line'][0])
    except Exception as e:
        print(f"Error processing: {e}")
    return patient_df

def processCareplan(careplan_df):
    try:
        careplan_df['category'] = getFirst(getFirst(careplan_df['category'][0])['coding'])['display'] if 'category' in careplan_df.columns.tolist() and len(careplan_df['category']) > 0 and 'coding' in getFirst(careplan_df['category'][0]) else pd.NA
        careplan_df['addresses'] = careplan_df['addresses'][0][0]['reference'].replace('urn:uuid:', '') if 'addresses' in careplan_df.columns.tolist() and len(careplan_df['category']) > 0 else pd.NA
        careplan_df['activity'] = [[{'display': item['detail']['code']['coding'][0]['display'], 'status': item['detail']['status']} for item in careplan_df['activity'][0]]] if 'activity' in careplan_df.columns.tolist() and len(careplan_df['activity']) > 0 else pd.NA
    except Exception as e:
        print(f"Error processing: {e}")
    return careplan_df

def processCondition(condition_df):
    try:
        condition_df['code.coding'] = condition_df['code.coding'][0][0]['display'] if 'code.coding' in condition_df.columns.tolist() and len(condition_df['code.coding']) > 0 else pd.NA
        condition_df = condition_df.rename(columns={'code.coding': 'code'})
    except Exception as e:
        print(f"Error processing: {e}")
    return condition_df

def processDiagnosticReport(diagnosticReport_df):
    try:
        diagnosticReport_df['code.coding'] = diagnosticReport_df['code.coding'][0][0]['display'] if 'code.coding' in diagnosticReport_df.columns.tolist() and len(diagnosticReport_df['code.coding']) > 0 else pd.NA
        diagnosticReport_df = diagnosticReport_df.rename(columns={'code.coding': 'code'})
        diagnosticReport_df['performer'] = diagnosticReport_df['performer'][0][0]['display'] if 'performer' in diagnosticReport_df.columns.tolist() and len(diagnosticReport_df['performer']) > 0 else pd.NA
        diagnosticReport_df['result'] = [[item['display'] for item in diagnosticReport_df['result'][0]]] if 'result' in diagnosticReport_df.columns.tolist() and len(diagnosticReport_df['result']) > 0 else pd.NA
    except Exception as e:
        print(f"Error processing: {e}")
    return diagnosticReport_df

def processEncounter(encounter_df):
    try:
        encounter_df['type'] = encounter_df['type'][0][0]['text'] if 'type' in encounter_df.columns.tolist() and len(encounter_df['type']) > 0 else pd.NA
        encounter_df['reason.coding'] = encounter_df['reason.coding'][0][0]['display'] if 'reason.coding' in encounter_df.columns.tolist() and len(encounter_df['reason.coding']) > 0 else pd.NA
        encounter_df = encounter_df.rename(columns={'reason.coding': 'reason'})
    except Exception as e:
        print(f"Error processing: {e}")
    return encounter_df

def processImmunization(immunization_df):
    try:
        immunization_df['vaccineCode.coding'] = immunization_df['vaccineCode.coding'][0][0]['display'] if 'vaccineCode.coding' in immunization_df.columns.tolist() and len(immunization_df['vaccineCode.coding']) > 0 else pd.NA
        immunization_df = immunization_df.rename(columns={'vaccineCode.coding': 'vaccineCode'})
    except Exception as e:
        print(f"Error processing: {e}")
    return immunization_df

def processObservation(observation_df):
    try:
        observation_df['code.coding'] = observation_df['code.coding'][0][0]['display'] if 'code.coding' in observation_df.columns.tolist() and len(observation_df['code.coding']) > 0 else pd.NA
        observation_df = observation_df.rename(columns={'code.coding': 'code'})
        if 'component' in observation_df.columns.tolist() and len(observation_df['component']) > 0:
            observation_df['component'] = [[{'code': item['code']['coding'][0]['display'], 'value': item['valueQuantity']['value'], 'unit': item['valueQuantity']['unit']} for item in observation_df['component'][0]]]
        elif 'valueQuantity.value' in observation_df.columns.tolist():
            observation_df['component'] = [[{'code': observation_df['code'][0], 'value': observation_df['valueQuantity.value'][0].item(), 'unit': observation_df['valueQuantity.unit'][0]}]]
            observation_df.drop(columns=['valueQuantity.value', 'valueQuantity.unit', 'valueQuantity.system', 'valueQuantity.code'], inplace=True)
        else:
            observation_df['component'] = pd.NA
    except Exception as e:
        print(f"Error processing: {e}")
    return observation_df

def processProcedure(procedure_df):
    try:
        procedure_df = procedure_df.rename(columns={'code.text': 'code'})
        procedure_df.drop(columns=['code.coding'], inplace=True)
        procedure_df['reasonReference_reference'] = procedure_df['reasonReference_reference'].str.replace('urn:uuid:', '') if 'reasonReference_reference' in procedure_df.columns.tolist() else pd.NA
    except Exception as e:
        print(f"Error processing: {e}")
    return procedure_df

def generateUnstructuredMetadata(additional_data_path, output_folder):
    try:
        file_metadata = []
        for dirname, _, filenames in os.walk(additional_data_path):
            for filename in filenames:
                file_path = os.path.join(dirname, filename)
                file_extension = os.path.splitext(filename)[1][1:]  # Extract the file extension without the dot
                file_metadata.append({
                    "name": filename,
                    "size": os.path.getsize(file_path),  # File size in bytes
                    "type": file_extension,             # File type (extension)
                    "category": os.path.basename(dirname),  # Folder name as category
                    "url": file_path  # Full path to the file
                })
        
        # Save file metadata to CSV
        file_metadata_df = pd.DataFrame(file_metadata)
        file_metadata_csv_path = os.path.join(output_folder, "additional_file_metadata.csv")
        file_metadata_df.to_csv(file_metadata_csv_path, index=False)
        print(f"Additional file metadata has been saved to: {file_metadata_csv_path}")
    except Exception as e:
        print(f"Error processing: {e}")

def save_csv_to_folder(dataframe, unique_name, base_name, output_folder):
    # Accessing now() directly from datetime after importing datetime class
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_folder = os.path.join(output_folder, unique_name)
    os.makedirs(unique_folder, exist_ok=True)
    file_name = f"{base_name}_{timestamp}.csv"
    file_path = os.path.join(unique_folder, file_name)
    dataframe.to_csv(file_path, index=False)
    print(f"Saved CSV file: {file_path}")

def main():
    try:
        file_path_list = []
        csv_files = []
        for dirname, _, filenames in os.walk('data/structure'):
            for filename in filenames:
                # Add condition to filter only .json files
                if filename.endswith('.json'):
                    file_path_list.append((dirname, filename))
                elif filename.endswith('.csv'):
                    csv_files.append((dirname, filename))
        
        metadata_df = pd.DataFrame(file_path_list, columns=["folder", "file"])
        metadata_df["group"] = metadata_df["folder"].apply(lambda x: extractGroup(x))
        metadata_df["subgroup"] = metadata_df["folder"].apply(lambda x: extractSubgroup(x))
        metadata_df = metadata_df[["folder", "group", "subgroup", "file"]]
        sel_index = list(metadata_df.group.value_counts()[0:2].index)
        group_df = metadata_df.loc[metadata_df.group.isin(sel_index)]
        
        # Initialize empty DataFrames
        patient_df = pd.DataFrame() 
        careplan_df = pd.DataFrame() 
        condition_df = pd.DataFrame() 
        diagnostic_report_df = pd.DataFrame() 
        encounter_df = pd.DataFrame() 
        immunization_df = pd.DataFrame() 
        observation_df = pd.DataFrame() 
        procedure_df = pd.DataFrame() 
        
        # Process files
        for index, row in tqdm(group_df.iterrows(), total=group_df.shape[0]):
            folder = row["folder"]
            file = row["file"]
            # Only process if the file is a .json file
            if file.endswith('.json'):
                sample_df = pd.read_json(os.path.join(folder, file))
                patient_df,\
                careplan_df,\
                condition_df,\
                diagnostic_report_df,\
                encounter_df,\
                immunization_df,\
                observation_df,\
                procedure_df = processOneFile(sample_df, patient_df,
                                               careplan_df,
                                               condition_df,
                                               diagnostic_report_df,
                                               encounter_df,
                                               immunization_df,
                                               observation_df,
                                               procedure_df)
        
        # Clean and rename columns
        patient_df, careplan_df, condition_df, diagnostic_report_df, encounter_df, immunization_df, observation_df, procedure_df = cleanAndRename(
            patient_df, careplan_df, condition_df, diagnostic_report_df, encounter_df, immunization_df, observation_df, procedure_df
        )
        
        # Save each DataFrame to a separate CSV file
        output_folder = "./output"
        os.makedirs(output_folder, exist_ok=True)  # Ensure output folder exists

        for folder, filename in csv_files:
            src_path = os.path.join(folder, filename)
            dest_path = os.path.join(output_folder, filename)
            shutil.copy(src_path, dest_path)
            print(f"Copied CSV file: {src_path} -> {dest_path}")
        
        save_csv_to_folder(patient_df, "patient", "patient_data", output_folder)
        save_csv_to_folder(careplan_df, "careplan", "careplan_data", output_folder)
        save_csv_to_folder(condition_df, "condition", "condition_data", output_folder)
        save_csv_to_folder(diagnostic_report_df, "diagnostic_report", "diagnostic_report_data", output_folder)
        save_csv_to_folder(encounter_df, "encounter", "encounter_data", output_folder)
        save_csv_to_folder(immunization_df, "immunization", "immunization_data", output_folder)
        save_csv_to_folder(observation_df, "observation", "observation_data", output_folder)
        save_csv_to_folder(procedure_df, "procedure", "procedure_data", output_folder)
        
        additional_data_path = "./data/unstructure"
        generateUnstructuredMetadata(additional_data_path, output_folder)
        
        print("All DataFrames have been exported as CSV files in the 'output' folder.")
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
        region_name=os.getenv('AWS_REGION')

        # # Ví dụ sử dụng hàm
        input_folder_path = "./output"
        mapping_json_path = "mapping.json"
        map_all_tables_from_folder(input_folder_path, mapping_json_path)

        upload_file_to_nestjs_api('./standard', 'bk-health-raw-bucket')

    except Exception as e:
        print(f"Error processing: {e}")

if __name__ == "__main__":
    main()
