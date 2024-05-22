import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 
from openai import AzureOpenAI #for using openai services 
from azure.data.tables import TableServiceClient, TableClient, UpdateMode # in order to use azure storage table  
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError # in order to use azure storage table  exceptions 
import csv #helping convert json to csv

# Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'


# Generic Function to update case  in the 'cases' table
def update_case_generic(caseid,field,value):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute(f"UPDATE cases SET {field} = ? WHERE id = ?", (value, caseid))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info(f"case {caseid} updated field name: {field} , value: {value}")
        return True
    except Exception as e:
        logging.error(f"Error update case: {str(e)}")
        return False    

#for each clinicArea the function create new entity or updates/appending  the entity with related csv row 
def Csv_Consolidation_by_clinicArea(csv_string,caseid,table_name,pagenumber):
    logging.info(f"starting Csv_Consolidation_by_clinicArea function")
    # Create a TableServiceClient using the connection string
    service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

    # Get a TableClient for the specified table
    table_client = service_client.get_table_client(table_name=table_name)

    # Reading the CSV data into a list of dictionaries
    csv_reader = csv.DictReader(io.StringIO(csv_string))
    records = list(csv_reader)
    
    # Group records by clinicalarea
    grouped_records = {}
    for record in records:
        clinicalarea = record['clinicalarea']
        if clinicalarea not in grouped_records:
            grouped_records[clinicalarea] = []
        grouped_records[clinicalarea].append(record)
    
    # Iterate over grouped records to update or insert into Azure Table Storage
    for clinicalarea, records in grouped_records.items():
        row_key = clinicalarea.replace(" ", "_")
        
        # Prepare the new CSV content
        output = io.StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=csv_reader.fieldnames)
        csv_writer.writeheader()
        csv_writer.writerows(records)
        new_content_csv = output.getvalue()

        try:
            # Try to get the existing entity
            entity = table_client.get_entity(partition_key=caseid, row_key=row_key)
            existing_content_csv = entity['contentCsv']
            logging.info(f"fun:Csv_Consolidation_by_clinicArea:check if entity existing")
            # Append the new records to the existing CSV content
            if existing_content_csv.strip():  # Check if existing content is not empty
                logging.info(f"fun:Csv_Consolidation_by_clinicArea:entity existing")
                existing_content_io = io.StringIO(existing_content_csv.replace('\\n', '\n'))
                existing_csv_reader = csv.DictReader(existing_content_io)
                combined_output = io.StringIO(newline='')
                combined_csv_writer = csv.DictWriter(combined_output, fieldnames=csv_reader.fieldnames)
                combined_csv_writer.writeheader()
                combined_csv_writer.writerows(existing_csv_reader)
                combined_csv_writer.writerows(records)
                final_content_csv = combined_output.getvalue()
            else:
                final_content_csv = new_content_csv
            # Encode the CSV string to preserve newlines
            encoded_content_csv = final_content_csv.replace('\n', '\\n')
            entity['contentCsv'] = encoded_content_csv
            # Update the pages column
            if 'pages' in entity:
                pages = entity['pages'].split(', ')
                if str(pagenumber) not in pages:
                    pages.append(str(pagenumber))
                entity['pages'] = ', '.join(pages)
            else:
                entity['pages'] = str(pagenumber)

            table_client.update_entity(entity, mode=UpdateMode.REPLACE)
            logging.info(f"fun:Csv_Consolidation_by_clinicArea:table updated")
        except ResourceNotFoundError:
            # If the entity does not exist, create a new one
            logging.info(f"fun:Csv_Consolidation_by_clinicArea:entity Not existing create new entity")
            # Encode the CSV string to preserve newlines
            encoded_content_csv = new_content_csv.replace('\n', '\\n')
            new_entity = {
                "PartitionKey": caseid,
                "RowKey": row_key,
                "contentCsv": encoded_content_csv,
                "pages": str(pagenumber)
            }
            table_client.create_entity(new_entity)

def get_content_analysis_csv(table_name, partition_key, row_key):
    """
    Retrieve the 'contentAnalysisCsv' field from the specified Azure Storage Table.

    :param table_name: Name of the table.
    :param partition_key: PartitionKey of the entity.
    :param row_key: RowKey of the entity.
    :param connection_string: Connection string for the Azure Storage account.
    :return: The value of the 'contentAnalysisCsv' field or None if not found.
    """
    try:
        # Create a TableServiceClient using the connection string
        service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient for the specified table
        table_client = service_client.get_table_client(table_name=table_name)

        # Retrieve the entity using PartitionKey and RowKey
        entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)

        # Return the value of 'contentAnalysisCsv' field
        encoded_content_csv = entity.get('contentAnalysisCsv')
        retrieved_csv = encoded_content_csv.replace('\\n', '\n') 
        return retrieved_csv
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Update field on specific entity/ row in storage table 
def update_entity_field(table_name, partition_key, row_key, field_name, new_value):
    """
    Updates a specific field of an entity in an Azure Storage Table.

    Parameters:
    - account_name: str, the name of the Azure Storage account
    - account_key: str, the key for the Azure Storage account
    - table_name: str, the name of the table
    - partition_key: str, the PartitionKey of the entity
    - row_key: str, the RowKey of the entity
    - field_name: str, the name of the field to update
    - new_value: the new value to set for the field
    """
    try:
        # Create a TableServiceClient using the connection string
        table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient
        table_client = table_service_client.get_table_client(table_name)

        # Retrieve the entity
        entity = table_client.get_entity(partition_key, row_key)

        # Update the field
        entity[field_name] = new_value

        # Update the entity in the table
        table_client.update_entity(entity, mode=UpdateMode.REPLACE)
        logging.info(f"update_documents_entity_field:Entity updated successfully.")

    except ResourceNotFoundError:
        logging.info(f"The entity with PartitionKey '{partition_key}' and RowKey '{row_key}' was not found.")
    except Exception as e:
        logging.info(f"An error occurred: {e}")

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="clinicareasconsolidation",
                               connection="medicalanalysis_SERVICEBUS") 
def ContentByClinicAreas(azservicebus: func.ServiceBusMessage):
    message_data = azservicebus.get_body().decode('utf-8')
    logging.info(f"Received messageesds: {message_data}")
    message_data_dict = json.loads(message_data)
    caseid = message_data_dict['caseid']
    doc_id = message_data_dict['doc_id']
    storageTable = message_data_dict['storageTable']
    pagenumber = message_data_dict['pagenumber']
    totalpages = message_data_dict['totalpages']
    content_csv = get_content_analysis_csv("documents", caseid, doc_id)
    logging.info(f"csv content: {content_csv}")
    Csv_Consolidation_by_clinicArea(content_csv,caseid,"ContentByClinicAreas",pagenumber)
    #update document status 
    update_entity_field("documents", caseid, doc_id, "status", 6)
    if pagenumber==totalpages: #if this is the last page 
        #update case status 
        update_case_generic(caseid,"status",9) #update case status to 7 "content analysis done"


