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



# get content csv from azure storage 
def get_contentcsv(path):
    try:
        logging.info(f"get_contentcsv function strating, path value: {path}")
        container_name = "medicalanalysis"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(path)
        download_stream = blob_client.download_blob()
        filecontent  = download_stream.read().decode('utf-8')
        logging.info(f"get_contentcsv: data from the txt file is {filecontent}")
        return filecontent
    except Exception as e:
        logging.error(f"get_contentcsv: Error update case: {str(e)}")
        return None    



#save ContentByClinicAreas content 
def save_ContentByClinicAreas(content,caseid,filename):
    try:
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        destinationPath = f"{basicPath}/ContentByClinicAreas/{filename}"
        blob_client = container_client.upload_blob(name=destinationPath, data=content)
        logging.info(f"the ContentByClinicAreas content file url is: {blob_client.url}")
        return destinationPath
    
    except Exception as e:
        print("An error occurred:", str(e))



#this function sending service bus event for each clinic area 
def create_servicebus_event_for_each_RowKey(table_name, caseid):
    # Create a TableServiceClient object using the connection string
    service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)
    
    # Get the table client
    table_client = service_client.get_table_client(table_name=table_name)
    

    # Query the table for entities with the given PartitionKey
    entities = table_client.query_entities(f"PartitionKey eq '{caseid}'")

    # Print the RowKey for each entity
    for entity in entities:
        #preparing data for service bus
        data = { 
                "clinicArea" :entity['RowKey'],
                "sourceTable" :table_name,
                "caseid" :caseid
            } 
        json_data = json.dumps(data)
        create_servicebus_event("handleduplicatediagnosis", json_data) #send event to service bus 
        logging.info(f"create_servicebus_event_for_each_RowKey:event data:{data}")
    logging.info(f"create_servicebus_event_for_each_RowKey: events sending done")

 #Create event on azure service bus 
def create_servicebus_event(queue_name, event_data):
    try:
        # Create a ServiceBusClient using the connection string
        servicebus_client = ServiceBusClient.from_connection_string(connection_string_servicebus)

        # Create a sender for the queue
        sender = servicebus_client.get_queue_sender(queue_name)

        with sender:
            # Create a ServiceBusMessage object with the event data
            message = ServiceBusMessage(event_data)

            # Send the message to the queue
            sender.send_messages(message)

        logging.info("create_servicebus_event:Event created successfully.")
    
    except Exception as e:
        logging.error(f"create_servicebus_event:An error occurred:, {str(e)}")

#  Function check how many rows in partition of azure storage table where status = 6 (ContentByClinicAreas done)
def count_rows_in_partition( table_name,partition_key):
    # Create a TableServiceClient object using the connection string
    service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)
    
    # Get the table client
    table_client = service_client.get_table_client(table_name=table_name)
    
    # Define the filter query to count entities with the specified partition key and where contentAnalysisCsv is not null or empty
    filter_query = f"PartitionKey eq '{partition_key}' and status eq 6"
    
    # Query the entities and count the number of entities
    entities = table_client.query_entities(query_filter=filter_query)
    count = sum(1 for _ in entities)  # Sum up the entities
    
    if count>0:
        return count
    else:
        return 0

# Generic Function to update case  in the 'cases' table
def update_case_generic(caseid,field,value,field2,value2):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # update case
        cursor.execute(f"UPDATE cases SET {field} = ?,{field2} = ? WHERE id = ?", (value,value2, caseid))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info(f"case {caseid} updated field name: {field} , value: {value} and field name: {field2} , value: {value2}")
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
        if clinicalarea!="Not Specified": #if clinicalarea not defined - jump on it 
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
            # Try to get the existing entity!!need to change here 
            entity = table_client.get_entity(partition_key=caseid, row_key=row_key)
            existing_content_csv_path2 = entity.get('contentCsv', None)#new
            logging.info(f"existing_content_csv_path2: {existing_content_csv_path2}")#new
            #existing_content_csv_path = entity['contentCsv']
            logging.info(f"fun:Csv_Consolidation_by_clinicArea:check if entity existing")
            # Append the new records to the existing CSV content
            if existing_content_csv_path2:#existing_content_csv_path.strip():  # Check if existing content path is not empty
                logging.info(f"fun:Csv_Consolidation_by_clinicArea:entity existing")
                #get content csv from txt file from azure storage
                existing_content_csv =  get_contentcsv(existing_content_csv_path2)
                existing_content_io = io.StringIO(existing_content_csv.replace('\\n', '\n'))
                existing_csv_reader = csv.DictReader(existing_content_io)
                combined_output = io.StringIO(newline='')
                combined_csv_writer = csv.DictWriter(combined_output, fieldnames=csv_reader.fieldnames)
                combined_csv_writer.writeheader()
                combined_csv_writer.writerows(existing_csv_reader)
                combined_csv_writer.writerows(records)
                final_content_csv = combined_output.getvalue()
            else:
                logging.info(f"fun:Csv_Consolidation_by_clinicArea:existing content path is empty")
                final_content_csv = new_content_csv
                logging.info(f"fun:Csv_Consolidation_by_clinicArea:final_content_csv: {final_content_csv}")
            # Encode the CSV string to preserve newlines
            encoded_content_csv = final_content_csv.replace('\n', '\\n')
            #save in azure storage blob 
            filename = f"{row_key}.txt"
            logging.info(f"fun:Csv_Consolidation_by_clinicArea step2:filename: {filename}")
            destinationPath = save_ContentByClinicAreas(encoded_content_csv,caseid,filename)
            logging.info(f"fun:Csv_Consolidation_by_clinicArea step2:destinationPath: {destinationPath}")
            entity['contentCsv'] = destinationPath
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
             #save in azure storage blob 
            filename = f"{row_key}.txt"
            destinationPath = save_ContentByClinicAreas(encoded_content_csv,caseid,filename)
            new_entity = {
                "PartitionKey": caseid,
                "RowKey": row_key,
                "contentCsv": destinationPath,
                "pages": str(pagenumber)
            }
            table_client.create_entity(new_entity)

def get_content_analysis_csv(table_name, partition_key, row_key):

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
        logging.info(f"update_entity_field:Entity updated successfully.")

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
    pages_done = count_rows_in_partition( "documents",caseid) # check how many entities finished this process 
    logging.info(f"total pages: {totalpages}, total pages passed {pages_done}")
    if pages_done==totalpages:
        updateCaseResult = update_case_generic(caseid,"status",9,"contentByClinicAreas",1) #update case status to 9 "ContentByClinicAreas done"
        logging.info(f"update case result is: {updateCaseResult}")
        create_servicebus_event_for_each_RowKey("ContentByClinicAreas", caseid)
        logging.info(f"ContentByClinicAreas: Done")

    


