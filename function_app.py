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

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="clinicareasconsolidation",
                               connection="medicalanalysis_SERVICEBUS") 
def ContentByClinicAreas(azservicebus: func.ServiceBusMessage):
    logging.info('Python ServiceBuss Queue triggerr processed a message: %s',
                azservicebus.get_body().decode('utf-8'))
