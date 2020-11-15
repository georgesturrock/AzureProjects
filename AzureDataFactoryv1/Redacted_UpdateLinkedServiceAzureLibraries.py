# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 16:41:20 2020

@author: GSturrock

Key Resources:
    https://lucavallarelli.altervista.org/blog/service-principal-authentication-data-factory/
    https://docs.microsoft.com/en-us/rest/api/datafactory/v1/data-factory-linked-service
    https://docs.microsoft.com/en-us/powershell/module/az.datafactory/?view=azps-5.0.0&viewFallbackFrom=azps-2.5.0
    https://docs.microsoft.com/en-us/rest/api/datafactory/v1
    https://github.com/MicrosoftDocs/azure-docs/blob/master/articles/data-factory/v1/data-factory-copy-activity-tutorial-using-rest-api.md
    https://docs.microsoft.com/en-us/azure/storage/common/storage-auth-aad-app?tabs=dotnet
    https://docs.microsoft.com/en-us/python/api/overview/azure/datafactory?view=azure-python
    https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-python
    https://docs.microsoft.com/en-us/azure/role-based-access-control/built-in-roles
    https://docs.microsoft.com/en-us/python/api/azure-mgmt-datafactory/azure.mgmt.datafactory.operations.linkedservicesoperations?view=azure-python
    https://azure.github.io/azure-sdk-for-python/datafactory.html
    https://azuresdkdocs.blob.core.windows.net/$web/python/azure-mgmt-datafactory/0.14.0/azure.mgmt.datafactory.html
    https://docs.microsoft.com/en-us/rest/api/datafactory/linkedservices/createorupdate
    https://docs.microsoft.com/en-us/rest/api/datafactory/linkedservices/createorupdate#mysqllinkedservice
     
"""

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
import time
import requests
import json

### Variables
subID = 'xxxxxxxx-9999-xxxx-9999-xxxxxxxxxxxx'
tenant = 'xxxxxxxx-9999-xxxx-9999-xxxxxxxxxxxx'
clientId = '99999999-xxxx-9999-xxxx-999999999999'
clientSecret = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

resourceGroup = 'xxxxxxxxxxxx'
dataFactoryName = 'xxxxxxxxxxxxxxxxxxx'
LinkedServiceName = 'xxxxxxxxxxxx'

### Linked Service
credentials = ServicePrincipalCredentials(client_id=clientId, secret=clientSecret, tenant=tenant)
adf_client = DataFactoryManagementClient(credentials, subID)

#adf_client.linked_services.get(resourceGroup, dataFactoryName, LinkedServiceName)
#adf_client.linked_services.get('IronedgeData', 'IronEdgeDataFactory', 'LabtechMySql')
#adf_client.linked_services.create_or_update(resourceGroup, dataFactoryName, LinkedServiceName, )
#adf_client.pipelines.get(resourceGroup, dataFactoryName, 'LabtechComputersCopyToAzure')

### REST API Method
### Get Linked Service Information
getHeader = {
        'Authorization': 'Bearer '+credentials.token['access_token'], 
        'Content-Type': 'application/json',
        'x-ms-client-request-id': '01'
        }

getUrl = 'https://management.azure.com/subscriptions/'+subID+'/resourcegroups/'+resourceGroup+'/providers/Microsoft.DataFactory/datafactories/'+dataFactoryName+'/linkedservices/'+LinkedServiceName+'?api-version=2015-10-01'
#getUrl = 'https://management.azure.com/subscriptions/b259c7eb-d76c-4fc5-ad2a-b566fb7df04f/resourcegroups/IronedgeData/providers/Microsoft.DataFactory/datafactories/IronEdgeDataFactory/linkedservices/LabtechMySql?api-version=2015-10-01'

getResponse = requests.get(getUrl, headers = getHeader)
print(getResponse.status_code)
print(getResponse.content)

### Update Existing LabtechMySql
putHeader = {
        'Authorization': 'Bearer '+credentials.token['access_token'], 
        'Content-Type': 'application/json',
        'x-ms-client-request-id': '0101'
        }

putPayload = {
  "name": "xxxxxxxxxxxx",
  "properties": {
    "type": "OnPremisesMySql",
    "typeProperties": {
      "server": "99.999.9.999",
      "database": "xxxxxxx",
      "authenticationType": "Basic",
      "userName": "xxxxxx",
      "password": "xxxxxxxxxxxxxxx",
      "gatewayName": "xxxxxxxxxxxxxxxxx"
    }
  }
}
    
putPayload = json.dumps(putPayload)

putResponse = requests.put(getUrl, headers = putHeader, data = putPayload)

### Update Existing ConnectwiseSqlServerDB
LinkedServiceName = 'xxxxxxxxxxxxxxxxxxxxxx'

getUrl = 'https://management.azure.com/subscriptions/'+subID+'/resourcegroups/'+resourceGroup+'/providers/Microsoft.DataFactory/datafactories/'+dataFactoryName+'/linkedservices/'+LinkedServiceName+'?api-version=2015-10-01'
getResponse = requests.get(getUrl, headers = getHeader)
print(getResponse.status_code)
print(getResponse.content)

putHeader2 = {
        'Authorization': 'Bearer '+credentials.token['access_token'], 
        'Content-Type': 'application/json',
        'x-ms-client-request-id': '0102'
        }

putPayload2 = {
    "name": "xxxxxxxxxxxxxxxxxxxxxx",
    "properties":
    {
        "type": "OnPremisesSqlServer",
        "typeProperties": {
            "connectionString": "Data Source=99.999.9.99;Initial Catalog=xxxxxxxxxxxx;Integrated Security=False;User ID=xxxxxx;Password=xxxxxxxxxxxxxxx;",
            "gatewayName": "SeventyTwoGateway"
        }
    }
}
    
putPayload2 = json.dumps(putPayload2)

putResponse2 = requests.put(getUrl, headers = putHeader2, data = putPayload2)

### Create New Linked Service
newUrl = 'https://management.azure.com/subscriptions/'+subID+'/resourcegroups/'+resourceGroup+'/providers/Microsoft.DataFactory/datafactories/'+dataFactoryName+'/linkedservices/LabtechMySql33?api-version=2015-10-01'

newPayload = {
  "name": "xxxxxxxxxxxx33",
  "properties": {
    "type": "OnPremisesMySql",
    "typeProperties": {
      "server": "99.999.9.999",
      "database": "xxxxxxx",
      "authenticationType": "Basic",
      "userName": "xxxxxx",
      "password": "xxxxxxxxxxxxxxx",
      "gatewayName": "xxxxxxxxxxxxxxxxx"
    }
  }
}
    
newPayload = json.dumps(newPayload)

newResponse = requests.put(newUrl, headers = putHeader, data = newPayload)