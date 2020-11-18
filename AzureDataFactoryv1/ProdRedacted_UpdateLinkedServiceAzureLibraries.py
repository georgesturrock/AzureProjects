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

### Get Azure AD Authentication Token 
def authToken(cId, cSecret, ten):
    try:
        credentials = ServicePrincipalCredentials(client_id=cId, secret=cSecret, tenant=ten)
        adf_client = DataFactoryManagementClient(credentials, subID)
    except Exception as e:
        print('Auth Token error: ', e)
    return credentials.token['access_token']

#adf_client.linked_services.get(resourceGroup, dataFactoryName, LinkedServiceName)
#adf_client.linked_services.get('IronedgeData', 'IronEdgeDataFactory', 'LabtechMySql')
#adf_client.linked_services.create_or_update(resourceGroup, dataFactoryName, LinkedServiceName, )
#adf_client.pipelines.get(resourceGroup, dataFactoryName, 'LabtechComputersCopyToAzure')

### REST API Method
def setHeader(aTok, crID):
    getHeader = {
            'Authorization': 'Bearer '+aTok, 
            'Content-Type': 'application/json',
            'x-ms-client-request-id': crID
            }
    return getHeader

### Get Linked Service Information
def getLinkedService(getHeader, aTok, subID, rg, df, lsn): 
    getUrl = 'https://management.azure.com/subscriptions/'+subID+'/resourcegroups/'+rg+'/providers/Microsoft.DataFactory/datafactories/'+df+'/linkedservices/'+lsn+'?api-version=2015-10-01'
    getResponse = requests.get(getUrl, headers = getHeader)
    print(lsn, "Get LinkedService Response Code: ", getResponse.status_code)
    print(getResponse.content)
    return getResponse.status_code, getResponse.content, getUrl

### Update Existing LabtechMySql
def updateOnPremMySqlLinkedService(putHeader, aTok, lsn, server, db, user, pw, gate):
    putPayload = {
      "name": lsn,
      "properties": {
        "type": "OnPremisesMySql",
        "typeProperties": {
          "server": server,
          "database": db,
          "authenticationType": "Basic",
          "userName": user,
          "password": pw,
          "gatewayName": gate
        }
      }
    }
    putPayload = json.dumps(putPayload)
    putResponse = requests.put(getUrl, headers = putHeader, data = putPayload)
    return putResponse

### Update Existing ConnectwiseSqlServerDB
def updateOnPremSQLLinkedService(putHeader2, aTok, lsn, dSource, cat, user, pw, gate):
    putPayload2 = {
        "name": "ConnectwiesSqlServerDB",
        "properties":
        {
            "type": "OnPremisesSqlServer",
            "typeProperties": {
                "connectionString": "Data Source=10.254.0.25;Initial Catalog=cwwebapp_ieg;Integrated Security=False;User ID=bguser;Password=kVRNj2%4#0F@if8;",
                "gatewayName": "SeventyTwoGateway"
            }
        }
    }
    putPayload2 = json.dumps(putPayload2)
    putResponse2 = requests.put(getUrl, headers = putHeader2, data = putPayload2)
    return putResponse2

if __name__ == '__main__':
    ### Variables
    subID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
    tenant = 'ffffffff-gggg-hhhh-iiii-jjjjjjjjjjjj'
    clientId = 'kkkkkkkk-llll-mmmm-nnnn-oooooooooooo'
    clientSecret = 'pppppppppppppppppppppppppppppppppp'
    
    resourceGroup = 'qqqqqqqqqqqq'
    dataFactoryName = 'rrrrrrrrrrrrrrrrrrr'
    LinkedServiceNameList = ['ssssssssssss', 'tttttttttttttttttttttt']
    
    accessToken = authToken(clientId, clientSecret, tenant)
    
    if len(accessToken > 0):
        header = setHeader(accessToken, round(time.time()))
        getRcLT, getContentLT, LTURL = getLinkedService(header, accessToken, subID, resourceGroup, dataFactoryName, LinkedServiceNameList[0])
        if getRcLT == 200:
            ###update password with put
            putRLT = updateOnPremMySqlLinkedService(header, accessToken, LinkedServiceNameList[0], 'db', 'user', 'pw', 'Gateway')
            print(putRLT.content)
        else:
            print("Linked Service ", LinkedServiceNameList[0], " Not Found")
            print(getContentLT)
        
        getRcCW, getContentCW, CWURL = getLinkedService(header, accessToken, subID, resourceGroup, dataFactoryName, LinkedServiceNameList[1])
        if getRcCW == 200:
            ###update password with put
            putRCW = updateOnPremSQLLinkedService(header, accessToken, LinkedServiceNameList[1], 'db', 'user', 'pw', 'Gateway')
            print(putRCW.content)
        else:
            print("Linked Service ", LinkedServiceNameList[1], " Not Found")
            print(getContentCW)
    else:
        print("Access Token not generated")