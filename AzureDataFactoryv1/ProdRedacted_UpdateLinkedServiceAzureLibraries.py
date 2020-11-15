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
#from azure.mgmt.resource import ResourceManagementClient
#from azure.mgmt.datafactory import DataFactoryManagementClient
#from azure.mgmt.datafactory.models import *
#import time
import requests
import json

### Get Azure AD Authentication Token 
def authToken(cId, cSecret, ten):
    try:
        credentials = ServicePrincipalCredentials(client_id=cId, secret=cSecret, tenant=ten)
#        adf_client = DataFactoryManagementClient(credentials, subID)
    except Exception as e:
        print('Auth Token error: ', e)
    return credentials.token['access_token']

#adf_client.linked_services.get(resourceGroup, dataFactoryName, LinkedServiceName)
#adf_client.linked_services.get('IronedgeData', 'IronEdgeDataFactory', 'LabtechMySql')
#adf_client.linked_services.create_or_update(resourceGroup, dataFactoryName, LinkedServiceName, )
#adf_client.pipelines.get(resourceGroup, dataFactoryName, 'LabtechComputersCopyToAzure')

### REST API Method
### Get Linked Service Information
def getLinkedService(aTok, subID, rg, df, lsn): 
    getHeader = {
            'Authorization': 'Bearer '+aTok, 
            'Content-Type': 'application/json',
            'x-ms-client-request-id': '01'
            }

    getUrl = 'https://management.azure.com/subscriptions/'+subID+'/resourcegroups/'+rg+'/providers/Microsoft.DataFactory/datafactories/'+df+'/linkedservices/'+lsn+'?api-version=2015-10-01'
    getResponse = requests.get(getUrl, headers = getHeader)
    print(lsn, "Get LinkedService Response Code: ", getResponse.status_code)
    print(getResponse.content)
    return getResponse.status_code, getResponse.content, getUrl

### Update Existing LabtechMySql
def updateOnPremMySqlLinkedService(getUrl, aTok, lsn, server, db, user, pw, gate):
    putHeader = {
            'Authorization': 'Bearer '+aTok, 
            'Content-Type': 'application/json',
            'x-ms-client-request-id': '0101'
            }
    
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
def updateOnPremSQLLinkedService(getUrl, aTok, lsn, dSource, cat, user, pw, gate):
    putHeader2 = {
        'Authorization': 'Bearer '+aTok, 
        'Content-Type': 'application/json',
        'x-ms-client-request-id': '0102'
        }
    
    connectString = '"Data Source='+dSource+';Initial Catalog='+cat+';Integrated Security=False;User ID='+user+';Password='+pw+';"'
    
    putPayload2 = {
        "name": '"'+lsn+'"',
        "properties":
        {
            "type": "OnPremisesSqlServer",
            "typeProperties": {
                "connectionString": connectString,
                "gatewayName": '"'+gate+'"'
            }
        }
    }
    putPayload2 = json.dumps(putPayload2)
    putResponse2 = requests.put(getUrl, headers = putHeader2, data = putPayload2)
    return putResponse2

### Create New MySQL Linked Service
def createOnPremMySQLLinkedService(aTok, lsn, server, db, user, pw, gate):
    putHeader2 = {
       'Authorization': 'Bearer '+aTok, 
       'Content-Type': 'application/json',
       'x-ms-client-request-id': '0102'
    }
    
    newUrl = 'https://management.azure.com/subscriptions/'+subID+'/resourcegroups/'+resourceGroup+'/providers/Microsoft.DataFactory/datafactories/'+dataFactoryName+'/linkedservices/'+lsn+'?api-version=2015-10-01'
   
    newPayload = {
      "name": '"'+lsn+'"',
      "properties": {
        "type": "OnPremisesMySql",
        "typeProperties": {
          "server": '"'+server+'"',
          "database": '"'+db+'"',
          "authenticationType": "Basic",
          "userName": '"'+user+'"',
          "password": '"'+pw+'"',
          "gatewayName": '"'+gate+'"'
        }
      }
    }
       
    newPayload = json.dumps(newPayload)
    newResponse = requests.put(newUrl, headers = putHeader2, data = newPayload)
    return newResponse, newUrl
    

if __name__ == "__main__":
    ### Variables
    subID = 'xxxxxxxx-9999-xxxx-9999-xxxxxxxxxxxx'
    tenant = '99999999-xxxx-9999-xxxx-999999999999'
    clientId = 'yyyyyyyy-8888-yyyy-8888-yyyyyyyyyyyy'
    clientSecret = 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz'
    
    resourceGroup = 'aaaaaaaaaaaa'
    dataFactoryName = 'bbbbbbbbbbbbbbbbbbb'
    LinkedServiceNameList = ['cccccccccccc', 'dddddddddddddddddddddd']
    
    accessToken = authToken(clientId, clientSecret, tenant)
    
    if len(accessToken > 0):
        getRcLT, getContentLT, LTURL = getLinkedService(accessToken, subID, resourceGroup, dataFactoryName, LinkedServiceNameList[0])
        if getRcLT == 200:
            ###update password with put
            putRLT = updateOnPremMySqlLinkedService(LTURL, accessToken, LinkedServiceNameList[0], 'ddddddd', 'uuuuuu', 'ppppppppppppppp', 'ggggggggggggggggg')
            print(putRLT.content)
        else:
            print("Linked Service ", LinkedServiceNameList[0], " Not Found")
            print(getContentLT)
        
        getRcCW, getContentCW, CWURL = getLinkedService(accessToken, subID, resourceGroup, dataFactoryName, LinkedServiceNameList[1])
        if getRcCW == 200:
            ###update password with put
            putRCW = updateOnPremSQLLinkedService(CWURL, accessToken, LinkedServiceNameList[1], 'ddddddd', 'uuuuuu', 'ppppppppppppppp', 'ggggggggggggggggg')
            print(putRCW.content)
        else:
            print("Linked Service ", LinkedServiceNameList[1], " Not Found")
            print(getContentCW)
    else:
        print("Access Token not generated")