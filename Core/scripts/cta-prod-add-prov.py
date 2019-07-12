#!/usr/bin/env python

import json
import os

from DIRAC.Core.Base import Script

Script.parseCommandLine()

# from DIRAC
import DIRAC
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
# from CTADIRAC
from CTADIRAC.DataManagementSystem.Client.ProvClient import ProvClient
from CTADIRAC.DataManagementSystem.Client.ProvBase import DatasetEntity
from CTADIRAC.DataManagementSystem.Client.ProvBase import WasAttributedTo
from CTADIRAC.DataManagementSystem.Client.ProvBase import Used
from CTADIRAC.DataManagementSystem.Client.ProvBase import WasGeneratedBy
from CTADIRAC.DataManagementSystem.Client.ProvBase import ValueEntity
from CTADIRAC.DataManagementSystem.Client.ProvBase import Activity
from CTADIRAC.DataManagementSystem.Client.ProvBase import WasAssociatedWith

provClient = ProvClient()

# read provenance dictionary
f = open("provDict.txt",'r')
for line in f:
  provStr = line.strip()

f.close()
provList = json.loads(provStr.replace("'", "\""))

# get input data of the job 
jobID = os.environ['JOBID']
dirac = Dirac()

res = dirac.getJobInputData(jobID)

if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

inputData = res['Value'][int(jobID)]
outputData = []

# read output lfns from file
f = open("output_lfns.txt",'r')
for line in f:
  outputData.append(line.strip())
f.close()

# Used to get guid and location from DFC
fc = FileCatalogClient()

def add_activity(cta_activity):
    current_activity = Activity(id=cta_activity['activity_uuid'])
    current_activity.name=cta_activity['activity_name']
    current_activity.startTime=cta_activity['start']['time_utc']
    current_activity.endTime=cta_activity['stop']['time_utc']
    current_activity.comment=''
    current_activity.activityDescription_id=cta_activity['activity_name']+'_'+cta_activity['system']['ctapipe_version']

    provClient.addActivity(current_activity)
    if not res['OK']:
      DIRAC.gLogger.error(res['Message'])
      DIRAC.exit(-1)
    # Association with the agent
    wAW = WasAssociatedWith()
    wAW.activity = cta_activity['activity_uuid']
    wAW.agent    = "CTAO"
       #wAW.role = ?
    provClient.addWasAssociatedWith(wAW)
    if not res['OK']:
      DIRAC.gLogger.error(res['Message'])
      DIRAC.exit(-1)

# For each activity
for cta_activity in provList:
    add_activity(cta_activity)
    
    # For each input file
    for cta_input in cta_activity['input']:
      local_file = os.path.basename(cta_input['url'])
      for lfn in inputData:
        if local_file ==  os.path.basename(lfn):
          res = fc.getFileMetadata(lfn)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
          filename_uuid = res['Value']['Successful'][lfn]['GUID']
          res = fc.getReplicas(lfn)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
          location = res['Value']['Successful'][lfn].keys()
          current_input_file = DatasetEntity(id=filename_uuid, classType = 'dataset', \
                                         name = lfn, location = location)

          res = provClient.getDatasetEntity(filename_uuid)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
          else:
            if not res['Value']:
              res = provClient.addDatasetEntity(current_input_file)
              if not res['OK']:
                DIRAC.gLogger.error(res['Message'])
                DIRAC.exit(-1)
              
               # Attribution to the agent - wAT.role = ?
              wAT = WasAttributedTo(entity = filename_uuid, agent = "CTAO")
              res = provClient.addWasAttributedTo(wAT)
              if not res['OK']:
                DIRAC.gLogger.error(res['Message'])
                DIRAC.exit(-1)
            
          # Add the Used relationship
          used1 = Used(role = cta_input['role'], activity_id = cta_activity['activity_uuid'], entity_id = filename_uuid) # incremental id
          res = provClient.addUsed(used1)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
  
    
    # For each output file
    for cta_output in cta_activity['output']:
      local_file = os.path.basename(cta_output['url'])
      for lfn in outputData:
        if local_file == os.path.basename(lfn):
          res = fc.getFileMetadata(lfn)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
          filename_uuid = res['Value']['Successful'][lfn]['GUID']
        
          res = fc.getReplicas(lfn)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
          location = res['Value']['Successful'][lfn].keys()
        # If Entity already exists in the database, raise an Exception or a error message - #current_output_file.entityDescription_id= ???
        #if session.query(exists().where(DatasetEntity.id==filename_uuid)):
        #    print ("ERROR")
        #else:
          current_output_file = DatasetEntity(id=filename_uuid, classType = 'dataset', name = lfn ,\
                                               location = location)
          

          res = provClient.addDatasetEntity(current_output_file)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        
           # Attribution to the agent - wAT.role = ?
          wAT = WasAttributedTo(entity = filename_uuid, agent = "CTAO")

          res = provClient.addWasAttributedTo(wAT)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)

            
          # Add the wasgeneratedBy relationship - incremental id
          wGB1 = WasGeneratedBy(role = cta_output['role'], activity_id = cta_activity['activity_uuid'],\
                             entity_id = filename_uuid) 

          res = provClient.addWasGeneratedBy(wGB1)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)

          # Add the status as an output ValueEntity
          current_output_value = ValueEntity(id=cta_activity['activity_uuid']+'_status')
          current_output_value.name = 'status'
          current_output_value.classType = 'value'
          current_output_value.valueXX = cta_activity['status']
          current_output_value.entityDescription_id = 'status'

          res = provClient.addValueEntity(current_output_value)
          if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)

