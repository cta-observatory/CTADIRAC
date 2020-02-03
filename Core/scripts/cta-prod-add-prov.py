#!/usr/bin/env python

import json
import os
import datetime

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
from CTADIRAC.DataManagementSystem.Client.ProvBase import GenerationDescription
from CTADIRAC.DataManagementSystem.Client.ProvBase import UsageDescription
from CTADIRAC.DataManagementSystem.Client.ProvBase import WasConfiguredBy
from CTADIRAC.DataManagementSystem.Client.ProvBase import Parameter
from CTADIRAC.DataManagementSystem.Client.ProvBase import ConfigFile
from CTADIRAC.DataManagementSystem.Client.ProvBase import ParameterDescription
from CTADIRAC.DataManagementSystem.Client.ProvBase import ConfigFileDescription


def get_activityDescription(cta_activity):
    return cta_activity['activity_name'] + '_' + cta_activity['system']['ctapipe_version']


def add_activity(cta_activity, activityDescription_id=None):
    current_activity = Activity(id=cta_activity['activity_uuid'])
    current_activity.name = cta_activity['activity_name']
    current_activity.startTime = cta_activity['start']['time_utc']
    current_activity.endTime = cta_activity['stop']['time_utc']
    current_activity.comment = ''
    current_activity.activityDescription_id = activityDescription_id

    res = provClient.addActivity(current_activity)
    if not res['OK']:
        DIRAC.gLogger.error(res['Message'])
        DIRAC.exit(-1)
    # Association with the agent
    wAW = WasAssociatedWith()
    wAW.activity = cta_activity['activity_uuid']
    wAW.agent = "CTAO"
    # wAW.role = ?
    res = provClient.addWasAssociatedWith(wAW)
    if not res['OK']:
        DIRAC.gLogger.error(res['Message'])
        DIRAC.exit(-1)

def get_usageDescription(cta_activity, role):
    res = provClient.getUsageDescription(cta_activity, role)
    if not res['OK']:
        DIRAC.gLogger.error(res['Message'])
        DIRAC.exit(-1)
    else:
        return res

def get_generationDescription(cta_activity, role):
    res = provClient.getGenerationDescription(cta_activity, role)
    if not res['OK']:
        DIRAC.gLogger.error(res['Message'])
        DIRAC.exit(-1)
    else:
        return res


###############################################################################
def addProvenance():

    # read provenance dictionary
    f = open("provDict.txt", 'r')
    for line in f:
        provStr = line.strip()
    f.close()
    provList = json.loads(provStr.replace("'", "\""))

    # instance of DIRAC
    #dirac = Dirac()

    # get the jobID of the DIRAC job
    # jobID = os.environ['JOBID']

    # get inputData of the job
    # res = dirac.getJobInputData(jobID)
    # if not res['OK']:
    #  DIRAC.gLogger.error(res['Message'])
    #  DIRAC.exit(-1)
    # inputData = res['Value'][int(jobID)]
    inputData = ['/bidon/proton_20deg_180deg_run22___cta-prod3-demo-2147m-LaPalma-baseline.simtel.gz']

    # get outputData of the job
    # res = dirac.getJobOutputData(jobID)
    # if not res['OK']:
    #  DIRAC.gLogger.error(res['Message'])
    #  DIRAC.exit(-1)
    # outputData = res['Value'][int(jobID)]
    outputData = []
    # read output lfns from file
    f = open("output_lfns.txt", 'r')
    for line in f:
        outputData.append(line.strip())
    f.close()

    # FileCatalogClient instance used to get guid and location from DFC
    fc = FileCatalogClient()

    # For each activity
    for cta_activity in provList:
        activityDescription_id = get_activityDescription(cta_activity)
        add_activity(cta_activity, activityDescription_id)

        # For each input file
        for cta_input in cta_activity['input']:
            local_file = os.path.basename(cta_input['url'])
            for lfn in inputData:
                if local_file == os.path.basename(lfn):

                    # Read the DIRAC FileMetadata and Replicas
                    res = fc.getFileMetadata(lfn)
                    if not res['OK']:
                        DIRAC.gLogger.error(res['Message'])
                        DIRAC.exit(-1)
                    filename_uuid = res['Value']['Successful'][lfn]['GUID']
                    creation_date = res['Value']['Successful'][lfn]['CreationDate'].isoformat()
                    res = fc.getReplicas(lfn)
                    if not res['OK']:
                        DIRAC.gLogger.error(res['Message'])
                        DIRAC.exit(-1)
                    location = res['Value']['Successful'][lfn].keys()

                    # Get entityDescription_id from the UsageDescription
                    dict_usageDescription = get_usageDescription(activityDescription_id, cta_input['role'])['Value']
                    usageDescription_id = dict_usageDescription['id']
                    entityDescription_id = dict_usageDescription['entityDescription_id']

                    # Define the DatasetEntity
                    current_input_file = DatasetEntity(id=filename_uuid, classType='dataset', \
                                                       name=lfn, location=location, generatedAtTime=creation_date, \
                                                       entityDescription_id=entityDescription_id)

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
                            wAT = WasAttributedTo(entity=filename_uuid, agent="CTAO")
                            res = provClient.addWasAttributedTo(wAT)
                            if not res['OK']:
                                DIRAC.gLogger.error(res['Message'])
                                DIRAC.exit(-1)

                    # Add the Used relationship
                    used1 = Used(role=cta_input['role'], activity_id=cta_activity['activity_uuid'], \
                                 entity_id=filename_uuid, usageDescription_id=usageDescription_id)  # incremental id
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
                    creation_date = res['Value']['Successful'][lfn]['CreationDate'].isoformat()

                    res = fc.getReplicas(lfn)
                    if not res['OK']:
                        DIRAC.gLogger.error(res['Message'])
                        DIRAC.exit(-1)
                    location = res['Value']['Successful'][lfn].keys()

                    # Get the GenerationDescription
                    dict_generationDescription = get_generationDescription(activityDescription_id, \
                                                                           cta_input['role'])[ 'Value']
                    generationDescription_id = dict_generationDescription['id']
                    entityDescription_id = dict_generationDescription['entityDescription_id']

                    # If Entity already exists in the database, raise an Exception or a error message - #current_output_file.entityDescription_id= ???
                    # if session.query(exists().where(DatasetEntity.id==filename_uuid)):
                    #    print ("ERROR")
                    # else:
                    current_output_file = DatasetEntity(id=filename_uuid, classType='dataset', \
                                                        name=lfn, location=location, generatedAtTime=creation_date, \
                                                        entityDescription_id=entityDescription_id)

                    res = provClient.addDatasetEntity(current_output_file)
                    if not res['OK']:
                        DIRAC.gLogger.error(res['Message'])
                        DIRAC.exit(-1)

                    # Attribution to the agent - wAT.role = ?
                    wAT = WasAttributedTo(entity=filename_uuid, agent="CTAO")

                    res = provClient.addWasAttributedTo(wAT)
                    if not res['OK']:
                        DIRAC.gLogger.error(res['Message'])
                        DIRAC.exit(-1)

                    # Add the wasGeneratedBy relationship - incremental id
                    wGB1 = WasGeneratedBy(role=cta_output['role'], activity_id=cta_activity['activity_uuid'], \
                                          entity_id=filename_uuid, generationDescription_id=generationDescription_id)

                    res = provClient.addWasGeneratedBy(wGB1)
                    if not res['OK']:
                        DIRAC.gLogger.error(res['Message'])
                        DIRAC.exit(-1)

        # Add the status as an output ValueEntity
        current_output_value = ValueEntity(id=cta_activity['activity_uuid'] + '_status')
        current_output_value.name = 'status'
        current_output_value.classType = 'value'
        current_output_value.value = cta_activity['status']
        # current_output_value.generatedAtTime =
        # Get the GenerationDescription
        dict_generationDescription = get_generationDescription(activityDescription_id, 'status')['Value']
        generationDescription_id = dict_generationDescription['id']
        entityDescription_id = dict_generationDescription['entityDescription_id']
        current_output_value.entityDescription_id = entityDescription_id

        res = provClient.addValueEntity(current_output_value)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)

        # Add the wasGeneratedBy relationship - incremental id
        wGB1 = WasGeneratedBy(role='status', activity_id=cta_activity['activity_uuid'], \
                              entity_id=cta_activity['activity_uuid'] + '_status', \
                              generationDescription_id=generationDescription_id)

        res = provClient.addWasGeneratedBy(wGB1)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)

        # For each config parameter
        for cta_config_key, cta_config_value in cta_activity['config']['MuonDisplayerTool'].iteritems():
            parameter_id = cta_activity['activity_uuid'] + '_' + cta_config_key
            parameterDescription = provClient.getParameterDescription(activityDescription_id=activityDescription_id, \
                                                                      parameter_name=cta_config_key)
            parameterDescription_id = parameterDescription['Value']['id']
            parameter = Parameter(id=parameter_id, name=cta_config_key, value=cta_config_value, \
                                  parameterDescription_id=parameterDescription_id)
            res = provClient.addParameter(parameter)
            if not res['OK']:
                DIRAC.gLogger.error(res['Message'])
                DIRAC.exit(-1)
            wCB = WasConfiguredBy(artefactType='Parameter', \
                                  activity_id=cta_activity['activity_uuid'], parameter_id=parameter_id)
            res = provClient.addWasConfiguredBy(wCB)
            if not res['OK']:
                DIRAC.gLogger.error(res['Message'])
                DIRAC.exit(-1)

    return DIRAC.S_OK()

###############################################################################
if __name__ == '__main__':
    # args = Script.getPositionalArgs()
    try:
        provClient = ProvClient()
        # res = addProvenance( args )
        res = addProvenance()
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
