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

def get_agent_key(cta_activity):
    res = provClient.getAgentKey("CTAO")
    if not res['OK']:
        DIRAC.gLogger.error('Agent CTAO not found')
        DIRAC.exit(-1)
    return res["Value"]["internal_key"]

def get_activityDescription_key(cta_activity):
    activityDescription_key = None
    res = provClient.getActvityDescriptionKey(cta_activity['activity_name'],
                                              cta_activity['system']['ctapipe_version'])
    if not res['OK']:
        DIRAC.gLogger.error(res['Message'])
        DIRAC.exit(-1)
    if not res['Value']['internal_key']:
        DIRAC.gLogger.error('WARNING: No activity description found')
    else:
        activityDescription_key = res['Value']['internal_key']
    return activityDescription_key

def add_activity(cta_activity, activityDescription_key, agent_key=None):
    current_activity = Activity()
    current_activity.id = cta_activity['activity_uuid']
    current_activity.name = cta_activity['activity_name']
    current_activity.startTime = cta_activity['start']['time_utc']
    current_activity.endTime = cta_activity['stop']['time_utc']
    current_activity.comment = ''
    current_activity.activityDescription_key = activityDescription_key

    # Add the activity in yhe database
    res = provClient.addActivity(current_activity)
    if not res['OK']:
        DIRAC.gLogger.error(res['Message'])
        DIRAC.exit(-1)

    # get the activity internal key
    activity_key = res["Value"]["internal_key"]

    # Association with the agent if specified
    if agent_key:
        wAW = WasAssociatedWith()
        wAW.agent_key = agent_key
        wAW.activity_key = activity_key
        # wAW.role = ?
        res = provClient.addWasAssociatedWith(wAW)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)

    return activity_key

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

def add_dataset(cta_data, dirac_data, fc, entityDescription_key, agent_key):

    local_file = os.path.basename(cta_data['url'])
    for lfn in dirac_data:
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

            # Define the DatasetEntity
            current_file = DatasetEntity(id=filename_uuid, classType='dataset', \
                                         name=lfn, location=location, generatedAtTime=creation_date, \
                                         entityDescription_key=entityDescription_key)

            # Check if the file already exists
            res = provClient.getDatasetEntity(filename_uuid)
            if not res['OK']:
                DIRAC.gLogger.error(res['Message'])
                DIRAC.exit(-1)

            if res['Value']:
                entity_key = res['Value']
            else:
                res = provClient.addDatasetEntity(current_file)
                if not res['OK']:
                    DIRAC.gLogger.error(res['Message'])
                    DIRAC.exit(-1)
                entity_key = res['Value']['internal_key']

            # Association with the agent if specified
            if agent_key:
                # Attribution to the agent - wAT.role =
                wAT = WasAttributedTo()
                wAT.agent_key = agent_key
                wAT.entity_key = entity_key
                res = provClient.addWasAttributedTo(wAT)
                if not res['OK']:
                    DIRAC.gLogger.error(res['Message'])
                    DIRAC.exit(-1)

            return entity_key

###############################################################################
def addProvenance(test_VM=None):

    # read provenance dictionary
    f = open("provDict.txt", 'r')
    for line in f:
        provStr = line.strip()
    f.close()
    provList = json.loads(provStr.replace("'", "\""))

    if not test_VM:

        # instance of DIRAC
        dirac = Dirac()

        # get the jobID of the DIRAC job
        jobID = os.environ['JOBID']

        # get inputData of the job
        res = dirac.getJobInputData(jobID)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        inputData = res['Value'][int(jobID)]

        # get outputData of the job
        # res = dirac.getJobOutputData(jobID)
        # if not res['OK']:
        #  DIRAC.gLogger.error(res['Message'])
        #  DIRAC.exit(-1)
        # outputData = res['Value'][int(jobID)]

    else:
        inputData = ['/bidon/proton_20deg_180deg_run22___cta-prod3-demo-2147m-LaPalma-baseline.simtel.gz']

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

        # get Agent key
        agent_key = get_agent_key(cta_activity)

        # get activity description key
        activityDescription_key = get_activityDescription_key(cta_activity)

        # Add the activity in the database and the wasAssociatedWith
        activity_key = add_activity(cta_activity, activityDescription_key, agent_key)

        # For each input file
        for cta_input in cta_activity['input']:

            # Get entityDescription_key from the UsageDescription
            dict_usageDescription = get_usageDescription(activityDescription_key, cta_input['role'])['Value']
            usageDescription_key = dict_usageDescription['internal_key']
            entityDescription_key = dict_usageDescription['entityDescription_key']

            # Add the dataset
            entity_key = add_dataset(cta_input, inputData, fc, entityDescription_key, agent_key)

            # Add the Used relationship
            #  time = ?
            used = Used(role=cta_input['role'], activity_key=activity_key, \
                         entity_key=entity_key, usageDescription_key=usageDescription_key)
            res = provClient.addUsed(used)
            if not res['OK']:
                DIRAC.gLogger.error(res['Message'])
                DIRAC.exit(-1)

        # For each output file
        for cta_output in cta_activity['output']:

            # Get entityDescription_key from the GenerationDescription
            dict_generationDescription = get_generationDescription(activityDescription_key, cta_output['role'])['Value']
            generationDescription_key = dict_generationDescription['internal_key']
            entityDescription_key = dict_generationDescription['entityDescription_key']

            # Add the dataset
            entity_key = add_dataset(cta_output, outputData, fc, entityDescription_key, agent_key)

            # Add the wasGeneratedBy relationship
            wGB1 = WasGeneratedBy(role=cta_output['role'], activity_key=activity_key, \
                                          entity_key=entity_key, generationDescription_key=generationDescription_key)

            res = provClient.addWasGeneratedBy(wGB1)
            if not res['OK']:
                DIRAC.gLogger.error(res['Message'])
                DIRAC.exit(-1)

        # Set the status as an output ValueEntity
        if cta_activity['status']:

                # Get the GenerationDescription
                dict_generationDescription = get_generationDescription(activityDescription_key, 'status')['Value']
                generationDescription_key = dict_generationDescription['internal_key']
                entityDescription_key = dict_generationDescription['entityDescription_key']

                # Add the ValueEntity
                current_output_value = ValueEntity(id=cta_activity['activity_uuid'] + '_status')
                current_output_value.name = 'status'
                current_output_value.classType = 'value'
                current_output_value.value = cta_activity['status']
                # current_output_value.generatedAtTime =
                current_output_value.entityDescription_key = entityDescription_key

                res = provClient.addValueEntity(current_output_value)
                if not res['OK']:
                    DIRAC.gLogger.error(res['Message'])
                    DIRAC.exit(-1)
                entity_key = res['Value']['internal_key']

                # Add the wasGeneratedBy relationship
                wGB1 = WasGeneratedBy(role='status', activity_key=activity_key, \
                              entity_key=entity_key, generationDescription_key=generationDescription_key)

        res = provClient.addWasGeneratedBy(wGB1)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        '''
        # For each config parameter
        for cta_config_key, cta_config_value in cta_activity['config']['MuonDisplayerTool'].iteritems():
            parameter_id = cta_activity['activity_uuid'] + '_' + cta_config_key
            parameterDescription = provClient.getParameterDescription(activityDescription_key=activityDescription_key, \
                                                                      parameter_name=cta_config_key)
            parameterDescription_key = parameterDescription['Value']['id']
            parameter = Parameter(id=parameter_id, name=cta_config_key, value=cta_config_value, \
                                  parameterDescription_key=parameterDescription_id)
            res = provClient.addParameter(parameter)
            if not res['OK']:
                DIRAC.gLogger.error(res['Message'])
                DIRAC.exit(-1)
            wCB = WasConfiguredBy(artefactType='Parameter', \
                                  activity_key=activity_key, parameter_key=parameter_id)
            res = provClient.addWasConfiguredBy(wCB)
            if not res['OK']:
                DIRAC.gLogger.error(res['Message'])
                DIRAC.exit(-1)
    '''
    return DIRAC.S_OK()

###############################################################################
if __name__ == '__main__':
    args = Script.getPositionalArgs()
exit()
    try:
        provClient = ProvClient()
        res = addProvenance( args )
        #res = addProvenance()
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
