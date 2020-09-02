#!/usr/bin/env python

import json


from DIRAC.Core.Base import Script

#Script.setUsageMessage('\n'.join([__doc__.split('\n')[1]]))

Script.parseCommandLine()

import DIRAC
# from CTADIRAC

from CTADIRAC.DataManagementSystem.Client.ProvClient import ProvClient
from CTADIRAC.DataManagementSystem.Client.ProvBase import Agent
from CTADIRAC.DataManagementSystem.Client.ProvBase import ActivityDescription
from CTADIRAC.DataManagementSystem.Client.ProvBase import DatasetDescription
from CTADIRAC.DataManagementSystem.Client.ProvBase import UsageDescription
from CTADIRAC.DataManagementSystem.Client.ProvBase import GenerationDescription
from CTADIRAC.DataManagementSystem.Client.ProvBase import ValueDescription
from CTADIRAC.DataManagementSystem.Client.ProvBase import DatasetEntity
from CTADIRAC.DataManagementSystem.Client.ProvBase import WasAttributedTo
from CTADIRAC.DataManagementSystem.Client.ProvBase import Used
from CTADIRAC.DataManagementSystem.Client.ProvBase import WasGeneratedBy
from CTADIRAC.DataManagementSystem.Client.ProvBase import ValueEntity
from CTADIRAC.DataManagementSystem.Client.ProvBase import Activity
from CTADIRAC.DataManagementSystem.Client.ProvBase import WasAssociatedWith
from CTADIRAC.DataManagementSystem.Client.ProvBase import WasConfiguredBy
from CTADIRAC.DataManagementSystem.Client.ProvBase import Parameter
from CTADIRAC.DataManagementSystem.Client.ProvBase import ConfigFile
from CTADIRAC.DataManagementSystem.Client.ProvBase import ParameterDescription
from CTADIRAC.DataManagementSystem.Client.ProvBase import ConfigFileDescription

provClient = ProvClient()

# Add an ActivityDescription and get its internal_key

actDesc1 = ActivityDescription(name='ctapipe-display-muons',\
                               type='Reconstruction',subtype='',version='0.6.2', doculink='', \
                               description='Muon reconstruction')
#
res = provClient.addActivityDescription(actDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
else:
  activityDescription_key = res['Value']['internal_key']

# Create the description of input entities
dataDesc1 = DatasetDescription(name='protons_events', description='proton file',\
                               classType='datasetDescription', type='data', contentType='application/octet-stream')
res = provClient.addDatasetDescription(dataDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
else:
  entityDescription_key = res['Value']['internal_key']

usedDesc1 = UsageDescription(activityDescription_key=activityDescription_key, \
                             entityDescription_key=entityDescription_key, role="dl0.sub.evt", type='Main')
res = provClient.addUsageDescription(usedDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

# Create the description of output entities
dataDesc2  = DatasetDescription(name='muons', description='muon file', \
                                classType='datasetDescription', type='data', contentType='application/octet-stream')
res = provClient.addDatasetDescription(dataDesc2)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
else:
  entityDescription_key = res['Value']['internal_key']
  
wGBDesc1   = GenerationDescription(activityDescription_key=activityDescription_key, \
                                   entityDescription_key=entityDescription_key, role="dl1.tel.evt.muon", type='Main')
res = provClient.addGenerationDescription(wGBDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
                                     
valueDesc1 = ValueDescription(name='status', description='activity status', type='Quality', \
                              valueType='string')
res = provClient.addValueDescription(valueDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
else:
  entityDescription_key = res['Value']['internal_key']
  
wGBDesc2   = GenerationDescription(activityDescription_key=activityDescription_key, \
                                   entityDescription_key=entityDescription_key, role="status", type='Quality')
res = provClient.addGenerationDescription(wGBDesc2)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

# Create description of configFiles and Parameters
paramDesc1 = ParameterDescription(name='events', valueType='string', description='name of the event file',\
                                  ucd='meta.id',utype='ProvenanceDM.ParameterDescription', \
                                  activityDescription_key = activityDescription_key)
res = provClient.addParameterDescription(paramDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
  
paramDesc2 = ParameterDescription(name='outfile', valueType='string', description='name of the output file',\
                                  ucd='meta.id',utype='ProvenanceDM.ParameterDescription', \
                                  activityDescription_key = activityDescription_key)
res = provClient.addParameterDescription(paramDesc2)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

# CTAO Agent
agent = Agent(id='CTAO')
agent.name ="CTA Observatory"
agent.type = "Organization"

res = provClient.addAgent(agent)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
