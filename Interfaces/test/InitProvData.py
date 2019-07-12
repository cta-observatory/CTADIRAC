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

provClient = ProvClient()

# Create an instance of ActivityDescription
actDesc1 = ActivityDescription(id='ctapipe-display-muons_0.6.2',name='ctapipe_display_muons',\
                               activity_type='',activity_subtype='',version='0.6.2', doculink='')
# Create the description of input entities
dataDesc1 = DatasetDescription(id='proton_events', name='protons', description='proton file', classType='datasetDescription')
usedDesc1 = UsageDescription(id='ctapipe-display-muons_0.6.2_proton_events',activityDescription_id=actDesc1.id, entityDescription_id=dataDesc1.id, role="dl0.sub.evt")
# Create the description of output entities
dataDesc2  = DatasetDescription(id='muons_hdf5', name='muons', description='muon file', classType='datasetDescription')
wGBDesc1   = GenerationDescription(id='ctapipe-display-muons_0.6.2_muons_hdf5',activityDescription_id=actDesc1.id, entityDescription_id=dataDesc2.id, role="dl0.sub.evt")
valueDesc1 = ValueDescription(id='status', classType='valueDescription')
wGBDesc2   = GenerationDescription(id='ctapipe-display-muons_0.6.2_status', activityDescription_id=actDesc1.id, entityDescription_id=valueDesc1.id, role="quality")

#
res = provClient.addActivityDescription(actDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
res = provClient.addDatasetDescription(dataDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
res = provClient.addUsageDescription(usedDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
res = provClient.addDatasetDescription(dataDesc2)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
res = provClient.addGenerationDescription(wGBDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
res = provClient.addValueDescription(valueDesc1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
res = provClient.addGenerationDescription(wGBDesc2)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

# CTAO Agent
agent = Agent(id="CTAO")
agent.name ="CTA Observatory"
agent.type = "Organization"

res = provClient.addAgent(agent)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)
