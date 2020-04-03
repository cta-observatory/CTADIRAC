"""
    Test for the Client
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script



Script.setUsageMessage('\n'.join([__doc__.split('\n')[1]]))

Script.parseCommandLine()

from ProvClient import ProvClient
from ProvBase import Agent
from ProvBase import ActivityDescription
from ProvBase import DatasetDescription
from ProvBase import UsageDescription
from ProvBase import GenerationDescription
from ProvBase import ValueDescription
from ProvBase import DatasetEntity
from ProvBase import WasAttributedTo
from ProvBase import Used
from ProvBase import WasGeneratedBy
from ProvBase import ValueEntity
from ProvBase import Activity
from ProvBase import WasAssociatedWith

provClient = ProvClient()

# Create an instance of ActivityDescription
actDesc1 = ActivityDescription(id='ctapipe_display_muons_0.6.1',name='ctapipe_display_muons', \
   description = '', type='',subtype='',version='0.6.1', doculink='')
# Create the description of input entities
dataDesc1 = DatasetDescription(id='proton_events', name='protons', description='proton file', classType='datasetDescription')
usedDesc1 = UsageDescription(id='ctapipe_display_muons_0.6.1_proton_events',activityDescription_id=actDesc1.id, entityDescription_id=dataDesc1.id, role="dl0.sub.evt")
# Create the description of output entities
dataDesc2  = DatasetDescription(id='muons_hdf5', name='muons', description='muon file', classType='datasetDescription')
wGBDesc1   = GenerationDescription(id='ctapipe_display_muons_0.6.1_muons_hdf5',activityDescription_id=actDesc1.id, entityDescription_id=dataDesc2.id, role="dl0.sub.evt")
valueDesc1 = ValueDescription(id='status', classType='valueDescription')
wGBDesc2   = GenerationDescription(id='ctapipe_display_muons_0.6.1_status', activityDescription_id=actDesc1.id, entityDescription_id=valueDesc1.id, role="quality")

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


current_activity = Activity(id='ed7e27d4-a0a7-43a2-97a1-511348dd37bd')
current_activity.name = 'ctapipe-display-muons'
current_activity.startTime = 'cta_activity_start_time_utc'
current_activity.endTime= 'cta_activity_stop_time_utc'
current_activity.comment=''
current_activity.activityDescription_id = 'ctapipe_display_muons_0.6.1'

res = provClient.addActivity(current_activity)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

# Association with the agent
wAW = WasAssociatedWith()
wAW.activity = 'ed7e27d4-a0a7-43a2-97a1-511348dd37bd'
wAW.agent    = "CTAO"

res = provClient.addWasAssociatedWith(wAW)

if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

current_input_file = DatasetEntity(id='hfilename_uuid', classType = 'dataset', name = 'logical_name',
                                   location = 'file_url')

res = provClient.addDatasetEntity(current_input_file)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

# Attribution to the agent - wAT.role = ?
wAT = WasAttributedTo(entity = 'hfilename_uuid', agent = "CTAO")

res = provClient.addWasAttributedTo(wAT)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

# Add the Used relationship
used1 = Used(role = 'cta_input_role', activity_id = current_activity.id, entity_id = current_input_file.id)
res = provClient.addUsed(used1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)


# Add the wasgeneratedBy relationship - incremental id
wGB1 = WasGeneratedBy(role = 'cta_output_role', activity_id = current_activity.id, entity_id = current_input_file.id)

res = provClient.addWasGeneratedBy(wGB1)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

# Add the status as an output ValueEntity
current_output_value = ValueEntity(id='cta_activity_uuid_status')
current_output_value.name = 'status'
current_output_value.classType = 'value'
current_output_value.valueXX = 'cta_activity_status'
current_output_value.entityDescription_id = 'status'

res = provClient.addValueEntity(current_output_value)
if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

