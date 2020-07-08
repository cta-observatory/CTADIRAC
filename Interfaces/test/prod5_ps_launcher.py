""" Launcher script to launch a production with 2 steps
    simulation Step: Prod5MCPipeNSBJob
    calibimgreco Step: EvnDisp3RefJobC7
"""

__RCSID__ = "$Id$"

import json
from copy import copy

from DIRAC.Core.Base import Script
Script.parseCommandLine()
import DIRAC

from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient
from DIRAC.ProductionSystem.Client.ProductionStep import ProductionStep
from CTADIRAC.Interfaces.API.Prod5MCPipeNSBJob import Prod5MCPipeNSBJob
from CTADIRAC.Interfaces.API.EvnDispProd5Job import EvnDispProd5Job
from CTADIRAC.Core.Utilities.tool_box import get_dataset_MQ
from DIRAC.Core.Workflow.Parameter import Parameter

# get arguments
args = Script.getPositionalArgs()
if len(args) != 2:
  DIRAC.gLogger.error('At least 2 arguments required: prod_name DL0_data_set')
  DIRAC.exit(-1)
prod_name = args[0]
DL0_data_set = args[1]

##################################
# Create the production
prod_sys_client = ProductionClient()

##################################
# Define the first ProductionStep
##################################
# Note that there is no InputQuery,
# since jobs created by this steps don't require any InputData
prod_step_1 = ProductionStep()
prod_step_1.Name = 'Simulation'
prod_step_1.Type = 'MCSimulation' # This corresponds to the Transformation Type
prod_step_1.Outputquery = get_dataset_MQ(DL0_data_set)
prod_step_1.Outputquery['nsb'] = {'in': [1, 5]}

# Here define the job description (i.e. Name, Executable, etc.) to be associated to the first ProductionStep, as done when using the TS
job1 =  Prod5MCPipeNSBJob()
# Initialize JOB_ID
job1.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))
# configuration
job1.version ='2020-06-29'
job1.compiler='gcc83_matchcpu'
job1.setName('Prod5_MC_Pipeline_NSB')
job1.set_site('Paranal')
job1.set_particle('gamma')
job1.set_pointing_dir('North')
job1.zenith_angle = 20
job1.n_shower = 25000

job1.setOutputSandbox(['*Log.txt'])
job1.start_run_number = '0'
job1.run_number = '@{JOB_ID}'  # dynamic
job1.setupWorkflow(debug=False)
# Add the job description to the first ProductionStep
prod_step_1.Body = job1.workflow.toXML()
# Add the step to the production
prod_sys_client.addProductionStep(prod_step_1)

##################################
# Define the second ProductionStep
##################################
prod_step_2 = ProductionStep()
prod_step_2.Name = 'DL1_image_analysis'
prod_step_2.Type = 'DataReprocessing' # This corresponds to the Transformation Type
prod_step_2.ParentStep = prod_step_1
prod_step_2.Inputquery = get_dataset_MQ(DL0_data_set)
prod_step_2.Outputquery = get_dataset_MQ(DL0_data_set.replace('DL0', 'DL1'))

# Here define the job description to be associated to the second ProductionStep
job2 = EvnDispProd5Job()
job2.setName('Prod5_EvnDisp')
# output
job2.setOutputSandbox(['*Log.txt'])
# refine output meta data if needed
output_meta_data = copy(prod_step_2.Outputquery)
job2.set_meta_data(output_meta_data)
job2.set_file_meta_data(nsb=output_meta_data['nsb']['='])

# check if La Palma else use default
if output_meta_data['site'] == 'LaPalma':
    job2.prefix = "CTA.prod5N"
    job2.layout_list = 'BL-0LSTs05MSTs-MSTF BL-0LSTs05MSTs-MSTN \
                        BL-4LSTs00MSTs-MSTN BL-4LSTs05MSTs-MSTF \
                        BL-4LSTs05MSTs-MSTN BL-4LSTs09MSTs-MSTF \
                        BL-4LSTs09MSTs-MSTN BL-4LSTs15MSTs-MSTF \
                        BL-4LSTs15MSTs-MSTN'
    DIRAC.gLogger.notice('LaPalma layouts:\n',job2.layout_list.split())
elif output_meta_data['site'] == 'Paranal':
    DIRAC.gLogger.notice('Paranal layouts:\n',job2.layout_list.split())

job2.ts_task_id = '@{JOB_ID}'  # dynamic
job2.setupWorkflow(debug=False)
job.setType('EvnDisp3')  # mandatory *here*
prod_step_2.Body = job2.workflow.toXML()
prod_step_2.GroupSize = 5

# Add the step to the production
prod_sys_client.addProductionStep(prod_step_2)

##################################
# Get the production description
##################################
prod_description = prod_sys_client.prodDescription

# Create the production
res = prod_sys_client.addProduction(prod_name, json.dumps(prod_description))
if not res['OK']:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)

# Start the production, i.e. instatiate the transformation steps
res = prod_sys_client.startProduction(prod_name)

if not res['OK']:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)

DIRAC.gLogger.notice('Production %s successfully created' % prod_name)
