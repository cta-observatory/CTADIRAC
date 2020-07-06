""" Launcher script to launch a production with 2 steps
    simulation Step: Prod5MCPipeNSBJob
    calibimgreco Step: EvnDisp3RefJobC7
"""

__RCSID__ = "$Id$"

import json

from DIRAC.Core.Base import Script
Script.parseCommandLine()
import DIRAC

from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient
from DIRAC.ProductionSystem.Client.ProductionStep import ProductionStep
from CTADIRAC.Interfaces.API.Prod5MCPipeNSBJob import Prod5MCPipeNSBJob
from CTADIRAC.Interfaces.API.EvnDisp3RefJobC7 import EvnDisp3RefJobC7
from DIRAC.Core.Workflow.Parameter import Parameter

# get arguments
args = Script.getPositionalArgs()
if len(args) != 1:
  DIRAC.gLogger.error('At least 1 argument required: prodName')
  DIRAC.exit(-1)
prodName = args[0]

# Define the production
prodClient = ProductionClient()

# Define the first ProductionStep
# Note that there is no InputQuery, since jobs created by this steps don't require any InputData
prodStep1 = ProductionStep()
prodStep1.Name = 'Sim_prog'
prodStep1.Type = 'MCSimulation' # This corresponds to the Transformation Type
prodStep1.Outputquery = {'thetaP': 20, 'data_level': 0, 'particle': 'gamma', 'tel_sim_prog': 'sim_telarray', 'array_layout': 'Advanced-Baseline', 'configuration_id': 7, 'site': 'LaPalma', 'MCCampaign': 'PROD5', 'phiP': 0.0, 'tel_sim_prog_version': '2020-06-29', 'outputType': {'in': ['Data', 'Log']}}

# Here define the job description (i.e. Name, Executable, etc.) to be associated to the first ProductionStep, as done when using the TS
job1 =  Prod5MCPipeNSBJob()

# Initialize JOB_ID
job1.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))

job1.version ='2020-06-29'
job1.compiler='gcc83_matchcpu'
job1.setName('Prod5_MC_Pipeline_NSB')
# parameters from command line
job1.set_site('LaPalma')
job1.set_particle('gamma')
job1.set_pointing_dir('South')
job1.zenith_angle = 20
job1.n_shower = 100

job1.setOutputSandbox(['*Log.txt'])
job1.start_run_number = '100000'
job1.run_number = '@{JOB_ID}'  # dynamic
job1.setupWorkflow(debug=False)
# Add the job description to the first ProductionStep
prodStep1.Body = job1.workflow.toXML()


# Add the step to the production
prodClient.addProductionStep(prodStep1)

# Define the second ProductionStep
prodStep2 = ProductionStep()
prodStep2.Name = 'Reco_prog'
prodStep2.Type = 'DataReprocessing' # This corresponds to the Transformation Type
prodStep2.ParentStep = prodStep1
prodStep2.Inputquery = {'thetaP': 20, 'data_level': 0, 'particle': 'gamma', 'tel_sim_prog': 'sim_telarray', 'array_layout': 'Advanced-Baseline', 'configuration_id': 7, 'site': 'LaPalma', 'MCCampaign': 'PROD5', 'phiP': 0.0, 'tel_sim_prog_version': '2020-06-29', 'outputType': 'Data'}
prodStep2.Outputquery = {'thetaP': 20, 'data_level': 1, 'particle': 'gamma', 'calibimgreco_prog': 'evndisp', 'analysis_prog': 'evndisp', 'array_layout': 'Advanced-Baseline', 'configuration_id': 7, 'site': 'LaPalma', 'MCCampaign': 'PROD5', 'phiP': 0.0, 'calibimgreco_prog_version': 'prod3b_d20200521', 'outputType': {'in': ['Data', 'Log']}}

# Here define the job description to be associated to the second ProductionStep
job2 = EvnDisp3RefJobC7()
job2.setName('EvnDisp')
job2.version = "prod3b_d20200521"
# output
job2.setOutputSandbox(['*Log.txt'])
# change here for Paranal or La Palma
job2.prefix = "CTA.prod4N" 
#  set calibration file and parameters file
job2.calibration_file ="prod3b.Paranal-20171214.ped.root"
job2.configuration_id = 7
job2.setupWorkflow(debug=False)
prodStep2.Body = job2.workflow.toXML()


# Add the step to the production
prodClient.addProductionStep(prodStep2)

# Get the production description
prodDesc = prodClient.prodDescription

# Create the production
res = prodClient.addProduction(prodName, json.dumps(prodDesc))

if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

# Start the production, i.e. instatiate the transformation steps
res = prodClient.startProduction(prodName)

if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

DIRAC.gLogger.notice('Production %s successfully created' % prodName)
