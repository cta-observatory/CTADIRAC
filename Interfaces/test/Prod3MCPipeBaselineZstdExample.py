""" Prod3 MC Pipe Script to create a Transformation
          JB, LA September 2017
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([ __doc__.split('\n')[1],
                                     'Usage:',
                                     '  %s array_layout site particle pointing_dir zenith_angle nShower' % Script.scriptName,
                                     'Arguments:',
                                     '  array_layout: Baseline',
                                     '  site: Paranal or LaPalma',
                                     '  particle: gamma, proton, electron',
                                     '  pointing_dir: North or South',
                                     '  zenith_agle: 20',
                                     '  nShower: from 5 to 25000',
                                     '\ne.g: %s Baseline Paranal gamma South 20 5'% Script.scriptName,
                                     ]))

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.Core.Workflow.Parameter import Parameter
from CTADIRAC.Interfaces.API.Prod3MCPipeBaselineZstdJob import Prod3MCPipeBaselineZstdJob
# from Prod3MCPipeBaselineZstdJob import Prod3MCPipeBaselineZstdJob


def submitTS(job):
  """ Create a transformation executing the job workflow  """

  ### Temporary fix to initialize JOB_ID #######
  job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "", True, False, "Temporary fix"))
  job.workflow.addParameter(Parameter("PRODUCTION_ID", "000000", "string", "", "", True, False, "Temporary fix"))
  job.setType('MCSimulation') ## Used for the JobType plugin

  t = Transformation()
  # t.setTransformationName("Prod3Exemple")  # This must be unique. If not set it's asked in the prompt
  t.setType("MCSimulation")
  t.setDescription("MC Prod3 BaseLine HE test")
  t.setLongDescription("corsika-simtel production")  # mandatory
  t.setBody (job.workflow.toXML())

  res = t.addTransformation()  # Transformation is created here

  if not res['OK']:
    print(res['Message'])
    DIRAC.exit(-1)

  t.setStatus("Active")
  t.setAgentType("Automatic")

  return res

#########################################################

def runProd3(args = None):
  """ Simple wrapper to create a Prod3MCPipeBaselineJob and setup parameters
      from positional arguments given on the command line.

      Parameters:
      args -- a list of 6 strings corresponding to job arguments
              array_layout site particle pointing_dir zenith_angle nShower
              demo LaPalma  gamma South 20 1000
  """
  # get arguments
  layout = args[0]
  site = args[1]
  particle = args[2]
  pointing = args[3]
  zenith = args[4]
  nShower= args[5]

    ### Main Script ###
  job = Prod3MCPipeBaselineZstdJob()

  # override for testing
  job.setName('Prod3_BL_HighE_LaPalma_20deg_%s'%particle)

  # package and version
  job.setPackage('corsika_simhessarray')
  job.setVersion('2017-09-28_zstd')
  job.extra_tag = "--highE"
  job.configuration_id = 2

  # layout, site, particle, pointing direction, zenith angle
  # demo, LaPalma,  gamma, South,  20
  job.setArrayLayout(layout)
  job.setSite(site)
  job.setParticle(particle)
  job.setPointingDir(pointing)
  job.setZenithAngle(zenith)

  # 5 is enough for testing
  job.setNShower(nShower)

  ### Set the startrunNb here (it will be added to the Task_ID)
  startrunNb = '100000'
  job.setStartRunNumber(startrunNb)

  # set run number for TS submission: JOB_ID variable left for dynamic resolution during the Job. It corresponds to the Task_ID
  job.setRunNumber('@{JOB_ID}')

  # get dirac log files
  job.setOutputSandbox(['*Log.txt'])

  # add the sequence of executables
  job.setupWorkflow(debug=False)

  # submit to the Transformation System
  res = submitTS(job)

  # debug
  Script.gLogger.info(job.workflow)

  return res

#########################################################
if __name__ == '__main__':

  args = Script.getPositionalArgs()
  if (len(args) != 6):
    Script.showHelp()
  try:
    res = runProd3(args)
    if not res['OK']:
      DIRAC.gLogger.error (res['Message'])
      DIRAC.exit(-1)
    else:
      DIRAC.gLogger.notice('Done')
  except Exception:
    DIRAC.gLogger.exception()
    DIRAC.exit(-1)
