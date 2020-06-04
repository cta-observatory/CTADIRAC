""" EvnDisp Script to create a Transformation with Input Data
"""

from copy import copy

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s inputDatasetName group_size' % Script.scriptName,
                                     'Arguments:',
                                     '  input_dataset_name group_size',
                                     '\ne.g: %s evndisp-gamma-N 5' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from CTADIRAC.Interfaces.API.EvnDisp3RefJobC7 import EvnDisp3RefJobC7
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter
from CTADIRAC.Core.Utilities.tool_box import get_dataset_MQ

def submit_wms(job):
    """ Submit the job to the WMS
    @todo launch job locally
    """
    dirac = Dirac()
    base_path = '/vo.cta.in2p3.fr/MC/PROD3/Paranal/gamma/simtel/2066/Data/000xxx'
    input_data = ['%s/gamma_20deg_0deg_run100___cta-prod3-demo_desert-2150m-Paranal-baseline.simtel.zst' % base_path,
                  '%s/gamma_20deg_0deg_run101___cta-prod3-demo_desert-2150m-Paranal-baseline.simtel.zst' % base_path]

    job.setInputData(input_data)
    result = dirac.submitJob(job)
    if result['OK']:
        Script.gLogger.notice('Submitted job: ', result['Value'])
    return result

def submit_trans(job, input_meta_query, group_size):
    """ Create a transformation executing the job workflow  """

    #DIRAC.gLogger.notice('submit_trans : %s' % trans_name)

    # Initialize JOB_ID
    job.workflow.addParameter( Parameter( "JOB_ID", "000000", "string", "", "",
                                       True, False, "Temporary fix" ) )
   
    t = Transformation()
    t.setType("DataReprocessing")
    t.setDescription("EvnDisplay MQ example")
    t.setLongDescription( "EvnDisplay calib_imgreco") # mandatory
    t.setBody (job.workflow.toXML())
    t.setGroupSize(group_size)
    t.setInputMetaQuery(input_meta_query)
    res = t.addTransformation()  # transformation is created here
    if not res['OK']:
        return res
      
    #t.setStatus("Active")
    t.setAgentType("Automatic")
    trans_id = t.getTransformationID()

    return trans_id
    
#########################################################

def runEvnDisp3(args):
  """ Simple wrapper to create a EvnDisp3RefJob and setup parameters
      from positional arguments given on the command line.

      Parameters:
      args -- (dataset_name group_size)
  """
  DIRAC.gLogger.notice( 'runEvnDisp3' )
  # get arguments
  mode = args[0]

  if mode == 'TS':
    dataset_name = args[1]
    group_size = int(args[2])

  ################################
  job = EvnDisp3RefJobC7(cpuTime = 432000)  # to be adjusted!!

  job.setName( 'EvnDisp3' )
  job.setType('EvnDisp3')
  job.setOutputSandbox(['*Log.txt'])

  job.version = "prod3b_d20200521"
  # change here for Paranal or La Palma
  job.prefix = "CTA.prod4S" # don't mind reference to prod4, it's prod3
  #  set calibration file and parameters file
  job.calibration_file ="prod3b.Paranal-20171214.ped.root"
  job.configuration_id = 6

  if mode == 'WMS':
    job.base_path = '/vo.cta.in2p3.fr/user/a/arrabito'
    job.ts_task_id = '1'
    output_meta_data = {'array_layout': 'Baseline', 'site': 'Paranal',
                         'particle': 'gamma', 'phiP': 0.0, 'thetaP': 20.0,
                         job.program_category + '_prog': 'simtel',
                         job.program_category + '_prog_version': job.version,
                         'data_level': 0, 'configuration_id': job.configuration_id}
    job.set_meta_data(output_meta_data)
    job.setupWorkflow(debug=True)
    # subtmit to the WMS for debug
    #job.setDestination('LCG.CNAF.it')
    res = submit_wms(job)
  elif mode == 'TS':
    input_meta_query = get_dataset_MQ(dataset_name)
    output_meta_data = copy(input_meta_query)
    job.set_meta_data(output_meta_data)
    job.ts_task_id = '@{JOB_ID}'  # dynamic
    job.setupWorkflow(debug=False)
    ### submit the workflow to the TS
    res = submit_trans(job, input_meta_query, group_size)
  else:
    DIRAC.gLogger.error('1st argument should be the job mode: WMS or TS,\n\not %s' % mode)
    return None

  return res

#########################################################
if __name__ == '__main__':

  
  arguments = Script.getPositionalArgs()
  if len(arguments) not in [1, 3]:
        Script.showHelp()
  try:
      result = runEvnDisp3(arguments)
      if not result['OK']:
          DIRAC.gLogger.error(result['Message'])
          DIRAC.exit(-1)
      else:
          DIRAC.gLogger.notice('Done')
  except Exception:
      DIRAC.gLogger.exception()
      DIRAC.exit(-1)

 
