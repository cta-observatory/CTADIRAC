""" SimpleCtapipe script to create a Transformation

@authors: J. Bregeon, L. Arrabito, D. Landriu, J. Lefaucheur
            April 2018
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )

# switches for job configuration
Script.registerSwitch('', 'backend=', 'Backend: TS,WMS,paramWMS default=WMS')
Script.registerSwitch('', 'trans_name=', 'Transformation name, mandatory for backend=TS')
Script.registerSwitch('', 'group_size=', 'Nb of input files per job, default=1')
#  switches for ctapipe
Script.registerSwitch('', 'config_file=', 'Configuration file, mandatory')
Script.registerSwitch('', 'output_type=', 'Data level output, e.g. DL1 or DL2, str, mandatory')
Script.registerSwitch('', 'max_events=', 'Maximal number of events to be processed, int, optional')

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from CTADIRAC.Interfaces.API.SimpleCtapipeJob import SimpleCtapipeJob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from CTADIRAC.Core.Utilities.tool_box import read_lfns_from_file

# Default values
backend = 'WMS'
trans_name = None
group_size = 1
config_file = None
max_events = 10000000000
output_type = None

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "backend":
    backend = switch[1]
  if switch[0].lower() == "trans_name":
    trans_name = switch[1]    
  elif switch[0].lower() == "group_size":
    group_size = int(switch[1])
  elif switch[0].lower() == "config_file":
    config_file = switch[1]
  elif switch[0].lower() == "output_type":
    output_type = switch[1]
  elif switch[0].lower() == "max_events":
    max_events = switch[1]

# Mandatory switches    
if output_type is None:
  DIRAC.gLogger.error('Missing --output_type argument')
  DIRAC.exit(-1)
elif output_type not in ['DL1','DL2']:
  DIRAC.gLogger.error('Wrong --output_type')
  Script.showHelp()
  DIRAC.exit(-1)
  
if config_file is None:
  DIRAC.gLogger.error('Missing --config_file argument')
  DIRAC.exit(-1)

if backend == 'TS' and trans_name is None:
  DIRAC.gLogger.error('Missing --trans_name argument')
  DIRAC.exit(-1)
  
def submit_trans(job, trans_name, infileList, group_size):
    """ Create a transformation executing the job workflow
    """
    DIRAC.gLogger.notice('submit_trans : %s' % trans_name)

    # Initialize JOB_ID
    job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))

    trans = Transformation()
    trans.setTransformationName(trans_name)  # this must be unique
    trans.setType("DataReprocessing")
    trans.setDescription("SimpleCtapipe example")
    trans.setLongDescription("ctapipe classify, reconstruct and merge: calib_imgreco")  # mandatory
    trans.setBody(job.workflow.toXML())
    trans.setGroupSize(group_size)
    
    res = trans.addTransformation()  # transformation is created here
    if not res['OK']:
      DIRAC.gLogger.error('Error creating transformation: ', res['Message'])
      return res

    transID = res['Value']

    # Activate the transformation
    trans.setStatus("Active")
    trans.setAgentType("Automatic")
    
    # Add files to the transformation
    tc = TransformationClient()
    res = tc.addFilesToTransformation(transID , infileList)
    if not res['OK']:
      DIRAC.gLogger.error('Error adding files to the transformation: ', res['Message'])

    return res

def submit_paramWMS( job, infileList ):
  """ Submit parametric jobs to the WMS  """
  
  job.setParameterSequence( 'InputData', infileList, addToWorkflow = 'ParametricInputData' )
  job.setOutputData( ['*.h5'], outputPath='ctapipe_data2' )
  job.setName( 'ctapipejob' )

  dirac = Dirac()
  res = dirac.submit( job )

  if res['OK']:
    Script.gLogger.notice( 'Submission Result: ', res['Value'] )

  return res

def submit_WMS(job, infileList):
    """ Submit the job locally or to the WMS
    """
    dirac = Dirac()
    job.setInputData(infileList)
    res = dirac.submit(job)
    if res['OK']:
      Script.gLogger.notice('Submission Result: ', res['Value'])
    return res

def run_ctapipe():
    """ Simple wrapper to create a SimpleCtapipeJob and setup parameters
        from switches given on the command line
    """
    # init FileCatalogClient
    fc = FileCatalogClient()
    
    # create and common job configuration
    job = SimpleCtapipeJob(cpuTime=432000)
    job.setName('ctapipe job')
    job.version = 'v0.6.2'
    job.setType('DataReprocessing')  # Needed for job meshing. Check Operations/CTA/JobTypeMapping on the CS
    job.setOutputSandbox(['*Log.txt'])

    # configure ctapipe job from config_file and command line switches
    job.setConfig(config_file)
    job.output_type = output_type
    job.max_events = max_events
    
    # select the input file according to config_file parameters
    input_file = job.getInputfile()
    # read list of input file names
    input_file_list = read_lfns_from_file(input_file)
    # get the metadata of the inputs
    res = fc.getFileUserMetadata(input_file_list[0])
    if not res['OK']:
      DIRAC.gLogger.error("Failed to get user metadata:", result['Message'])
      return res
    meta_data_dict = res['Value']
    # set meta data for ctapipe outputs
    job.setCtapipeMD(meta_data_dict)
    # add the sequence of the xecutables 
    job.setupWorkflow(debug=True)

    # submit parametric jobs to WMS
    if backend == 'paramWMS':
        res = submit_paramWMS(job, input_file_list)
    # submit a single job to WMS for debug 
    if backend == 'WMS':
        res = submit_WMS(job, input_file_list)
    # submit to the Transformation System
    elif backend == 'TS':
        res = submit_trans(job, trans_name, input_file_list, group_size)

    return res


#########################################################
if __name__ == '__main__':

    try:
        res = run_ctapipe()
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
