""" Simple Example to launch ctapipe jobs adding provenance data
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )

# switches for job configuration
Script.registerSwitch('', 'backend=', 'Backend: TS,WMS,paramWMS default=WMS')
Script.registerSwitch('', 'trans_name=', 'Transformation name, mandatory for backend=TS')
Script.registerSwitch('', 'group_size=', 'Nb of input files per job, default=1')

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from ProvCtapipeJob import ProvCtapipeJob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

# Default values
backend = 'WMS'
trans_name = None
group_size = 1
output_type = None

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "backend":
    backend = switch[1]
  if switch[0].lower() == "trans_name":
    trans_name = switch[1]    
  elif switch[0].lower() == "group_size":
    group_size = int(switch[1])

# Mandatory switches    
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
  job.setName( 'prov_ctapipe' )

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
    """ Simple wrapper to create a ProvCtapipeJob 
    """
    # init FileCatalogClient
    fc = FileCatalogClient()
    
    # create and common job configuration
    job = ProvCtapipeJob(cpuTime=432000)
    job.setName('prov_ctapipe job')
    job.version = 'v0.6.2'
    #job.setDestination('LCG.IN2P3-CC.fr')
    job.setType('DataReprocessing')  # Needed for job meshing. Check Operations/CTA/JobTypeMapping on the CS
    input_file_list = ['/vo.cta.in2p3.fr/user/a/arrabito/ProvTest/proton_20deg_180deg_run22___cta-prod3-demo-2147m-LaPalma-baseline.simtel.gz']
    #input_file_list = ['/vo.cta.in2p3.fr/MC/PROD3/LaPalma/proton/simtel/1431/Data/083xxx/proton_20deg_180deg_run83337___cta-prod3-demo-2147m-LaPalma-baseline.simtel.gz']
    #input_file_list = ['/vo.cta.in2p3.fr/user/a/arrabito/ProvTest/proton_20deg_180deg_run22___cta-prod3-demo-2147m-LaPalma-baseline.simtel.gz','/vo.cta.in2p3.fr/MC/PROD3/LaPalma/proton/simtel/1431/Data/083xxx/proton_20deg_180deg_run83337___cta-prod3-demo-2147m-LaPalma-baseline.simtel.gz']
    job.setInputData(input_file_list)
    job.setInputSandbox(['MuonRecProv.py','MuonDisplayerTool.py','AddProvData.py'])
    job.setOutputSandbox(['*Log.txt','muons_provRFC.txt'])

    # configure ctapipe job 
    job.output_type = 'DL2'
    
    # get the metadata of the inputs
    res = fc.getFileUserMetadata(input_file_list[0])
    if not res['OK']:
      DIRAC.gLogger.error("Failed to get user metadata:", res['Message'])
      return res
    
    meta_data_dict = res['Value']
    #set meta data for ctapipe outputs
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
