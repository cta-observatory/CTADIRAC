""" SimpleCtapipe script to create a Transformation with Input Data
    for La Palma Baseline analysis

@authors: J. Bregeon, L. Arrabito, D. Landriu, J. Lefaucheur
            April 2018
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:', '  %s infile' % Script.scriptName,
                                  'Arguments:',
                                  ' something:
                                  '\ne.g: %s to_be_define'
                                  % Script.scriptName,
                                  ]))

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
#from CTADIRAC.Interfaces.API.SimpleCtapipeJob import SimpleCtapipeJob
from SimpleCtapipeJob import SimpleCtapipeJob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter


def read_lfns_from_file(file_path):
    """ Read a simple list of LFNs from an ASCII files,
    expects just one LFN per line
    """
    content = open(file_path, 'r').readlines()
    input_file_list = []
    for line in content:
        infile = line.strip()
        if line != "\n":
            input_file_list.append(infile)
    return input_file_list

def submit_TS_no_MQ(job, infileList):
    """ Create a transformation executing the job workflow
    Input files are given as a list of LFNs
    """
    t = Transformation()
    tc = TransformationClient()
    t.setType("DataReprocessing")
    t.setDescription("Runs ImageExtractor analysis for array HB9 SCT")
    t.setLongDescription("merge_simtel, ImageExtractor DL0->DL1 conversion for HB9 SCT")
    t.setGroupSize(10)
    t.setBody(job.workflow.toXML())

    res = t.addTransformation()  # Transformation is created here

    if not res['OK']:
        Script.gLogger.error(res['Message'])
        DIRAC.exit(-1)

    t.setStatus("Active")
    t.setAgentType("Automatic")
    transID = t.getTransformationID()
    Script.gLogger.notice('Adding %s files to transformation' %
                          len(infileList))
    tc.addFilesToTransformation(transID['Value'], infileList)
    return res

def submit_TS(job, transName, mqJson):
    """ Create a transformation executing the job workflow
    Input files are given through as an existing data set
    """
    DIRAC.gLogger.notice('submitTS')

    # Initialize JOB_ID
    job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))

    t = Transformation()
    t.setTransformationName(transName)  # this must be unique
    t.setType("DataReprocessing")
    t.setDescription("SimpleCtapipe example")
    t.setLongDescription("ctapipe classify, reconstruct and merge: calib_imgreco")  # mandatory
    t.setBody(job.workflow.toXML())
    t.setGroupSize(5)
    t.setFileMask(mqJson)  # catalog query is defined here
    t.addTransformation()  # transformation is created here
    t.setStatus("Active")
    t.setAgentType("Automatic")

    return t

def submit_WMS(job, infileList):
    """ Submit the job locally or to the WMS
    """
    dirac = Dirac()
    job.setInputData(infileList)
    job.setJobGroup('SimpleCtapipe-test')
    res = dirac.submit(job)
    Script.gLogger.notice('Submission Result: ', res)
    return res

def run_ctapipe(args):
    """ Simple wrapper to create a SimpleCtapipeJob and setup parameters
        from positional arguments given on the command line.

        Parameters:
        args -- mode file_path/data_set
    """
    # get arguments
    mode = args[0]
    if mode is 'WMS':
        file_path = args[1]
    else if mode is 'TS':
        input_data_set = args[1]
    else:
        DIRAC.gLogger.error('1st argument should be the job mode: WMS or TS')
        DIRAC.exit(-1)

    # create and common job configuration
    job = SimpleCtapipeJob(cpuTime=432000)
    job.version = 'v0.5.3'
    job.setType('DataReprocessing')
    job.setOutputSandbox(['*Log.txt'])

    # specific configuration
    if mode is 'WMS':
        # read list of input file names

        # set meta data
        job.set_metadata(infileList[0])
        job.setupWorkflow(debug=True)

        # subtmit to the WMS for debug
        job.setDestination('LCG.IN2P3-CC.fr')
        # job.setDestination('LCG.DESY-ZEUTHEN.de')
        # job.setDestination('LCG.GRIF.fr')
        res = submit_WMS(job, infileList[:2])
    else

    # submit to the Transformation System
    res = submit_TS_no_MQ(job, infileList[:100])


    return res


#########################################################
if __name__ == '__main__':

    args = Script.getPositionalArgs()
    if len(args) != 1:
        Script.showHelp()
    try:
        res = run_ctapipe(args)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
