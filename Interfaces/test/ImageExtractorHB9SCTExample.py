""" ImageExtractor cript to create a Transformation with Input Data
    for HB9 SCT analysis
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:', '  %s infile' % Script.scriptName,
                                  'Arguments:',
                                  ' infile: ascii file with input files LFNs',
                                  '\ne.g: %s Paranal_gamma_North_HB9_merged.list'
                                  % Script.scriptName,
                                  ]))

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient 
from CTADIRAC.Interfaces.API.ImageExtractorHB9SCTJob import ImageExtractorHB9SCTJob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter


# submit to TS with no Meta Query, add files manually
def submit_TS_no_MQ(job, infileList):
    """ Create a transformation executing the job workflow
    """
    t = Transformation()
    tc = TransformationClient()
    t.setType("DataReprocessing")
    t.setDescription("Runs ImageExtractor analysis for array HB9 SCT")
    t.setLongDescription("merge_simtel, ImageExtractor DL0->DL1 conversion for HB9 SCT")
    t.setGroupSize(5)
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


# submit TS - via Meta Query
def submit_TS(job, transName, mqJson):
    """ Create a transformation executing the job workflow
    """
    DIRAC.gLogger.notice('submitTS')

    # Initialize JOB_ID
    job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))

    t = Transformation()
    t.setTransformationName(transName)  # this must be unique
    t.setType("DataReprocessing")
    t.setDescription("ImageExtractor HB9 SCT example")
    t.setLongDescription("Image Extractor calib_imgreco")  # mandatory
    t.setBody(job.workflow.toXML())
    t.setGroupSize(5)
    t.setFileMask(mqJson)  # catalog query is defined here
    t.addTransformation()  # transformation is created here
    t.setStatus("Active")
    t.setAgentType("Automatic")

    return


# Submit WMS
def submit_WMS(job, infileList):
    """ Submit the job locally or to the WMS
    """
    dirac = Dirac()
    job.setInputData(infileList)
    job.setJobGroup('IE-HB9-SCT-test')
    res = dirac.submit(job)
    Script.gLogger.notice('Submission Result: ', res)
    return res


def run_IE_HB9SCT(args):
    """ Simple wrapper to create a ImageExtractorHB9SCTJob and setup parameters
        from positional arguments given on the command line.

        Parameters:
        args -- infile mode
    """
    # get arguments
    infile = args[0]

    # debug with WMS - list of input file names
    f = open(infile, 'r')
    infileList = []
    for line in f:
        infile = line.strip()
        if line != "\n":
            infileList.append(infile)

    # create and configure job
    job = ImageExtractorHB9SCTJob(cpuTime=36000)
    job.setType('DataReprocessing')
    job.setVersion('v0.5.1')
    job.set_metadata(infileList[0])
    job.setOutputSandbox(['*Log.txt'])
    job.setupWorkflow(debug=True)

    # submit to the Transformation System
    # res = submit_TS_no_MQ(job, infileList[:10])

    # or to the WMS for debug
    res = submit_WMS(job, infileList[:2])

    return res


#########################################################
if __name__ == '__main__':

    args = Script.getPositionalArgs()
    if len(args) != 1:
        Script.showHelp()
    try:
        res = run_IE_HB9SCT(args)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
