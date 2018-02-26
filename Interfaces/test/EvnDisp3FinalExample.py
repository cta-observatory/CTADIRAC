""" EvnDisp Script to create a Transformation with Input Data
    version created to run the Final version of EventDisplay
    https://forge.in2p3.fr/issues/27528
                        February 16th 2018 - J. Bregeon
"""

import json
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s trans_name input_dataset_name group_size' % Script.scriptName,
                                     'Arguments:',
                                     '  trans_name: name of the transformation',
                                     '  input_dataset_name: name of the input dataset',
                                     '  group_size: n files to process',
                                     '\ne.g: %s evndisp-gamma-N Paranal_gamma_North_20deg_HB9' % Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from CTADIRAC.Interfaces.API.EvnDisp3FinalJob import EvnDisp3FinalJob
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult


def get_dataset_MQ(dataset_name):
    """ Return the Meta Query associated with a given dataset
    """
    fc = FileCatalogClient()
    result = returnSingleResult(fc.getDatasetParameters(dataset_name))
    if not result['OK']:
        DIRAC.gLogger.error("Failed to retrieved dataset:",
                            result['Message'])
        DIRAC.exit(-1)
    else:
        DIRAC.gLogger.info("Successfully retrieved dataset: ", dataset_name)
    return result['Value']['MetaQuery']

def submit_trans(job, transName, mqJson, group_size):
    """ Create a transformation executing the job workflow
    """
    DIRAC.gLogger.notice('submitTS : %s' % transName)

    # Initialize JOB_ID
    job.workflow.addParameter( Parameter( "JOB_ID", "000000", "string", "", "",
                                       True, False, "Temporary fix" ) )

    trans = Transformation()
    trans.setTransformationName(transName) # this must be unique
    trans.setType("DataReprocessing")
    trans.setDescription("EvnDisplay MQ example")
    trans.setLongDescription( "EvnDisplay calib_imgreco") # mandatory
    trans.setBody(job.workflow.toXML())
    trans.setGroupSize(group_size)
    trans.setFileMask(mqJson) # catalog query is defined here
    trans.addTransformation() # transformation is created here
    trans.setStatus("Active")
    trans.setAgentType("Automatic")
    return trans

def runEvnDisp3MQ(args=None):
    """ Simple wrapper to create a EvnDisp3RefJob and setup parameters
        from positional arguments given on the command line.

        Parameters:
        args -- infile mode
    """
    DIRAC.gLogger.notice( 'runEvnDisp3' )
    # get arguments
    transName = args[0]
    dataset_name = args[1]
    group_size = int(args[2])

    ################################
    job = EvnDisp3FinalJob(cpuTime=432000)  # to be adjusted!!

    ### Main Script ###
    # override for testing
    job.setName('EvnDisp3')
    ## add for testing
    job.setType('EvnDisp3')

    # change here for Paranal or La Palma # prefix
    #job.prefix("CTA.prod3Sb")
    #job.calibration_file = 'prod3b.Paranal-20171214.ped.root'

    # get input data set meta query
    # MDdict = {'MCCampaign':'PROD3', 'particle':particle,
    #           'array_layout':'full', 'site':'Paranal',
    #           'outputType':'Data', 'thetaP':{"=": 20}, 'phiP':{"=": 180.0},
    #           'tel_sim_prog':'simtel', 'tel_sim_prog_version':'2016-06-28',
    #           'sct'=False}
    meta_data_dict = get_dataset_MQ(dataset_name)
    # refining query as version was missing from data set MQ
    meta_data_dict['tel_sim_prog_version']='2016-06-28'
    # refining query to remove SCT files
    meta_data_dict['sct']='False'

    job.setEvnDispMD(meta_data_dict)

    # add the sequence of executables
    job.ts_task_id='@{JOB_ID}' # dynamic
    job.setupWorkflow(debug=False)

    # output
    job.setOutputSandbox( ['*Log.txt'] )

    ### submit the workflow to the TS
    res = submit_trans(job, transName, json.dumps(meta_data_dict), group_size)

    return res


#########################################################
if __name__ == '__main__':

    args = Script.getPositionalArgs()
    if ( len( args ) != 3 ):
        Script.showHelp()

    runEvnDisp3MQ(args)
