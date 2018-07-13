""" SimpleCtapipe script to create a Transformation with Input Data
    for La Palma Baseline analysis

@authors: J. Bregeon, L. Arrabito, D. Landriu, J. Lefaucheur
            April 2018
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:', '  %s infile' % Script.scriptName,
                                  'Arguments:',
                                  ' WMS mode: WMS file_path group_size
                                  ' TS mode: TS trans_name dataset_name group_size'
                                  '\ne.g: %s TS ctapipe_exp_test Prod3_LaPalma_Baseline_NSB1x_gamma_North_20deg_DL0 5'
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
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult


def check_dataset_query(dataset_name):
    """ print dfind command for a given dataset
    """
    md_dict = get_dataset_MQ(dataset_name)
    return debug_query(md_dict)

def debug_query(MDdict):
    """ just unwrap a meta data dictionnary into a dfind command
    """
    msg='dfind /vo.cta.in2p3.fr/MC/'
    for key,val in MDdict.items():
        try:
            val = val.values()[0]
        except:
            pass
        msg+=' %s=%s' % (key, val)
    return msg

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

def submit_trans(job, transName, mqJson, group_size):
    """ Create a transformation executing the job workflow
    Input files are given through as an existing data set
    """
    DIRAC.gLogger.notice('submit_trans : %s' % transName)

    # Initialize JOB_ID
    job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))

    trans = Transformation()
    trans.setTransformationName(transName)  # this must be unique
    trans.setType("DataReprocessing")
    trans.setDescription("SimpleCtapipe example")
    trans.setLongDescription("ctapipe classify, reconstruct and merge: calib_imgreco")  # mandatory
    trans.setBody(job.workflow.toXML())
    trans.setGroupSize(group_size)
    trans.setFileMask(mqJson)  # catalog query is defined here
    trans.addTransformation()  # transformation is created here
    trans.setStatus("Active")
    trans.setAgentType("Automatic")
    return trans

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
    if mode == 'WMS':
        file_path = args[1]
        group_size = int(args[2])
    elif mode == 'TS':
        trans_name = args[1]
        dataset_name = args[2]
        group_size = int(args[3])
    else:
        DIRAC.gLogger.error('1st argument should be the job mode: WMS or TS')
        DIRAC.exit(-1)

    # create and common job configuration
    job = SimpleCtapipeJob(cpuTime=432000)
    job.setName('ctapipe_exp')
    job.version = 'v0.5.3'
    job.setType('DataReprocessing')  # use EvnDisp3 to use all sites
    job.setOutputSandbox(['*Log.txt'])
    job.prefix = 'CTA.prod3Nb'

    # specific configuration
    if mode == 'WMS':
        # read list of input file names
        input_file_list = read_lfns_from_file(file_path)
        # set meta data
        job.set_metadata(input_file_list[0])
        job.ts_task_id = '0'
        job.setupWorkflow(debug=True)
        # subtmit to the WMS for debug
        job.setDestination('LCG.IN2P3-CC.fr')
        # job.setDestination('LCG.DESY-ZEUTHEN.de')
        # job.setDestination('LCG.GRIF.fr')
        res = submit_WMS(job, input_file_list[:group_size])

    elif mode == 'TS':
        # get input data set meta query
        # MDdict = {'MCCampaign':'PROD3', 'particle':particle,
        #           'array_layout':'full', 'site':'Paranal',
        #           'outputType':'Data', 'thetaP':{"=": 20}, 'phiP':{"=": 180.0},
        #           'tel_sim_prog':'simtel', 'tel_sim_prog_version':'2016-06-28',
        #           'sct'=False}
        meta_data_dict = get_dataset_MQ(dataset_name)
        # if needed, refine query for missing items (20 deg)
        # meta_data_dict['tel_sim_prog_version']='2016-06-28'
        # refining query to remove SCT files
        # meta_data_dict['sct']='False'
        # set meta data
        job.set_metadata(meta_data_dict)

        # add the sequence of executables
        job.ts_task_id = '@{JOB_ID}' # dynamic
        job.setupWorkflow(debug=True)
        # submit to the Transformation System
        res = submit_trans(job, trans_name, json.dumps(meta_data_dict),
                           group_size)
    return res


#########################################################
if __name__ == '__main__':

    args = Script.getPositionalArgs()
    if len(args) < 3:
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
