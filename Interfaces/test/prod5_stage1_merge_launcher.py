""" Launcher script to launch a Prod5 ctapipe stage 1 job
on the WMS or create a Transformation.

  November 19th 2020 - J. Bregeon
                  bregeon@in2p3.fr
"""

import json
from copy import copy

from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s mode file_path (trans_name) group_size' % Script.scriptName,
                                  'Arguments:',
                                  '  mode: WMS for testing, TS for production',
                                  '  trans_name: name of the transformation',
                                  '  input_dataset_name: name of the input dataset',
                                  '  group_size: n files to process',
                                  '\ne.g: python %s.py WMS MyNewTrans Prod4-Paranal-gamma-North-DL-3 5' % Script.scriptName,
                                 ]))

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from CTADIRAC.Interfaces.API.Prod5Stage1MergeJob import Prod5Stage1MergeJob
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Interfaces.API.Dirac import Dirac
from CTADIRAC.Core.Utilities.tool_box import get_dataset_MQ


def submit_trans(job, trans_name, input_meta_query, group_size):
    """ Create a transformation executing the job workflow
    """
    DIRAC.gLogger.notice('submit_trans : %s' % trans_name)

    # Initialize JOB_ID
    job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))

    trans = Transformation()
    trans.setTransformationName(trans_name)  # this must be unique
    trans.setType("DataReprocessing")
    trans.setDescription("CTAPIPE Stage 1 Merge TS")
    trans.setLongDescription("CTAPIPE Stage 1 merging")  # mandatory
    trans.setBody(job.workflow.toXML())
    trans.setGroupSize(group_size)
    trans.setInputMetaQuery(input_meta_query)
    result = trans.addTransformation()  # transformation is created here
    if not result['OK']:
        return result
    trans.setStatus("Active")
    trans.setAgentType("Automatic")
    trans_id = trans.getTransformationID()
    return trans_id

def submit_wms(job):
    """ Submit the job to the WMS
    @todo launch job locally
    """
    dirac = Dirac()
    base_path = '/vo.cta.in2p3.fr/MC/PROD5/LaPalma/gamma/ctapipe-stage1/2284/Data/000xxx'
    input_data = ['%s/gamma_20deg_0deg_run183___cta-prod5-lapalma_desert-2158m-LaPalma-dark.h5' % base_path,
                  '%s/gamma_20deg_0deg_run184___cta-prod5-lapalma_desert-2158m-LaPalma-dark.h5' % base_path,
                  '%s/gamma_20deg_0deg_run182___cta-prod5-lapalma_desert-2158m-LaPalma-dark.h5' % base_path,
                  '%s/gamma_20deg_0deg_run181___cta-prod5-lapalma_desert-2158m-LaPalma-dark.h5' % base_path,
                  '%s/gamma_20deg_0deg_run176___cta-prod5-lapalma_desert-2158m-LaPalma-dark.h5' % base_path]

    job.setInputData(input_data)
    job.setJobGroup('prod5_ctapipe_stage1_merge')
    result = dirac.submitJob(job)
    if result['OK']:
        Script.gLogger.notice('Submitted job: ', result['Value'])
    return result


def launch_job(args):
    """ Simple launcher to instanciate a Job and setup parameters
        from positional arguments given on the command line.

        Parameters:
        args -- mode (trans_name dataset_name group_size)
    """
    DIRAC.gLogger.notice('Launching jobs')
    # get arguments
    mode = args[0]

    if mode == 'TS':
        trans_name = args[1]
        dataset_name = args[2]
        group_size = int(args[3])

    # job setup - 72 hours
    job = Prod5Stage1MergeJob(cpuTime=259200.)
    # job.stage1_config = 'stage1_config_Prod3_LaPalma_Baseline_NSB1x.json'
    # override for testing
    job.setName('Prod5_ctapipe_stage1_merge')
    # output
    job.setOutputSandbox(['*Log.txt'])

    # specific configuration
    if mode == 'WMS':
        job.base_path = '/vo.cta.in2p3.fr/user/b/bregeon'
        job.ts_task_id = '22'
        dl1_meta_data = {'array_layout': 'Advanced-Baseline', 'site': 'LaPalma',
                         'particle': 'gamma', 'phiP': 180.0, 'thetaP': 20.0,
                         'data_level': 1, 'nsb': 1, 'outputType': 'Data', 'configuration_id': 7,
                         'MCCampaign': 'PROD5', 'stage1_prog': 'ctapipe-stage1', 'split': 'train',
                         'stage1_prog_version': 'v0.10.0'}

        job.set_meta_data(dl1_meta_data)
        job.set_file_meta_data(nsb=1)
        job.setupWorkflow(debug=True)
        # subtmit to the WMS for debug
        # job.setDestination('LCG.IN2P3-CC.fr')
        result = submit_wms(job)
    elif mode == 'TS':
        job.group_size = group_size        
        job.base_path = '/vo.cta.in2p3.fr/MC/PROD5'
        input_meta_query = get_dataset_MQ(dataset_name)
        print(input_meta_query)
        # refine output meta data if needed
        output_meta_data = copy(input_meta_query)
        job.set_meta_data(output_meta_data)
        job.set_file_meta_data(nsb=output_meta_data['nsb']['='])  # ,
                               # split=output_meta_data['split'])
        input_meta_query = {}
        job.ts_task_id = '@{JOB_ID}'  # dynamic
        job.setupWorkflow(debug=False)
        job.setType('EvnDisp3')  # mandatory *here*
        result = submit_trans(job, trans_name, input_meta_query, group_size)
    else:
        DIRAC.gLogger.error('1st argument should be the job mode: WMS or TS,\n\
                             not %s' % mode)
        return None

    return result


#########################################################
if __name__ == '__main__':

    arguments = Script.getPositionalArgs()
    if len(arguments) not in [1, 4]:
        Script.showHelp()
    try:
        result = launch_job(arguments)
        if not result['OK']:
            DIRAC.gLogger.error(result['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
