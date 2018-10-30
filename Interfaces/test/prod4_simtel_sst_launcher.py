""" Launcher script to launch a Prod4Prod4SimtelSSTJob
on the WMS or create a Transformation.

    https://forge.in2p3.fr/issues/35179
                    JB, October 2018
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
from CTADIRAC.Interfaces.API.Prod4SimtelSSTJob import Prod4SimtelSSTJob
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Interfaces.API.Dirac import Dirac
from CTADIRAC.Core.Utilities.tool_box import get_dataset_MQ


def submit_trans(job, trans_name, mqJson, group_size):
    """ Create a transformation executing the job workflow
    """
    DIRAC.gLogger.notice('submit_trans : %s' % trans_name)

    # Initialize JOB_ID
    job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))

    trans = Transformation()
    trans.setTransformationName(trans_name)  # this must be unique
    trans.setType("DataReprocessing")
    trans.setDescription("Prod4 Simtel TS")
    trans.setLongDescription("Prod4 Simtel SST only processing")  # mandatory
    trans.setBody(job.workflow.toXML())
    trans.setGroupSize(group_size)
    trans.setFileMask(mqJson) # catalog query is defined here
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
    # base_path = '/vo.cta.in2p3.fr/MC/PROD4/Paranal/proton/corsika/1829/Data/101xxx'
    # input_data = ['%s/run101000_proton_za20deg_azm0deg-paranal-sst.corsika.zst' % base_path,
    #               '%s/run101003_proton_za20deg_azm0deg-paranal-sst.corsika.zst' % base_path,
    #               '%s/run101006_proton_za20deg_azm0deg-paranal-sst.corsika.zst' % base_path]
    base_path = '/vo.cta.in2p3.fr/user/b/bregeon/Paranal/proton/corsika/0000/Data/000xxx'
    input_data = ['%s/run22_proton_za20deg_azm0deg-paranal-sst.corsika.zst' % base_path,
                  '%s/run23_proton_za20deg_azm0deg-paranal-sst.corsika.zst' % base_path]

    job.setInputData(input_data)
    job.setJobGroup('Prod4SimtelSSTJob')
    result = dirac.submit(job)
    if result['OK']:
        Script.gLogger.notice('Submitted job: ', result['Value'])
    return result

def run_simtel_sst(args):
    """ Simple wrapper to create a Prod4SimtelSSTJob and setup parameters
        from positional arguments given on the command line.

        Parameters:
        args -- mode (trans_name dataset_name group_size)
    """
    DIRAC.gLogger.notice('run_simtel_sst')
    # get arguments
    mode = args[0]

    if mode == 'TS':
        trans_name = args[1]
        dataset_name = args[2]
        group_size = int(args[3])

    # job setup - 72 hours
    job = Prod4SimtelSSTJob(cpuTime=259200.)
    # override for testing
    job.setName('Prod4_Simtel')
    job.version='2018-10-22'

    # output
    job.setOutputSandbox(['*Log.txt'])

    # specific configuration
    if mode == 'WMS':
        job.base_path = '/vo.cta.in2p3.fr/user/b/bregeon'
        job.ts_task_id = '123'
        output_meta_data = {'array_layout': 'Baseline-SST-only', 'site': 'Paranal',
                           'particle': 'proton', 'phiP': 0.0, 'thetaP': 20.0,
                           job.program_category + '_prog': 'simtel',
                           job.program_category + '_prog_version': job.version,
                           'data_level': 0, 'configuration_id': 4}
        job.set_meta_data(output_meta_data)
        job.setupWorkflow(debug=True)
        # subtmit to the WMS for debug
        job.setDestination('LCG.IN2P3-CC.fr')
        result = submit_wms(job)
    elif mode == 'TS':
        job.N_output_files = 5
        input_meta_query = get_dataset_MQ(dataset_name)
        # refine output meta data if needed
        output_meta_data = copy(input_meta_query)
        job.set_meta_data(output_meta_data)

        job.ts_task_id = '@{JOB_ID}'  # dynamic
        job.setupWorkflow(debug=False)
        job.setType('EvnDisp3')  # mandatory *here*
        result = submit_trans(job, trans_name, json.dumps(input_meta_query), group_size)
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
        result = run_simtel_sst(arguments)
        if not result['OK']:
            DIRAC.gLogger.error(result['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
