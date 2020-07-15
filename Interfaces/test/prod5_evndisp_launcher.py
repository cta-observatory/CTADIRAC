""" Launcher script to launch a Prod5 EvnDispProd4Job
on the WMS or create a Transformation.

    https://forge.in2p3.fr/issues/40751

  July 6th 2020 - J. Bregeon
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
from CTADIRAC.Interfaces.API.EvnDispProd5Job import EvnDispProd5Job
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
    trans.setDescription("Prod5 EventDisplay TS")
    trans.setLongDescription("Prod5 EventDisplay processing")  # mandatory
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
    base_path = '/vo.cta.in2p3.fr/MC/PROD5/LaPalma/gamma/sim_telarray/2104/Data/100xxx'
    input_data = ['%s/gamma_20deg_180deg_run100298___cta-prod5-lapalma_desert-2158m-LaPalma-dark.simtel.zst' % base_path,
    '%s/gamma_20deg_180deg_run100299___cta-prod5-lapalma_desert-2158m-LaPalma-dark.simtel.zst'%base_path]

    job.setInputData(input_data)
    job.setJobGroup('EvnDispProd5')
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
    job = EvnDispProd5Job(cpuTime=259200.)
    # override for testing
    job.setName('Prod5_EvnDisp')
    # output
    job.setOutputSandbox(['*Log.txt'])

    # specific configuration
    if mode == 'WMS':
        job.base_path = '/vo.cta.in2p3.fr/user/b/bregeon'
        job.ts_task_id = '111'
        simtel_meta_data = {'array_layout': 'Baseline-Advanced', 'site': 'LaPalma',
                           'particle': 'gamma', 'phiP': 0.0, 'thetaP': 20.0}
        job.prefix = 'CTA.prod5N'
        job.layout_list = 'BL-0LSTs05MSTs-MSTF BL-0LSTs05MSTs-MSTN \
                           BL-4LSTs00MSTs-MSTN BL-4LSTs05MSTs-MSTF \
                           BL-4LSTs05MSTs-MSTN BL-4LSTs09MSTs-MSTF \
                           BL-4LSTs09MSTs-MSTN BL-4LSTs15MSTs-MSTF \
                           BL-4LSTs15MSTs-MSTN'

        job.set_meta_data(simtel_meta_data)
        job.set_file_meta_data({'nsb':1})
        job.setupWorkflow(debug=True)
        # subtmit to the WMS for debug
        # job.setDestination('LCG.IN2P3-CC.fr')
        result = submit_wms(job)
    elif mode == 'TS':
        input_meta_query = get_dataset_MQ(dataset_name)
        # refine output meta data if needed
        output_meta_data = copy(input_meta_query)
        job.set_meta_data(output_meta_data)
        job.set_file_meta_data(nsb=output_meta_data['nsb']['='])
        if output_meta_data['site'] == 'LaPalma':
            job.prefix = 'CTA.prod5N'
            job.layout_list = 'BL-0LSTs05MSTs-MSTF BL-0LSTs05MSTs-MSTN \
                               BL-4LSTs00MSTs-MSTN BL-4LSTs05MSTs-MSTF \
                               BL-4LSTs05MSTs-MSTN BL-4LSTs09MSTs-MSTF \
                               BL-4LSTs09MSTs-MSTN BL-4LSTs15MSTs-MSTF \
                               BL-4LSTs15MSTs-MSTN'
            DIRAC.gLogger.notice('LaPalma layouts:\n',job.layout_list.split())
        elif output_meta_data['site'] == 'Paranal':
            DIRAC.gLogger.notice('Paranal layouts:\n',job.layout_list.split())
        # adjust calibration file
        if output_meta_data['nsb']['='] == 5:
            job.calibration_file = 'prod5/prod5-halfmoon-IPR.root'
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
