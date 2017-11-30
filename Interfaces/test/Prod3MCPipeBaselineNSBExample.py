""" Prod3 MC Pipe Script to create a Transformation
          JB, LA April 2017
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s array_layout site particle pointing_dir\
                                   zenith_angle nShower' % Script.scriptName,
                                  'Arguments:',
                                  '  array_layout: demo',
                                  '  site: Paranal or LaPalma',
                                  '  particle: gamma, proton, electron',
                                  '  pointing_dir: North or South',
                                  '  zenith_agle: 20',
                                  '  nShower: from 5 to 25000',
                                  '\ne.g: %s Baseline Paranal gamma South 20 5'% Script.scriptName,
                                 ]))

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.Core.Workflow.Parameter import Parameter
from CTADIRAC.Interfaces.API.Prod3MCPipeBaselineNSBJob import Prod3MCPipeBaselineNSBJob
#from Prod3MCPipeBaselineNSBJob import Prod3MCPipeBaselineNSBJob


def submit_ts(job):
    """ Create a transformation executing the job workflow
    """
    # Temporary fix to initialize JOB_ID #######
    job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))
    job.workflow.addParameter(Parameter("PRODUCTION_ID", "000000", "string",
                                        "", "", True, False, "Temporary fix"))
    job.setType('MCSimulation')  # Used for the JobType plugin

    trans = Transformation()
    # This must be unique. If not set it's asked in the prompt
    # t.setTransformationName("Prod3Exemple")
    trans.setType("MCSimulation")
    trans.setDescription("MC Prod3 BaseLine NSB test")
    trans.setLongDescription("corsika-simtel production")  # mandatory
    trans.setBody(job.workflow.toXML())

    res = trans.addTransformation()  # Transformation is created here

    if not res['OK']:
        print(res['Message'])
        DIRAC.exit(-1)

    trans.setStatus("Active")
    trans.setAgentType("Automatic")

    return res

#########################################################


def run_prod3(args=None):
    """ Simple wrapper to create a Prod3MCPipeBaselineJob and setup parameters
    from positional arguments given on the command line.

    Parameters:
    args -- a list of 6 strings corresponding to job arguments
    array_layout site particle pointing_dir zenith_angle nShower
    demo LaPalma  gamma South 20 1000
    """
    # get arguments
    layout = args[0]
    site = args[1]
    particle = args[2]
    pointing = args[3]
    zenith = args[4]
    n_shower = args[5]

    # Main Script ###
    job = Prod3MCPipeBaselineNSBJob()

    # override for testing
    job.setName('BL_NSB_LaPalma_20deg_%s' % particle)

    # package and version
    job.setPackage('corsika_simhessarray')
    job.setVersion('2017-09-01-p1')  # final with fix for gamma-diffuse
    job.configuration_id = 1  # 0 exists already
    job.N_output_files = 2  # La Palma

    # layout, site, particle, pointing direction, zenith angle
    # demo, LaPalma,  gamma, South,  20
    job.setArrayLayout(layout)
    job.setSite(site)
    job.setParticle(particle)
    job.setPointingDir(pointing)
    job.setZenithAngle(zenith)

    # 5 is enough for testing
    job.setNShower(n_shower)

    # Set the startrunNb here (it will be added to the Task_ID)
    start_run_nb = '0'
    job.setStartRunNumber(start_run_nb)

    # set run number for TS submission: JOB_ID variable left for
    # dynamic resolution during the Job. It corresponds to the Task_ID
    job.setRunNumber('@{JOB_ID}')

    # get dirac log files
    job.setOutputSandbox(['*Log.txt'])

    # add the sequence of executables
    job.setupWorkflow(debug=True)

    # submit to the Transformation System
    res = submit_ts(job)

    # debug
    Script.gLogger.info(job.workflow)

    return res


#########################################################
if __name__ == '__main__':
    """ Do things
    """
    ARGS = Script.getPositionalArgs()
    if len(ARGS) != 6:
        Script.showHelp()
    try:
        RES = run_prod3(ARGS)
        if not RES['OK']:
            DIRAC.gLogger.error(RES['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except RuntimeError:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
