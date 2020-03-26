""" Prod3 MC Pipe Script to run realistic test jobs
          JB, March 2020
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s array_layout site particle pointing_dir zenith_angle nShower' % Script.scriptName,
                                     'Arguments:',
                                     '  mode: WMS or TS',
                                     '  array_layout: demo',
                                     '  site: Paranal or LaPalma',
                                     '  particle: gamma, proton, electron',
                                     '  pointing_dir: North or South',
                                     '  zenith_agle: 20',
                                     '  nShower: from 5 to 25000',
                                     '\ne.g: %s TS Baseline Paranal gamma South 20 5'% Script.scriptName,
                                     ] ) )

Script.parseCommandLine()

import DIRAC
from DIRAC.TransformationSystem.Client.Transformation import Transformation
from DIRAC.Core.Workflow.Parameter import Parameter
from CTADIRAC.Interfaces.API.Prod3MCPipeTestJob import Prod3MCPipeTestJob
from DIRAC.Interfaces.API.Dirac import Dirac


def submit_trans(job, trans_name):
    """ Create a transformation executing the job workflow  """
    DIRAC.gLogger.notice('submit_trans : %s' % trans_name)

    # Initialize JOB_ID
    job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))
    job.workflow.addParameter( Parameter( "PRODUCTION_ID", "000000", "string",
                                        "", "", True, False, "Temporary fix" ) )
    job.setType('MCSimulation') ## Used for the JobType plugin

    trans = Transformation()
    trans.setTransformationName(trans_name)
    trans.setType("MCSimulation")
    trans.setDescription("MC Prod3 BaseLine Corsika7 test")
    trans.setLongDescription("corsika-simtel production")  # mandatory
    trans.setBody(job.workflow.toXML())
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
    job.setJobGroup('Prod3MCPipeTestJob')
    result = dirac.submitJob(job)
    if result['OK']:
        Script.gLogger.notice('Submitted job: ', result['Value'])
    return result

#########################################################

def run_prod3( args = None ):
    """ Simple wrapper to create a Prod3MCPipeBaselineJob and setup parameters
      from positional arguments given on the command line.

      Parameters:
      args -- a list of 6 strings corresponding to job arguments
              array_layout site particle pointing_dir zenith_angle nShower
              demo LaPalma  gamma South 20 1000
    """
    # get arguments
    mode = args[0]
    layout = args[1]
    site = args[2]
    particle = args[3]
    pointing = args[4]
    zenith = args[5]
    nShower= args[6]

    ### Main Script ###
    job = Prod3MCPipeTestJob()

    # override for testing
    job.setName('BL_Corsika7_Test_Paranal_20deg_%s'%particle)

    # package and version
    job.setPackage('corsika_simtelarray')
    job.setVersion( '2019-09-03' )  # final with fix for gamma-diffuse
    job.configuration_id=-1

    # layout, site, particle, pointing direction, zenith angle
    # demo, LaPalma,  gamma, South,  20
    job.setArrayLayout(layout)
    job.setSite(site)
    job.setParticle(particle)
    job.setPointingDir(pointing)
    job.setZenithAngle( zenith )

    # 5 is enough for testing
    job.setNShower(nShower)

    ### Set the startrunNb here (it will be added to the Task_ID)
    startrunNb = '0'
    job.setStartRunNumber( startrunNb )

    # set run number for TS submission: JOB_ID variable left for dynamic resolution during the Job. It corresponds to the Task_ID
    job.setRunNumber( '@{JOB_ID}' )

    # get dirac log files
    job.setOutputSandbox( ['*Log.txt'] )

    # specific configuration
    if mode == 'WMS':
        job.base_path = '/vo.cta.in2p3.fr/user/b/bregeon'
        job.start_run_number = '100'
        job.run_number = '31'
        job.setupWorkflow(debug=True)
        # subtmit to the WMS for debug
        job.setDestination('LCG.IN2P3-CC.fr')
        # job.setDestination('LCG.CIEMAT.es')
        result = submit_wms(job)
    elif mode == 'TS':
        job.base_path = '/vo.cta.in2p3.fr/MC/PRODTest/'
        job.start_run_number = '0'
        job.run_number = '@{JOB_ID}'  # dynamic
        job.setupWorkflow(debug=False)
        job.setType('MCSimulation')
        tag = ''  # _test9_MCSimPostWorkflow_5k'
        trans_name = 'MC_Prod3_Test_%s_%s_%s_%s%s' %\
                    (job.cta_site, job.particle, job.pointing_dir, job.zenith_angle, tag)
        result = submit_trans(job, trans_name)
    else:
        DIRAC.gLogger.error('1st argument should be the job mode: WMS or TS,\n\
                             not %s' % mode)
        return None

    return result

#########################################################
if __name__ == '__main__':

    arguments = Script.getPositionalArgs()
    if len(arguments) != 7:
        Script.showHelp()
    try:
        result = run_prod3(arguments)
        if not result['OK']:
            DIRAC.gLogger.error(result['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
