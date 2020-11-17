""" Prod3 MC Pipe Script to create a Transformation
          JB, LA March 2018
"""

from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                       'Usage:',
                       '  %s array_layout site particle pointing_dir zenith_angle nShower' % Script.scriptName,
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
from CTADIRAC.Interfaces.API.Prod3MCPipeDivergentJob import Prod3MCPipeDivergentJob
# from Prod3MCPipeDivergentJob import Prod3MCPipeDivergentJob
from DIRAC.Interfaces.API.Dirac import Dirac


def submitTS(job):
    """ Create a transformation executing the job workflow  """

    # ## Temporary fix to initialize JOB_ID #######
    job.workflow.addParameter(Parameter("JOB_ID", "000000", "string", "", "",
                                        True, False, "Temporary fix"))
    job.workflow.addParameter(Parameter("PRODUCTION_ID", "000000", "string",
                                        "", "", True, False, "Temporary fix"))
    job.setType('MCSimulation')  # Used for the JobType plugin

    t = Transformation()
    # This must be unique. If not set it's asked in the prompt
    # t.setTransformationName("Prod3Exemple")
    t.setType("MCSimulation")
    t.setDescription("MC Prod3 BaseLine test")
    t.setLongDescription("corsika-simtel production")  # mandatory
    t.setBody(job.workflow.toXML())

    res = t.addTransformation()  # Transformation is created here

    if not res['OK']:
        print(res['Message'])
        DIRAC.exit(-1)

    t.setStatus("Active")
    t.setAgentType("Automatic")

    return res


def submitWMS(job):
    """ Submit the job to the WMS
    """
    job.setDestination('LCG.IN2P3-CC.fr')
    dirac = Dirac()
    res = dirac.submit(job)
    Script.gLogger.notice('Submission Result: ', res)
    return res


def runProd3(args=None):
    """ Simple wrapper to create a Prod3MCPipeDivergentJob and setup parameters
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
    nShower = args[5]
    div_cfg_id = args[6]

    # Job Wrapper
    job = Prod3MCPipeDivergentJob()

    # override for testing
    job.setName('Div_LaPalma_20deg_%s_%s' % (particle, div_cfg_id))

    # package and version
    # job.setPackage('corsika_simhessarray')
    # job.setVersion('2017-04-19-div')  # divergent pointing

    # layout, site, particle, pointing direction, zenith angle
    # Baseline, LaPalma,  gamma, South,  20
    job.setArrayLayout(layout)
    job.setSite(site)
    job.setParticle(particle)
    job.setPointingDir(pointing)

    # more attributes
    job.zenith_angle = zenith
    job.nShower = nShower
    job.start_run_number = '0'
    job.run_number = '@{JOB_ID}'  # dynamicly resolved

    # Divergent configuration ID
    job.div_cfg_id = div_cfg_id

    # get dirac log files
    job.setOutputSandbox(['*Log.txt'])

    # submit to the wms
#    job.run_number = '123'
#    job.setOutputData(['%s/Data/*.log.gz' % job.inputpath,
#                       '%s/Data/*.simtel.gz' % job.inputpath])
#    job.setupWorkflow(debug=True, register=False)
#    res = submitWMS(job)

    # submit to the Transformation System
    job.setupWorkflow(debug=False, register=True)
    res = submitTS(job)

    # debug
    Script.gLogger.info(job.workflow)

    return res


if __name__ == '__main__':
    args = Script.getPositionalArgs()
    if (len(args) != 7):
        Script.showHelp()
    try:
        res = runProd3(args)
        if not res['OK']:
            DIRAC.gLogger.error(res['Message'])
            DIRAC.exit(-1)
        else:
            DIRAC.gLogger.notice('Done')
    except Exception:
        DIRAC.gLogger.exception()
        DIRAC.exit(-1)
