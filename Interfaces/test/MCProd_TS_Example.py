#!/usr/bin/env python
"""
  Create a Transformation for MC Simulation Production Jobs 
"""
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [runMin] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  cfgFile:    Corsika config file as in prod2_cfg.tar.gz',
                                     '  reprocessing configuration: STD/NSBX3/4MSST/SCSST/ASTRI/NORTH'] ) )

Script.parseCommandLine()

def MCProd_TS_Example( args = None ) :

  from DIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.Core.Workflow.Parameter import Parameter
  from DIRAC.Interfaces.API.Dirac import Dirac
  from CTADIRAC.Interfaces.API.ProdTSJob import ProdTSJob

  if (len(args) != 2):
    Script.gLogger.notice('Wrong number of arguments')
    Script.showHelp()

  cfgfile = args[0]
  simtelArrayConfig = args[1]

  j = ProdTSJob()

  j.setVersion('prod-2_15122013')

  j.setApplication('corsika_autoinputs')

  j.setProdName('ConfigTestTS')

  #j.setPathRoot('/vo.cta.in2p3.fr/MC/PROD2/') # official
  j.setPathRoot('/vo.cta.in2p3.fr/user/a/arrabito/MC/PROD2/') # for test

  j.setParameters(['fileCatalog.cfg','--template',cfgfile,'--mode','corsika_simtel_dst','--run_number', '@{JOB_ID}', '-N', '300', '-S',simtelArrayConfig,'--savecorsika','False'])

  j.setInputSandbox( ['LFN:/vo.cta.in2p3.fr/MC/PROD2/CFG_rev6956/prod2_cfg.tar.gz'])

  j.setOutputSandbox( ['*.log','applicationLog.txt'])

  j.setCPUTime(200000)

  j.workflow.addParameter(Parameter("JOB_ID","000000","string","","",True,False, "Temporary fix")) 
  j.workflow.addParameter(Parameter("PRODUCTION_ID","000000","string","","",True, False, "Temporary fix"))

  t = Transformation( )

  t.setTransformationName("ProdExemple") # This must vary 
  #t.setTransformationGroup("Group1")
  t.setType("MCSimulation")

  t.setDescription("MC prod example")
  t.setLongDescription( "corsika-simtel production" ) #mandatory
  t.setBody ( j.workflow.toXML() )

  t.addTransformation() #transformation is created here
  t.setStatus("Active")
  t.setAgentType("Automatic")


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    MCProd_TS_Example( args )
  except Exception:
    Script.gLogger.exception()


