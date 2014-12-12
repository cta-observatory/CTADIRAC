#!/usr/bin/env python
"""
  Create a Transformation for MC Simulation Production Jobs 
"""
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [corsikaTemplate] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  corsikaTemplate:    Corsika config file as in /vo.cta.in2p3.fr/MC/PROD2/xxx/prod2_cfg.tar.gz',
                                     '  reprocessing configuration: STD/NSBX3/4MSST/SCSST/ASTRI/NORTH/3INROW'] ) )

Script.parseCommandLine()

def MCProdTSExample( args = None ) :

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

  j.setVersion('prod-2_13112014b')

  j.setApplication('corsika_autoinputs')

  j.setProdName('ConfigxxxTS')

  #j.setPathRoot('/vo.cta.in2p3.fr/MC/PROD2/') # official
  j.setPathRoot('/vo.cta.in2p3.fr/user/a/arrabito/MC/PROD2/') # for test

  #mode = 'corsika_standalone'
  #mode = 'corsika_simtel'
  mode = 'corsika_simtel_dst'

  start_run_number = 0 

  j.setParameters(['fileCatalog.cfg','--template',cfgfile,'--mode',mode,'--run_number', '@{JOB_ID}', '-i', start_run_number, '-N', '25000', '-S',simtelArrayConfig,'--savecorsika','False'])

  j.setInputSandbox( ['LFN:/vo.cta.in2p3.fr/MC/PROD2/Armazones2K/prod2_cfg.tar.gz'])

  j.setOutputSandbox( ['*.log'])

  j.setCPUTime(200000)

  ### Temporary fix #######
  j.workflow.addParameter(Parameter("JOB_ID","000000","string","","",True,False, "Temporary fix")) 
  j.workflow.addParameter(Parameter("PRODUCTION_ID","000000","string","","",True, False, "Temporary fix"))

  t = Transformation( )

  t.setTransformationName("ProdExemple") # This must be unique
  #t.setTransformationGroup("Group1")
  t.setType("MCSimulation")

  t.setDescription("MC prod example")
  t.setLongDescription( "corsika-simtel production" ) #mandatory
  t.setBody ( j.workflow.toXML() )

  res = t.addTransformation() # Transformation is created here

  if not res['OK']:
    print res['Message']
    DIRAC.exit(-1)

  t.setStatus("Active")
  t.setAgentType("Automatic")


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    MCProdTSExample( args )
  except Exception:
    Script.gLogger.exception()


