#!/usr/bin/env python
"""
  Create a Transformation for MC Simulation Production Jobs using pipe
"""
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [corsikaTemplate] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  corsikaTemplate:    Corsika config file as in /vo.cta.in2p3.fr/MC/PROD2/CORSIKA_INPUTS/template.tar.gz',
                                     '  reprocessing configuration: 3INROW'] ) )

Script.parseCommandLine()

def MCProdPipeTSExample( args = None ) :

  from DIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.Core.Workflow.Parameter import Parameter
  from DIRAC.Interfaces.API.Dirac import Dirac
  from CTADIRAC.Interfaces.API.ProdPipeTSJob import ProdPipeTSJob

  if (len(args) != 2):
    Script.gLogger.notice('Wrong number of arguments')
    Script.showHelp()

  cfgfile = args[0]
  simtelArrayConfig = args[1]

  j = ProdPipeTSJob()

  j.setVersion('prod-2_13112014')

  j.setApplication('corsika_autoinputs')

  j.setProdName('ConfigTestArmaz051214')

  #j.setPathRoot('/vo.cta.in2p3.fr/MC/PROD2/') # official
  j.setPathRoot('/vo.cta.in2p3.fr/user/a/arrabito/MC/PROD2/') # for test

  ####### example ##################
  j.setBannedSites(['LCG.UNI-DORTMUND.de','ARC.SE-SNIC-T2.se','LCG.INFN-TORINO.it','LCG.CAMK.pl','LCG.CYFRONET.pl'])

  mode = 'corsika_simtel_dst'

  j.setParameters(['fileCatalog.cfg','--template',cfgfile,'--mode',mode,'--run_number', '@{JOB_ID}', '-N', '25000', '-S',simtelArrayConfig,'--savecorsika','False'])

  j.setInputSandbox( ['LFN:/vo.cta.in2p3.fr/MC/PROD2/CORSIKA_INPUTS/template.tar.gz']) # to be updated if needed

  j.setOutputSandbox( ['*.log'])

  j.setCPUTime(720000)

  ### Temporary fix #######
  j.workflow.addParameter(Parameter("JOB_ID","000000","string","","",True,False, "Temporary fix")) 
  j.workflow.addParameter(Parameter("PRODUCTION_ID","000000","string","","",True, False, "Temporary fix"))

  t = Transformation( )

  t.setTransformationName("Armazones2K") # This must be unique
  #t.setTransformationGroup("Group1")
  t.setType("MCSimulation")

  t.setDescription("MC prod example")
  t.setLongDescription( "corsika-simtel production" ) #mandatory
  t.setBody ( j.workflow.toXML() )

  t.addTransformation() # Transformation is created here
  t.setStatus("Active")
  t.setAgentType("Automatic")


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    MCProdPipeTSExample( args )
  except Exception:
    Script.gLogger.exception()


