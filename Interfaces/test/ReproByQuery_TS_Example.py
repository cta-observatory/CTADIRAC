#!/usr/bin/env python
"""
  Create a Transformation for Reprocessing Production Jobs based on Catalog Query
"""
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  reprocessing configuration: STD/NSBX3/4MSST/SCSST/ASTRI/NORTH'] ) )

Script.parseCommandLine()

def ReproByQuery_TS_Example( args = None ) :

  from DIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from CTADIRAC.Interfaces.API.ReproTSJob import ReproTSJob

  if (len(args) != 1):
    Script.gLogger.notice('Wrong number of arguments')
    Script.showHelp()

  if args[0] not in ['STD','NSBX3','SCMST','SCSST','4MSST','ASTRI']:
    Script.gLogger.notice('reprocessing configuration incorrect:',args[0])
    Script.showHelp()

  simtelArrayConfig = args[0]

  j = ReproTSJob()

  j.setVersion('prod-2_15122013')

  j.setParameters(['fileCatalog.cfg','-S',simtelArrayConfig])

  j.setInputSandbox( ['LFN:/vo.cta.in2p3.fr/MC/PROD2/CFG_rev6956/prod2_cfg.tar.gz'])

  j.setOutputSandbox( ['*.log','applicationLog.txt'])

  j.setCPUTime(200000)

  t = Transformation( )
  tc = TransformationClient( )

  t.setTransformationName("ReprobyQuery_1") # This must vary 
  t.setType("DataReprocessing")

  t.setDescription("simtel repro example")
  t.setLongDescription( "simtel reprocessing" ) #mandatory
  t.setBody ( j.workflow.toXML() )

  t.addTransformation() #transformation is created here
  t.setStatus("Active")
  t.setAgentType("Automatic")
  transID = t.getTransformationID()
  tc.createTransformationInputDataQuery(transID['Value'], {'particle': 'proton','prodName':'ConfigtestTS','outputType':'Data'}) # Files are added here
 
if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    ReproReproByQuery_TS_Example( args )
  except Exception:
    Script.gLogger.exception()


