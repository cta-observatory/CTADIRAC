#!/usr/bin/env python
"""
  Create a Transformation for Reprocessing Production Jobs 
"""
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [inputfilelist] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  inputfilelist: Input File List',
                                     '  reprocessing configuration: STD/NSBX3/4MSST/SCSST/ASTRI/NORTH'] ) )

Script.parseCommandLine()

def ReproTSExample( args = None ) :

  from DIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from CTADIRAC.Interfaces.API.ReproTSJob import ReproTSJob

  if (len(args) != 2):
    Script.gLogger.notice('Wrong number of arguments')
    Script.showHelp()

  infile = args[0]
  f = open(infile,'r')

  infileList = []
  for line in f:
    infile = line.strip()
    if line!="\n":
      infileList.append(infile)

  if args[1] not in ['STD','NSBX3','SCMST','SCSST','4MSST','ASTRI','NORTH']:
    Script.gLogger.notice('reprocessing configuration incorrect:',args[1])
    Script.showHelp()

  simtelArrayConfig = args[1]

  j = ReproTSJob()

  j.setVersion('prod-2_15122013')

  j.setParameters(['fileCatalog.cfg','-S',simtelArrayConfig])

  j.setInputSandbox( ['LFN:/vo.cta.in2p3.fr/MC/PROD2/CFG_rev6956/prod2_cfg.tar.gz'])

  j.setOutputSandbox( ['*.log','applicationLog.txt'])

  j.setCPUTime(200000)

  t = Transformation( )
  tc = TransformationClient( )

  t.setTransformationName("Reprotest1") # This must vary 
  #t.setTransformationGroup("Group1")
  t.setType("DataReprocessing")

  t.setDescription("simtel repro example")
  t.setLongDescription( "simtel reprocessing" ) #mandatory
  t.setBody ( j.workflow.toXML() )

  t.addTransformation() #transformation is created here
  t.setStatus("Active")
  t.setAgentType("Automatic")
  transID = t.getTransformationID()
  tc.addFilesToTransformation(transID['Value'],infileList) # Files added here

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    Repro_TS_Example( args )
  except Exception:
    Script.gLogger.exception()


