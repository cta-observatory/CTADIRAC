#!/usr/bin/env python
"""
  Create a Transformation for bulk data replication
"""
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [File List] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  List of Files to replicate'] ) )

Script.parseCommandLine()

def DataReplicationTSExample( args = None ) :

  from DIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  if (len(args) != 1):
    Script.gLogger.notice('Wrong number of arguments')
    Script.showHelp()

  infile = args[0]
  f = open(infile,'r')

  infileList = []
  for line in f:
    infile = line.strip()
    if line!="\n":
      infileList.append(infile)

  t = Transformation( )
  tc = TransformationClient( )

  t.setTransformationName("DM_Replication") # This must be unique
  #t.setTransformationGroup("Group1")
  t.setType("Replication") 
  t.setSourceSE(['CYF-STORM-Disk','DESY-ZN-Disk']) # A list of SE where at least 1 SE is the valid one
  t.setTargetSE(['CEA-Disk'])
  t.setDescription("corsika Replication")
  t.setLongDescription( "corsika Replication" ) # Mandatory

  t.setGroupSize(1) # Here you specify how many files should be grouped within the same request, e.g. 100 

  t.setPlugin("Broadcast") # Mandatory for replication

  t.addTransformation() # Transformation is created here
  t.setStatus("Active")
  t.setAgentType("Automatic")

  transID = t.getTransformationID()
  tc.addFilesToTransformation(transID['Value'],infileList) # Files are added here


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    DataReplicationTSExample( args )
  except Exception:
    Script.gLogger.exception()


