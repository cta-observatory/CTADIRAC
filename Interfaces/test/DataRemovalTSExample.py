#!/usr/bin/env python
"""
  Create a Transformation for bulk data removal 
"""
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [File List] ...' % Script.scriptName,
                                     'Arguments:',
                                     '  List of Files to remove'] ) )

Script.parseCommandLine()

def DataRemovalTSExample( args = None ) :

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

  t.setTransformationName("DM_Removal26") # Must be unique 
  #t.setTransformationGroup("Group1")
  t.setType("Removal")
  t.setPlugin("Standard") # Not needed. The default is 'Standard'

  t.setDescription("corsika Removal")
  t.setLongDescription( "corsika Removal" ) # Mandatory

  t.setGroupSize( 1 )  # Here you specify how many files should be grouped within the same request, e.g. 100
  t.setBody ( "Removal;RemoveFile" ) # Mandatory (the default is a ReplicateAndRegister operation)

  t.addTransformation() # Transformation is created here
  t.setStatus("Active")
  t.setAgentType("Automatic")

  transID = t.getTransformationID()
  tc.addFilesToTransformation(transID['Value'],infileList) # Files are added here


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    DataRemovalTSExample( args )
  except Exception:
    Script.gLogger.exception()


