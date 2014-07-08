#!/usr/bin/env python
"""
  Create a Transformation for bulk data removal based on Catalog Query
"""

__RCSID__ = "$Id"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName] ) )

Script.parseCommandLine()

def DataRemovalByQuery_TS_Example( args = None ) :

  from DIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  t = Transformation( )
  tc = TransformationClient( )

  t.setTransformationName("DM_RemovalQuery1") # Must be unique 
  #t.setTransformationGroup("Group1")
  t.setType("Removal")
  t.setPlugin("Standard") # Not needed. The default is 'Standard'

  t.setDescription("corsika Removal")
  t.setLongDescription( "corsika Removal" ) # Mandatory

  t.setGroupSize(2) # Here you specify how many files should be grouped within the same request, e.g. 100 
  t.setBody ( "Removal;RemoveFile" ) # Mandatory (the default is a ReplicateAndRegister operation)

  t.addTransformation() # Transformation is created here
  t.setStatus("Active")
  t.setAgentType("Automatic")

  transID = t.getTransformationID()
  tc.createTransformationInputDataQuery(transID['Value'], {'particle': 'proton','prodName':'ConfigTestTS9','outputType':'Data'}) # Add files to Transformation based on Catalog Query

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    DataRemovalByQuery_TS_Example( args )
  except Exception:
    Script.gLogger.exception()


