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

from DIRAC.TransformationSystem.Client.Transformation import Transformation

def DataRemovalByQueryTSExample( args = None ) :

  t = Transformation( )

  t.setType("Removal")
  t.setPlugin("Standard") # Not needed. The default is 'Standard'

  t.setDescription("corsika Removal")
  t.setLongDescription( "corsika Removal" ) # Mandatory

  t.setGroupSize(100) # Here you specify how many files should be grouped within the same request, e.g. 100 
  t.setBody ( "Removal;RemoveFile" ) # Mandatory (the default is a ReplicateAndRegister operation)

  inputQuery = {"prodName":"ConfigSAC_24062013"}
  t.setInputMetaQuery(inputQuery)

  res = t.addTransformation() # Transformation is created here
  
  if not res['OK']:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)
  
  #t.setStatus("Active")
  #t.setAgentType("Automatic")
  
if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    DataRemovalByQueryTSExample( args )
  except Exception:
    Script.gLogger.exception()


