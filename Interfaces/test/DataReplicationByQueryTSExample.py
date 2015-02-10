#!/usr/bin/env python
"""
  Create a Transformation for bulk data replication based on Catalog Query
"""

__RCSID__ = "$Id"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName] ) )

Script.parseCommandLine()

def DataReplicationByQueryTSExample( args = None ) :

  from DIRAC.TransformationSystem.Client.Transformation import Transformation
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

  t = Transformation( )
  tc = TransformationClient( )

  t.setTransformationName("DM_ReplicationByQuery1") # This must vary 
  #t.setTransformationGroup("Group1")
  t.setType("Replication") 
  t.setSourceSE(['CYF-STORM-Disk','DESY-ZN-Disk']) # A list of SE where at least 1 SE is the valid one
  t.setTargetSE(['CEA-Disk'])
  t.setDescription("data Replication")
  t.setLongDescription( "data Replication" ) #mandatory

  t.setGroupSize(1)

  t.setPlugin("Broadcast")

  t.addTransformation() #transformation is created here
  t.setStatus("Active")
  t.setAgentType("Automatic")

  transID = t.getTransformationID()
  tc.createTransformationInputDataQuery(transID['Value'], {'particle': 'gamma','prodName':'Config_test300113','outputType':'Data','simtelArrayProdVersion':'prod-2_21122012_simtel','runNumSeries':'0'}) # Add files to Transformation based on Catalog Query


if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    DataReplicationByQuery_TS_Example( args )
  except Exception:
    Script.gLogger.exception()


