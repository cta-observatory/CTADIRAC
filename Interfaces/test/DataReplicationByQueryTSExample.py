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

from DIRAC.TransformationSystem.Client.Transformation import Transformation

def DataReplicationByQueryTSExample( args = None ) :

  t = Transformation( )

  t.setType("Replication") 
  t.setSourceSE(['CYF-STORM-Disk','DESY-ZN-Disk']) # A list of SE where at least 1 SE is the valid one
  t.setTargetSE(['CC-IN2P3-Disk'])
  t.setDescription("data Replication")
  t.setLongDescription( "data Replication" ) #mandatory

  t.setGroupSize(10)

  t.setPlugin("Broadcast")

  inputQuery = {'MCCampaign':'PROD4','site':'Paranal','calibimgreco_prog':'evndisp','calibimgreco_prog_version':'prod4_d20181110','particle':'gamma','array_layout':'Baseline-SST-only','configuration_id':4,'phiP':180.0,'thetaP':20.0,'outputType':'Data','data_level': 1}
  t.setInputMetaQuery(inputQuery)
  

  res = t.addTransformation() #transformation is created here
  if not res['OK']:
    DIRAC.gLogger.error(res['Message'])
    DIRAC.exit(-1)
    
  #t.setStatus("Active")
  #t.setAgentType("Automatic")
  

if __name__ == '__main__':

  args = Script.getPositionalArgs()

  try:
    DataReplicationByQueryTSExample( args )
  except Exception:
    Script.gLogger.exception()


