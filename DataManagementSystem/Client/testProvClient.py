"""
    Test for the Client
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1]]))

Script.parseCommandLine()

from ProvClient import ProvClient

provClient = ProvClient()

# CTAO Agent
agentDict = {'id':'CTAO', 'name':'CTA Observatory', 'type': 'Organization'}

res = provClient.addAgent(agentDict)

if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

res = provClient.getAgents()

if not res['OK']:
  DIRAC.gLogger.error(res['Message'])
  DIRAC.exit(-1)

print res['Value']
