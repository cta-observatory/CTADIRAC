""" Class that contains client access to the Provenance DB handler. """

__RCSID__ = "$Id$"

# # from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client

class ProvClient(Client):

  """ Exposes the functionality available in the DataManagementSystem/ProvenanceManagerHandler
  """

  def __init__(self, url=None, **kwargs):
    """ Simple constructor
    """

    Client.__init__(self, **kwargs)
    res = self.serverURL = 'DataManagement/ProvenanceManager' if not url else url

  def addAgent(self, agent):

    rpcClient = self._getRPC()
    return rpcClient.addAgent(agent)

  def getAgents(self):

    rpcClient = self._getRPC()
    return rpcClient.getAgents()
