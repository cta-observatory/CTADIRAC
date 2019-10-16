""" Class that contains client access to the Provenance DB handler. """

__RCSID__ = "$Id$"

# # from DIRAC
from DIRAC.Core.Base.Client import Client

class ProvClient(Client):

  """ Exposes the functionality available in the DataManagementSystem/ProvenanceManagerHandler
  """

  def __init__(self, url=None, **kwargs):
    """ Simple constructor
    """

    Client.__init__(self, **kwargs)
    res = self.serverURL = 'DataManagement/ProvenanceManager' if not url else url

  def addAgent(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addAgent(rowJSON)

  def addActivity(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addActivity(rowJSON)

  def addWasAssociatedWith(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addWasAssociatedWith(rowJSON)

  def addActivityDescription(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addActivityDescription(rowJSON)

  def addDatasetDescription(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addDatasetDescription(rowJSON)

  def addUsageDescription(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addUsageDescription(rowJSON)

  def addGenerationDescription(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addGenerationDescription(rowJSON)

  def addDatasetEntity(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addDatasetEntity(rowJSON)

  def addWasAttributedTo(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addWasAttributedTo(rowJSON)

  def addUsed(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addUsed(rowJSON)

  def addWasGeneratedBy(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addWasGeneratedBy(rowJSON)

  def addValueEntity(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addValueEntity(rowJSON)

  def addValueDescription(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addValueDescription(rowJSON)

  def getAgents(self):

    rpcClient = self._getRPC()
    return rpcClient.getAgents()
