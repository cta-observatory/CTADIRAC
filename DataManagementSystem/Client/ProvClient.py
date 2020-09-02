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

  def addActivity(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addActivity(rowJSON)

  def addDatasetEntity(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addDatasetEntity(rowJSON)

  def addValueEntity(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addValueEntity(rowJSON)

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

  def addAgent(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addAgent(rowJSON)

  def addWasAttributedTo(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addWasAttributedTo(rowJSON)

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

  def addValueDescription(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addValueDescription(rowJSON)

  def addWasConfiguredBy(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addWasConfiguredBy(rowJSON)

  def addParameter(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addParameter(rowJSON)

  def addConfigFile(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addConfigFile(rowJSON)

  def addParameterDescription(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addParameterDescription(rowJSON)

  def addConfigFileDescription(self, row):

    res = row.toJSON()
    rowJSON = res['Value']
    rpcClient = self._getRPC()
    print rowJSON
    return rpcClient.addConfigFileDescription(rowJSON)

  def getAgents(self):

    rpcClient = self._getRPC()
    return rpcClient.getAgents()

  def getAgentKey(self, agent_id):

    rpcClient = self._getRPC()
    return rpcClient.getAgentKey(agent_id)

  def getUsageDescription(self, activityDescription_id, role):

      rpcClient = self._getRPC()
      return rpcClient.getUsageDescription(activityDescription_id, role)

  def getParameterDescription(self, activityDescription_id, parameter_name):

      rpcClient = self._getRPC()
      return rpcClient.getParameterDescription(activityDescription_id, parameter_name)

  def getConfigFileDescription(self, activityDescription_id, configFile_name):

      rpcClient = self._getRPC()
      return rpcClient.getConfigFileDescription(activityDescription_id, configFile_name)

  def getActvityDescriptionKey(self, activityDescription_name, activityDescription_version):

      rpcClient = self._getRPC()
      return rpcClient.getActivityDescriptionKey(activityDescription_name, activityDescription_version)
