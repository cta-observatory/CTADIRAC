""" DISET request handler base class for the ProvenanceDB
"""

__RCSID__ = "$Id$"
## imports
import json

## from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler

## from CTADIRAC
from CTADIRAC.DataManagementSystem.DB.ProvenanceDB import ProvenanceDB

class ProvenanceManagerHandler(RequestHandler):

  """
  .. class:: ProvenanceManagerHandler
  ProvenanceDB interface in the DISET framework.
  """
  __provenanceDB = None

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """ initialize handler """

    try:
      cls.__provenanceDB = ProvenanceDB()
    except RuntimeError as error:
      gLogger.exception(error)
      return S_ERROR(error)

    return S_OK()

  def _parseRes(cls, res):
    if not res['OK']:
      gLogger.error('ProvenanceManager failure', res['Message'])
    return res


  types_addActivityDescription = [basestring]

  def export_addActivityDescription(cls, rowJSON):
    '''
    #Insert ActivityDescription
    #:param rowSON
    #:return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addActivityDescription(rowDict)
    return cls._parseRes(res)

  types_addActivity= [basestring]

  def export_addActivity(cls, rowJSON):
    '''
    #Insert ActivityDescription
    #:param rowJSON
    #:return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addActivity(rowDict)
    return cls._parseRes(res)

  types_addWasAssociatedWith= [basestring]

  def export_addWasAssociatedWith(cls, rowJSON):
    '''
    #Insert WasAssociatedWith
    #:param rowJSON
    #:return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addWasAssociatedWith(rowDict)
    return cls._parseRes(res)

  types_addAgent = [basestring]

  def export_addAgent(cls, rowJSON):
    '''
    Insert Agent
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addAgent(rowDict)
    return cls._parseRes(res)

  types_addDatasetDescription = [basestring]

  def export_addDatasetDescription(cls, rowJSON):
    '''
    Insert DatasetDescription
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addDatasetDescription(rowDict)
    return cls._parseRes(res)

  types_addUsageDescription = [basestring]

  def export_addUsageDescription(cls, rowJSON):
    '''
    Insert UsageDescription
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addUsageDescription(rowDict)
    return cls._parseRes(res)

  types_addGenerationDescription = [basestring]

  def export_addGenerationDescription(cls, rowJSON):
    '''
    Insert UsageDescription
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addGenerationDescription(rowDict)
    return cls._parseRes(res)

  types_addValueDescription = [basestring]

  def export_addValueDescription(cls, rowJSON):
    '''
    Insert UsageDescription
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addValueDescription(rowDict)
    return cls._parseRes(res)

  types_addDatasetEntity = [basestring]

  def export_addDatasetEntity(cls, rowJSON):
    '''
    Insert DatasetEntity
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addDatasetEntity(rowDict)
    return cls._parseRes(res)

  types_addWasAttributedTo = [basestring]

  def export_addWasAttributedTo(cls, rowJSON):
    '''
    Insert WasAttributedTo
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addWasAttributedTo(rowDict)
    return cls._parseRes(res)

  types_addUsed = [basestring]

  def export_addUsed(cls, rowJSON):
    '''
    Insert Used
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addUsed(rowDict)
    return cls._parseRes(res)

  types_addWasGeneratedBy = [basestring]

  def export_addWasGeneratedBy(cls, rowJSON):
    '''
    Insert WasGeneratedBy
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addWasGeneratedBy(rowDict)
    return cls._parseRes(res)

  types_addValueEntity = [basestring]

  def export_addValueEntity(cls, rowJSON):
    '''
    Insert ValueEntity
    :param rowJSON
    :return:
    '''

    rowDict = json.loads(rowJSON)
    res = cls.__provenanceDB.addValueEntity(rowDict)
    return cls._parseRes(res)

  types_getAgents = []

  def export_getAgents(cls):
    '''
    Get Agents
    :return:
    '''

    res = cls.__provenanceDB.getAgents()
    return cls._parseRes(res)

  types_getDatasetEntity = [basestring]

  def export_getDatasetEntity(cls, guid):
    '''
    Get DatasetEntity
    :param datasetEntity guid
    :return:
    '''

    res = cls.__provenanceDB.getDatasetEntity(guid)
    return cls._parseRes(res)

  types_getUsageDescription = []

  def export_getUsageDescription(cls, activityDescription_id, role):
    '''
    Get UsageDescription
    :param activity_id, role
    :return usageDescription_id
    '''

    res = cls.__provenanceDB.getUsageDescription(activityDescription_id, role)
    return cls._parseRes(res)

  types_getGenerationDescription = []

  def export_getGenerationDescription(cls, activityDescription_id, role):
    '''
    Get GenerationDescription
    :param activity_id, role
    :return GenerationDescription_id
    '''

    res = cls.__provenanceDB.getGenerationDescription(activityDescription_id, role)
    return cls._parseRes(res)




