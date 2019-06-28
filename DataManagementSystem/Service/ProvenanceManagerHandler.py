""" DISET request handler base class for the ProvenanceDB
"""

__RCSID__ = "$Id$"
## imports


## from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler

## from DMS
from CTADIRAC.DataManagementSystem.DB.ProvenanceDB import ProvenanceDB


class ProvenanceManagerHandler(RequestHandler):

  """
  .. class:: ProvenanceManagerHandler
  ProvenanceDB interface in the DISET framework.
  """
  __provenanceDB = None

  @classmethod
  def initializeHandler(cls):
    """ initialize handler """

    try:
      cls.__provenanceDB = ProvenanceDB()
    except RuntimeError as error:
      gLogger.exception(error)
      return S_ERROR(error)

    return S_OK()

  def _parseRes(self, res):
    if not res['OK']:
      gLogger.error('ProvenanceManager failure', res['Message'])
    return res

  #types_insert = [basestring]

  '''def export_insert(cls, row):
    result = cls.__provenanceDB.insert(row)
    if not result["OK"]:
      return result

    return S_OK()'''

  types_addAgent = [dict]

  def export_addAgent(cls, agentDict):
    '''
    Insert Agent
    :param agentDict
    :return:
    '''
    if isinstance(agentDict, dict):
      res = cls.__provenanceDB.addAgent(agentDict)

    return cls._parseRes(res)

  types_getAgents = []

  def export_getAgents(cls):
    '''
    Insert Agent
    :param agentDict
    :return:
    '''

    res = cls.__provenanceDB.getAgents()
    return cls._parseRes(res)