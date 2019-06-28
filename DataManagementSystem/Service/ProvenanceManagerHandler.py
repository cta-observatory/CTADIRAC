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
  def initializeHandler(cls, serviceInfoDict):
    """ initialize handler """

    try:
      cls.__provenanceDB = ProvenanceDB()
    except RuntimeError as error:
      gLogger.exception(error)
      return S_ERROR(error)

    return S_OK()
     # # create tables for empty db
    #return cls.__provenanceDB.createTables()
