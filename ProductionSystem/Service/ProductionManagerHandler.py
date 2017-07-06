""" DISET request handler base class for the TransformationDB.
"""

from DIRAC                                               import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                     import RequestHandler
from DIRAC.ProductionSystem.DB.ProductionDB      import ProductionDB
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

transTypes = [basestring, int, long]

__RCSID__ = "$Id$"

database = False
def initializeProductionManagerHandler( serviceInfo ):
  global database
  database = ProductionDB( 'ProductionDB', 'Production/ProductionDB' )
  return S_OK()

class ProductionManagerHandler( RequestHandler ):

  ####################################################################
  #
  # These are the methods to manipulate the Productions table
  #


  ####################################################################
  #
  # These are the methods to manipulate the ProductionTransformations table
  #

  ####################################################################
  #
  # These are the methods for production logging manipulation
  #

  ####################################################################
  #
  # These are the methods used for web monitoring
  #

  pass
