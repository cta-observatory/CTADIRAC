""" Class that contains client access to the transformation DB handler. """

__RCSID__ = "$Id$"

from DIRAC                                                         import S_OK, gLogger
from DIRAC.Core.Base.Client                                        import Client
from DIRAC.ConfigurationSystem.Client.Helpers.Operations           import Operations

class ProductionClient( Client ):


  """ Exposes the functionality available in the DIRAC/TransformationHandler
  """
  def __init__( self, **kwargs ):
    """ Simple constructor
    """
    Client.__init__( self, **kwargs )
    self.setServer( 'Production/ProductionManager' )

  def setServer( self, url ):
    self.serverURL = url

  def setName (self, prodName ):
    """
          set the name of the production
    """
    pass

  def addTransformation( self, transformation ):
    """
          add a transformation to the production
    """
    pass

  def addProduction( self ):
    """
          add a production to the system
    """
    pass

  def getProductions( self, condDict = None, older = None, newer = None, timeStamp = None,
                          orderAttribute = None, limit = 100, extraParams = False ):
    """ gets all the productions in the system, incrementally. "limit" here is just used to determine the offset.
        for now args are taken from the analogue method 'getTransformations' of the TS
    """
    pass

  def getProduction( self, prodID ):
    """ gets a specific production.
    """
    pass

  def cleanProduction( self, prodID ):
    """ clean the production, and set the status parameter
    """
    pass

  def deleteProduction( self, prodID ):
    """ delete the production from the system
    """
    pass

  def setStatus( self, status ):
    """ set the production status
    """
    pass

  def getProductionTransformations( self, condDict = None, older = None, newer = None, timeStamp = None,
                              orderAttribute = None, limit = 10000, inputVector = False ):
    """ gets all the production transformations for a production, incrementally.
        "limit" here is just used to determine the offset.
    """
    pass






