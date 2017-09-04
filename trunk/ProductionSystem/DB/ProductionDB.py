""" DIRAC Production DB

    Production database is used to collect and serve the necessary information
    in order to automate the task of transformation preparation for high level productions.
"""

from DIRAC                                                import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                   import DB

__RCSID__ = "$Id$"

#############################################################################

class ProductionDB( DB ):
  """ ProductionDB class
  """

  def __init__( self, dbname = None, dbconfig = None, dbIn = None ):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """
    pass