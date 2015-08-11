""" Manage data and meta-data for PROD3
"""

__RCSID__ = "$Id$"

# generic imports
import os, glob, json, tarfile, re, collections

# DIRAC imports
import DIRAC
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Interfaces.API.Dirac import Dirac

class Prod3DataManager(object) :
  """ Manage data and meta-data
  """
        
  def __init__(self,catalogs=['DIRACFileCatalog']):
    """ Constructor

    """
    self.setupCatalogClient( catalogs )
    self.printCatalogConfig( catalogs )
    self.setupDataManagerClient( catalogs )

  def setupCatalogClient( self, catalogs ):
    """ Init FileCatalog client
        Ideally we would like to use only FileCatalog but it doesn't work for setMetadata
        because the returned value has a wrong format. So use of FileCatalogClient instead
    """
    self.fc = FileCatalog( catalogs )
    self.fcc = FileCatalogClient()
    
  def printCatalogConfig( self, catalogs ):
    """ Dumps catalog configuration
    """
    for catalog in catalogs:
      res = self.fc._getCatalogConfigDetails( catalog )
      DIRAC.gLogger.info( 'CatalogConfigDetails:', res )

  def setupDataManagerClient( self, catalogs ):
    """ Init DataManager client
    """
    self.dm = DataManager( catalogs )

  def _getSEList( self ):
    """ get from CS the list of available SE for data upload
    """
    opsHelper = Operations()
    SEList = opsHelper.getValue( 'ProductionOutputs/SimtelProd', [] )
    SEList = List.randomize( SEList )
    DIRAC.gLogger.notice( 'List of available Storage Element is: ', SEList )

    # # Check if the local SE is in the list. If yes try it first by reversing list order
    localSEList = []
    res = getSEsForSite( DIRAC.siteName() )
    if res['OK']:
      localSEList = res['Value']

    retainedlocalSEList = []
    for localSE in localSEList:
      if localSE in SEList:
        DIRAC.gLogger.notice( 'The local Storage Element is an available SE: ', localSE )
        retainedlocalSEList.append( localSE )
        SEList.remove( localSE )

    SEList = retainedlocalSEList + SEList
    if len( SEList ) == 0:
      return DIRAC.S_ERROR( 'Error in building SEList' )

    return DIRAC.S_OK( SEList )

  def _checkemptydir( self, path ):
    """ check that the directory is not empty
    """
    if len ( glob.glob( path ) ) == 0:
      error = 'Empty directory: %s' % path
      return DIRAC.S_ERROR( error )
    else:
      return DIRAC.S_OK()

  def _getRunPath( self, filemetadata ):
    """ format path to string with 1 digit precision
    """
    fmd = json.loads( filemetadata )
    run_number = int( fmd['runNumber'] )
    run_numberMod = run_number % 1000
    runpath = '%03dxxx' % ( ( run_number - run_numberMod ) / 1000 )
    return runpath

  def _formatPath( self, path ):
    """ format path to string with 1 digit precision
    """
    if type( path ) == float or type( path ) == int:
      path = '%.1f' % path
    return str( path )

  def createTarLogFiles ( self, inputpath, tarname ):
    """ create tar of log and histogram files
    """
    tarmode = 'w:gz'
    tar = tarfile.open( tarname, tarmode )

    for subdir in ['Log/*', 'Histograms/*']:
      logdir = os.path.join( inputpath, subdir )

      res = self._checkemptydir( logdir )
      if not res['OK']:
        return res

      for localfile in glob.glob( logdir ):
        tar.add( localfile, arcname = localfile.split( '/' )[-1] )

    tar.close()

    return DIRAC.S_OK()

  def createMDStructure( self, metadata, metadatafield, basepath ):
    """ create meta data structure
    """
    # ## Add metadata fields to the DFC
    mdfield = json.loads( metadatafield )
    for key, value in mdfield.items():
      res = self.fc.addMetadataField( key, value )
      if not res['OK']:
        return res

    # ## Create the directory structure
    md = json.loads( metadata , object_pairs_hook = collections.OrderedDict )

    path = basepath
    for key, value in dict( ( k, md[k] ) for k in ( 'site', 'particle', 'process_program' ) if k in md ).items():
      path = os.path.join( path, self._formatPath( value ) )
      res = self.fc.createDirectory( path )
      if not res['OK']:
        return res

      # Set directory metadata for each subdir: 'site', 'particle', 'process_program'
      res = self.fcc.setMetadata( path, {key:value} )
      if not res['OK']:
        return res

    # Create the TransformationID subdir and set MD

    # ## Get the TransformationID
    if os.environ.has_key( 'JOBID' ):
      jobID = os.environ['JOBID']
      dirac = Dirac()
      res = dirac.getJobJDL( jobID )
      TransformationID = res['Value']['TransformationID']
    else:
      # ## This is used just when job runs locally or without TS
      TransformationID = 'TransformationID'

    path = os.path.join( path, TransformationID )
    res = self.fc.createDirectory( path )
    if not res['OK']:
      return res
    res = self.fcc.setMetadata( path, dict( ( k, md[k] ) for k in ( 'phiP', 'thetaP', 'array_layout' ) if k in md ) )
    if not res['OK']:
      return res

    # Create the Data and Log subdirs and set MD
    Transformation_path = path
    for subdir in ['Data', 'Log']:
      path = os.path.join( Transformation_path, subdir )
      res = self.fc.createDirectory( path )
      if not res['OK']:
        return res
      res = self.fcc.setMetadata( path, {'outputType':subdir} )
      if not res['OK']:
        return res

    return DIRAC.S_OK( Transformation_path )

  def putAndRegister( self, lfn, localfile, filemetadata ):
    """ put and register one file and set metadata
    """
    # ## Get the list of available SEs
    res = self._getSEList()
    if res['OK']:
      SEList = res['Value']
    else:
      return res

    # ## Try to upload file to a SE in the list
    for SE in SEList:
      msg = 'Try to upload local file: %s \nwith LFN: %s \nto %s' % ( localfile, lfn, SE )
      DIRAC.gLogger.notice( msg )
      res = self.dm.putAndRegister( lfn, localfile, SE )
      # ##  check if failed
      if not res['OK']:
        error = 'Failed to putAndRegister %s \nto %s \nwith message: %s' % ( lfn, SE, res['Message'] )
        DIRAC.gLogger.error( error )
        continue
      elif res['Value']['Failed'].has_key( lfn ):
        error = 'Failed to putAndRegister %s to %s' % ( lfn, SE )
        DIRAC.gLogger.error( error )
        continue
      else:
        # ## Set file metadata: jobID, subarray, sct
        fmd = json.loads( filemetadata )
        if os.environ.has_key( 'JOBID' ):
          fmd.update( {'jobID':os.environ['JOBID']} )
        filename = os.path.basename( localfile )
        # set subarray and sct md from filename
        p = re.compile( 'subarray-\d+' )
        if p.search( filename ) != None:
          subarray = p.search( filename ).group()
          fmd.update( {'subarray':subarray} )
        sct = 'False'
        p = re.compile( 'nosct' )
        if p.search( filename ) == None:
          sct = 'True'
        fmd.update( {'sct':sct} )
        res = self.fcc.setMetadata( lfn, fmd )
        if not res['OK']:
          return res

        return DIRAC.S_OK()

    error = 'Failed to upload file to any SE:  %s' % SEList
    return DIRAC.S_ERROR( error )

  def cleanLocalFiles ( self, datadir, pattern ):
    """ remove files matching pattern in datadir
    """

    for localfile in glob.glob( os.path.join( datadir, pattern ) ):
      DIRAC.gLogger.notice( 'Removing local file: ', localfile )
      os.remove( localfile )

    return DIRAC.S_OK()

    
