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

  def _getSEList( self, SEType = 'ProductionOutputs', DataType = 'SimtelProd' ):
    """ get from CS the list of available SE for data upload
    """
    opsHelper = Operations()
    optionName = os.path.join( SEType, DataType )
    SEList = opsHelper.getValue( optionName , [] )
    SEList = List.randomize( SEList )
    DIRAC.gLogger.notice( 'List of %s SE: %s ' % ( SEType, SEList ) )

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
        run_number is taken from filemetadata
        filemetadata can be a dict or the run_number itself
    """
    fmd = json.loads( filemetadata )
    if type( fmd ) == type( dict() ):
      run_number = int( fmd['runNumber'] )
    else:
      run_number = int( filemetadata )
    run_numberMod = run_number % 1000
    runpath = '%03dxxx' % ( ( run_number - run_numberMod ) / 1000 )
    return runpath

  def _formatPath( self, path ):
    """ format path to string with 1 digit precision
    """
    if type( path ) == float or type( path ) == int:
      path = '%.1f' % path
    return str( path )

  def _getInputData( self, run_number ):
    """ get InputData
    """
    # ## Get InputData
    if os.environ.has_key( 'JOBID' ):
      jobID = os.environ['JOBID']
      dirac = Dirac()
      res = dirac.getJobJDL( jobID )
      InputData = res['Value']['InputData']
    print 'InputData' % InputData
    for lfn in InputData:
      if run_number in lfn:
        return lfn

  def _setInputDataAsProcessed( self, run_number ):
    """ mark inputdata as 'processed'
    """
    lfn = self._getInputData( run_number )
    res = self.fcc.setMetadata( lfn, {'processed':'True'} )
    if not res['OK']:
      return res

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

  def putAndRegister( self, lfn, localfile, filemetadata, DataType = 'SimtelProd' ):
    """ put and register one file and set metadata
    """
    # ## Get the list of Production SE
    res = self._getSEList( 'ProductionOutputs', DataType )
    if res['OK']:
      ProductionSEList = res['Value']
    else:
      return res

    # ## Get the list of Failover SE
    res = self._getSEList( 'ProductionOutputsFailover', DataType )
    if res['OK']:
      FailoverSEList = res['Value']
    else:
      return res

    # ## Upload file to a Production SE
    res = self._putAndRegisterToSEList( lfn, localfile, ProductionSEList )
    if not res['OK']:
      DIRAC.gLogger.error( 'Failed to upload file to any Production SE: %s' % ProductionSEList )
      # ## Upload file to a Failover SE
      res = self._putAndRegisterToSEList( lfn, localfile, FailoverSEList )
      if not res['OK']:
        return DIRAC.S_ERROR( 'Failed to upload file to any Failover SE: %s' % FailoverSEList )

    # ## Set file metadata: jobID, subarray, sct
    if res['OK']:
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
      # ## Check added on sct to handle evndisp output file
      p = re.compile( 'sct' )
      if p.search( filename ) != None:
        fmd.update( {'sct':sct} )
      res = self.fcc.setMetadata( lfn, fmd )
      if not res['OK']:
        return res

      return DIRAC.S_OK()

  def _putAndRegisterToSEList( self, lfn, localfile, SEList ):
    """ put and register one file to one SE in the SEList
    """
    # ## Try to upload file to a SE in the list
    for SE in SEList:
      msg = 'Try to upload local file: %s \nwith LFN: %s \nto %s' % ( localfile, lfn, SE )
      DIRAC.gLogger.notice( msg )
      res = self.dm.putAndRegister( lfn, localfile, SE )
      # ##  check if failed
      if not res['OK']:
        DIRAC.gLogger.error( 'Failed to putAndRegister %s \nto %s \nwith message: %s' % ( lfn, SE, res['Message'] ) )
        continue
      elif res['Value']['Failed'].has_key( lfn ):
        DIRAC.gLogger.error( 'Failed to putAndRegister %s to %s' % ( lfn, SE ) )
        continue
      else:
        return DIRAC.S_OK()

    return DIRAC.S_ERROR()

  def cleanLocalFiles ( self, datadir, pattern = '*' ):
    """ remove files matching pattern in datadir
    """

    for localfile in glob.glob( os.path.join( datadir, pattern ) ):
      DIRAC.gLogger.notice( 'Removing local file: ', localfile )
      os.remove( localfile )

    return DIRAC.S_OK()

    
