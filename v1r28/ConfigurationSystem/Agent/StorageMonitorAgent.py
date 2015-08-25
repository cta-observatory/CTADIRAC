#############################################################################
# $HeadURL$
#############################################################################

""" The StorageMonitorAgent checks the storage space usage
    for SEs defined in the Agent section
"""

__RCSID__ = "$Id$"

from DIRAC                                              import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                        import AgentModule
from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient
from DIRAC.ConfigurationSystem.Client.CSAPI             import CSAPI
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.PrettyPrint import printTable

class StorageMonitorAgent( AgentModule ):

  addressTo = ''
  addressFrom = ''
  voName = ''
  bdii = 'lcg-bdii.cern.ch:2170'
  productionSEs = []
  subject = "StorageMonitorAgent"
 
  def initialize( self ):

    self.addressTo = self.am_getOption( 'MailTo', self.addressTo )
    self.addressFrom = self.am_getOption( 'MailFrom', self.addressFrom )

    if self.addressTo and self.addressFrom:
      self.log.info( "MailTo", self.addressTo )
      self.log.info( "MailFrom", self.addressFrom )

    self.productionSEs = self.am_getOption( 'ProductionSEs', self.productionSEs)
    if self.productionSEs:
      self.log.info( "ProductionSEs", self.productionSEs )
    else:
      self.log.fatal( "ProductionSEs option not defined for agent" )
      return S_ERROR()

    self.voName = self.am_getOption( 'VirtualOrganization', self.voName )
    if self.voName:
      self.log.info( "Agent will manage VO", self.voName )
    else:
      self.log.fatal( "VirtualOrganization option not defined for agent" )
      return S_ERROR()

    self.bdii = self.am_getOption( 'Bdii', self.bdii )
    if self.bdii:
      self.log.info( "Bdii", self.bdii )

    self.csAPI = CSAPI()
    return self.csAPI.initialize()

  def execute( self ):
    """ General agent execution method
    """
    # Get a "fresh" copy of the CS data
    result = self.csAPI.downloadCSData()
    if not result['OK']:
      self.log.warn( "Could not download a fresh copy of the CS data", result[ 'Message' ] )

    # Execute command to retrieve storage usage information
    cmdTuple = ['lcg-infosites', '--vo', self.voName , 'se']
    self.log.info( "Executing %s" % cmdTuple )
    ret = systemCall( 0, cmdTuple, env = {'LCG_GFAL_INFOSYS':self.bdii} )

    if not ret['OK']:
      return ret
    elif not ret['Value'][1] != '':
      self.log.error( "Error while executing %s" % cmdTuple )
      return S_ERROR()

    sedict = {}
    #### Parse the output of the command 
    for se in ret['Value'][1].split( '\n' ):
      if len( se.split() ) == 4:
        spacedict = {}
        SE = se.split()[3]
        if se.split()[0] != 'n.a' and se.split()[1] != 'n.a':
          # ## convert into TB
          available = float( se.split()[0] ) / 1e9
          used = float( se.split()[1] ) / 1e9
          spacedict['Available'] = available
          spacedict['Used'] = used
          spacedict['Total'] = available + used
          sedict[SE] = spacedict
    
    fields = ['SE', 'Available(TB)', 'Used(TB)', 'Total(TB)', 'Used(%)' ]
    records = []
    fullSEList = []

    for SE in self.productionSEs:
      available = '%.1f' % sedict[SE]['Available']
      used = '%.1f' % sedict[SE]['Used']
      total = '%.1f' % sedict[SE]['Total']
      fraction_used = sedict[SE]['Used'] / sedict[SE]['Total'] * 100
      if fraction_used > 90.:
        fullSEList.append(SE)
        self.log.warn( "%s full at %.1f%%" % (SE, fraction_used) )
      fraction_used = '%.1f' % fraction_used
      records.append( [SE, available, used, total, fraction_used] )

    body = printTable( fields, records, printOut = False )

    if len(fullSEList) > 0:
      self.subject = 'CRITICAL storage usage beyond 90%%: %s' % ( ', '.join( fullSEList ) )

    if self.addressTo and self.addressFrom:
      notification = NotificationClient()
      result = notification.sendMail( self.addressTo, self.subject, body, self.addressFrom, localAttempt = False )
      if not result['OK']:
        self.log.error( 'Can not send  notification mail', result['Message'] )

    return S_OK()
