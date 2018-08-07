"""
  Simple Wrapper on the Job class to handle Simtel Analysis

  https://forge.in2p3.fr/issues/33932
                        June 27th 2018 - J. Bregeon, C. Bigongiari
"""

__RCSID__ = "$Id$"

# generic imports
import os, json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

class SimtelTSJob(Job):
    """ Job extension class for Simtel Analysis

    """
    def __init__(self, cpuTime=432000):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Job.__init__(self)
        self.setCPUTime(cpuTime)
        # defaults
        self.setName('Simtel')
        self.package='corsika_simhessarray'
        self.program_category = 'tel_sim'
        self.version = '2018-06-12'
        self.configuration_id = 4
        self.output_data_level = 0
        self.base_path = '/vo.cta.in2p3.fr/user/c/ciro.bigongiari/Miniarray15/Simtel'
        self.fcc = FileCatalogClient()
        self.metadata = collections.OrderedDict()
        self.filemetadata = {}
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])
        self.ts_task_id = 1000
        self.simtel_config_file = 'ASTRI_MiniArray15_Paranal_ACDC_2018_06_12.cfg'
        self.thetaP = 20.0
        self.phiP = 0.0
        self.particle = 'Proton'
        self.output_path = os.path.join(self.base_path, self.particle, str(self.phiP))
        self.se_list = ["FRASCATI-USER", "CNAF-USER"]

    def get_output_path(self):
        """ Recompute the output path where to upload data
        """
        return os.path.join(self.base_path, self.particle, str(self.phiP))

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called.
        """
        # step 1 -- debug
        iStep = 1
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr',
                                        logFile='LS_Init_Log.txt')
            lsStep['Value']['name'] = 'Step%i_LS_Init'%iStep
            lsStep['Value']['descr_short'] = 'list files in working directory'
            iStep+=1

        # step 2
        swStep = self.setExecutable('cta-prod3-setupsw',
                                  arguments='%s %s'% (self.package,
                                                      self.version),
                                  logFile='SetupSoftware_Log.txt')
        swStep['Value']['name'] = 'Step%i_SetupSoftware' % iStep
        swStep['Value']['descr_short'] = 'Setup software'
        iStep+=1

        # step 3 - running
        evStep = self.setExecutable('./dirac_simtel_zstd.sh',
                                    arguments = "%s TELESCOPE_THETA=%s \
                                    TELESCOPE_PHI=%s" %  #  --taskid %s" %
                                    (self.simtel_config_file, self.thetaP, self.phiP),
                                    logFile='Simtel_Log.txt')
        evStep['Value']['name'] = 'Step%i_Simtel' % iStep
        evStep['Value']['descr_short'] = 'Run Simtel'
        iStep += 1

        # step 4 - running
        # outputpattern = args[0]
        # outputpath = args[1]
        # SEList = json.loads( args[2] )
        # simtel.log.tar simtel.zst.tar
        output_pattern = "*.simtel.zst.tar"
        dmStep = self.setExecutable('cta-user-managedata',
                                    arguments = "%s %s \'%s\'" %
                                    (self.get_output_path(), output_pattern,
                                     self.se_list),
                                    logFile='Upload_output_Log.txt')
        evStep['Value']['name'] = 'DM_Step%i' % iStep
        evStep['Value']['descr_short'] = 'Upload output data'
        iStep += 1

        # step 5 -- to be removed -- debug only
        if debug:
            lsStep = self.setExecutable('/bin/ls -alhtr',
                                        logFile='LS_End_Log.txt')
            lsStep['Value']['name']='Step%i_LS_End'%iStep
            lsStep['Value']['descr_short']='list files in working directory and in Data directory'
            iStep+=1
