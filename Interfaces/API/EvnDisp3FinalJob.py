"""
  Simple Wrapper on the Job class to handle EvnDisp Analysis
  for the final Prod3b pass
  https://forge.in2p3.fr/issues/27528
                        February 8th 2018 - J. Bregeon
"""

__RCSID__ = "$Id$"

# generic imports
import os, json, collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

class EvnDisp3FinalJob(Job):
    """ Job extension class for EvnDisp Analysis,
      takes care of running converter and evndisp.
    """

    def __init__(self, cpuTime=432000):
        """ Constructor

        Keyword arguments:
        cpuTime -- max cpu time allowed for the job
        """
        Job.__init__(self)
        self.setCPUTime(cpuTime)
        # defaults
        self.setName('Evndisplay_Analysis')
        self.package='evndisplay'
        self.program_category = 'analysis' #'calibimgreco'
        self.version = 'prod3b_d20180201'
        self.prefix = 'CTA.prod3S'
        self.layout_list = '3HB9-FD 3HB9-FG 3HB9-FA 3HB9-ND 3HB9-NG 3HB9-NA \
                            3HB9-TS-BB-FD 3HB9-TS-BB-FG 3HB9-TS-BB-FA \
                            3HB9-TS-BB-ND 3HB9-TS-BB-NG 3HB9-TS-BB-NA'
        self.focal_file = 'CTA.prod3b.EffectiveFocalLength.dat'
        self.calibration_file = 'prod3b.Paranal-20171214.ped.root'
        self.reconstructionparameter = 'EVNDISP.prod3.reconstruction.runparameter.NN.noLL'
        self.NNcleaninginputcard = 'EVNDISP.NNcleaning.dat'
        self.basepath = '/vo.cta.in2p3.fr/MC/PROD3/'
        self.outputpattern = './*evndisp.tar.gz'
        self.fcc = FileCatalogClient()
        self.metadata = collections.OrderedDict()
        self.filemetadata = {}
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])
        self.jobGroupID = 1

    def setEvnDispMD(self, path):
        """ Set evndisplay meta data starting from path metadata

        Parameters:
        path -- path from which get meta data
        """
        # # Get simtel meta data from path
        res = self.fcc.getFileUserMetadata(path)
        simtelMD = res['Value']

        # # Set evndisp directory meta data
        self.metadata['array_layout'] = simtelMD['array_layout']
        self.metadata['site'] = simtelMD['site']
        self.metadata['particle'] = simtelMD['particle']
        self.metadata['phiP'] = simtelMD['phiP']
        self.metadata['thetaP'] = simtelMD['thetaP']
        # self.metadata['process_program'] = 'evndisp' + '_' + self.version
        self.metadata['analysis_prog'] = 'evndisp'
        self.metadata['analysis_prog_version'] = self.version
        self.metadata['data_level'] = 1
        self.metadata['configuration_id'] = -1
        # here hardcode that we have no SCTs
        self.metadata['sct'] = 'False'

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

        # step 2bis
        # arguments are nbFiles=0 (not used) and fileSize=100kB
        eivStep = self.setExecutable('cta-prod3-verifysteps',
                                  arguments='analysisinputs 0 100',
                                  logFile='Verify_EvnDispInputs_Log.txt')
        eivStep['Value']['name'] = 'Step%i_VerifyEvnDispInputs' % iStep
        eivStep['Value']['descr_short'] = 'Verify EvnDisp Inputs'
        iStep += 1

        evStep = self.setExecutable('./dirac_prod3_evndisp',
                                    arguments = "--prefix %s --layout_list '%s' \
                                    --focal_file %s --calibration_file %s \
                                    --reconstructionparameter %s" % 
                                    (self.prefix, self.layout_list,
                                     self.focal_file, self.calibration_file,
                                     self.reconstructionparameter),
                                    logFile='EvnDisp_Log.txt')
        evStep['Value']['name'] = 'Step%i_EvnDisplay' % iStep
        evStep['Value']['descr_short'] = 'Run EvnDisplay'
        iStep += 1

        # step 4
        # ## the order of the metadata dictionary is important, since it's used to build the directory structure
        mdjson = json.dumps(self.metadata)

        metadatafield = {'array_layout':'VARCHAR(128)', 'site':'VARCHAR(128)',
                         'particle':'VARCHAR(128)',
                         'phiP':'float', 'thetaP': 'float',
                         'analysis_prog':'VARCHAR(128)',
                         'analysis_prog_version':'VARCHAR(128)'}

        mdfieldjson = json.dumps(metadatafield)
        file_md_json = json.dumps(self.filemetadata)
        scripts = '../CTADIRAC/Core/scripts'
        dmStep = self.setExecutable(scripts + '/cta-analysis-managedata.py',
                            arguments = "'%s' '%s' '%s' %s '%s' %s %s '%s'" %
                            (mdjson, mdfieldjson, file_md_json, self.basepath,
                            self.outputpattern, self.package,
                            self.program_category, self.catalogs),
                            logFile='DataManagement_Log.txt')
        dmStep['Value']['name'] = 'Step%i_DataManagement' % iStep
        dmStep['Value']['descr_short'] = 'Save files to SE and register them in DFC'
        iStep += 1
