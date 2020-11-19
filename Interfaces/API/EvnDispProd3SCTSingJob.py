"""
  Simple Wrapper on the Job class to handle EvnDisp Analysis
  for the Prod3 SCT 2020 analysis from container
  https://forge.in2p3.fr/issues/41709

  November 16th 2020 - J. Bregeon
                  bregeon@in2p3.fr
"""

__RCSID__ = "$Id$"

# generic imports
import json
import collections
# DIRAC imports
import DIRAC
from DIRAC.Interfaces.API.Job import Job


class EvnDispProd3SCTSingJob(Job):
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
        self.setName('Evndisplay_CalibReco')
        self.package = 'evndisplay'
        self.version = 'eventdisplay-cta-dl1-prod3b-sct.v02'
        self.container = True
        self.program_category = 'calibimgreco'
        self.prog_name = 'evndisp'
        self.configuration_id = 9
        self.output_data_level = 1
        self.base_path = '/vo.cta.in2p3.fr/MC/PROD3/'
        self.metadata = collections.OrderedDict()
        self.file_meta_data = dict()
        self.catalogs = json.dumps(['DIRACFileCatalog', 'TSCatalog'])
        self.ts_task_id = 0

    def set_meta_data(self, tel_sim_md):
        """ Set EventDisplay meta data

        Parameters:
        tel_sim_md -- metadata dictionary from the telescope simulation
        """
        # # Set evndisp directory meta data
        self.metadata['array_layout'] = tel_sim_md['array_layout']
        self.metadata['site'] = tel_sim_md['site']
        self.metadata['particle'] = tel_sim_md['particle']
        try:
            phiP = tel_sim_md['phiP']['=']
        except TypeError:
            phiP = tel_sim_md['phiP']
        self.metadata['phiP'] = phiP
        try:
            thetaP = tel_sim_md['thetaP']['=']
        except TypeError:
            thetaP = tel_sim_md['thetaP']
        self.metadata['thetaP'] = thetaP
        self.metadata[self.program_category+'_prog'] = self.prog_name
        self.metadata[self.program_category+'_prog_version'] = self.version
        self.metadata['data_level'] = self.output_data_level
        self.metadata['configuration_id'] = self.configuration_id

    def set_file_meta_data(self, nsb=1, sct="True"):
        """ Set evndisplay file meta data

        Parameters:
        meta_data_dict -- metadata dictionary
        """
        # Set evndisp file meta data
        self.file_meta_data['nsb'] = nsb
        self.file_meta_data['sct'] = sct

    def setupWorkflow(self, debug=False):
        """ Setup job workflow by defining the sequence of all executables
            All parameters shall have been defined before that method is called.
        """
        i_step = 0
        # step 1 -- debug
        if debug:
            ls_step = self.setExecutable('/bin/ls -alhtr', logFile='LS_Init_Log.txt')
            ls_step['Value']['name'] = 'Step%i_LS_Init' % i_step
            ls_step['Value']['descr_short'] = 'list files in working directory'
            i_step += 1

            env_step = self.setExecutable('/bin/env', logFile='Env_Log.txt')
            env_step['Value']['name'] = 'Step%i_Env' % i_step
            env_step['Value']['descr_short'] = 'Dump environment'
            i_step += 1

        # # step 2
        # sw_step = self.setExecutable('cta-prod-setup-software',
        #                           arguments='-p %s -v %s -a simulations -g %s'%
        #                           (self.package, self.version, self.compiler),\
        #                           logFile='SetupSoftware_Log.txt')
        # sw_step['Value']['name'] = 'Step%i_SetupSoftware' % i_step
        # sw_step['Value']['descr_short'] = 'Setup software'
        # i_step+=1

        # step 3 verify input data size
        # arguments are nbFiles=0 (not used) and fileSize=1000kB
        eiv_step = self.setExecutable('cta-prod3-verifysteps',
                            arguments="generic 1 1000 '*.simtel.gz'",
                            logFile='Verify_EvnDispInputs_Log.txt')
        eiv_step['Value']['name'] = 'Step%i_VerifyEvnDispInputs' % i_step
        eiv_step['Value']['descr_short'] = 'Verify EvnDisp Inputs'
        i_step += 1

        # step 4 run EventDisplay
        ev_step = self.setExecutable('singularity',
                         arguments='run /cvmfs/cta.in2p3.fr/software/containers/%s \
                                    /evndisp/dirac_sing_evndisp.sh' %
                                    (self.version),
                         logFile='EvnDisp_Log.txt')
        ev_step['Value']['name'] = 'Step%i_EvnDisplay' % i_step
        ev_step['Value']['descr_short'] = 'Run EvnDisplay'
        i_step += 1

        # step 5 set meta data and register Data
        meta_data_json = json.dumps(self.metadata)
        file_meta_data_json = json.dumps(self.file_meta_data)

        meta_data_field = {'array_layout': 'VARCHAR(128)', 'site': 'VARCHAR(128)',
                           'particle': 'VARCHAR(128)',
                           'phiP': 'float', 'thetaP': 'float',
                           self.program_category+'_prog': 'VARCHAR(128)',
		                   self.program_category+'_prog_version': 'VARCHAR(128)',
                           'data_level': 'int', 'configuration_id': 'int'}
        meta_data_field_json = json.dumps(meta_data_field)

        # register Data
        data_output_pattern = './Data/*.simtel.gz.DL1.root'
        scripts = '../CTADIRAC/Core/scripts/'
        dm_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                     arguments="'%s' '%s' '%s' %s '%s' %s %s '%s' Data" %
                                     (meta_data_json, meta_data_field_json,
                                      file_meta_data_json,
                                      self.base_path, data_output_pattern, self.package,
                                      self.program_category, self.catalogs),
                                     logFile='DataManagement_Log.txt')
        dm_step['Value']['name'] = 'Step%s_DataManagement' % i_step
        dm_step['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
        i_step += 1

        # register Data lin
        data_output_pattern = './Data/*.simtel.gz.lin.DL1.root'
        dm2_step = self.setExecutable(scripts + 'cta-prod-managedata.py',
                                     arguments="'%s' '%s' '%s' %s '%s' %s %s '%s' Data" %
                                     (meta_data_json, meta_data_field_json,
                                      file_meta_data_json,
                                      self.base_path, data_output_pattern, self.package,
                                      self.program_category, self.catalogs),
                                     logFile='DataManagement2_Log.txt')
        dm2_step['Value']['name'] = 'Step%s_DataManagement' % i_step
        dm2_step['Value']['descr_short'] = 'Save data files to SE and register them in DFC'
        i_step += 1

        # step 7 failover step
        if not debug:
            failover_step = self.setExecutable('/bin/ls -l',
                                               modulesList=['Script', 'FailoverRequest'])
            failover_step['Value']['name'] = 'Step%s_Failover' % i_step
            failover_step['Value']['descr_short'] = 'Tag files as unused if job failed'
            i_step += 1

        # Step 8 - debug only
        if debug:
            ls_step = self.setExecutable('/bin/ls -Ralhtr', logFile='LS_End_Log.txt')
            ls_step['Value']['name'] = 'Step%s_LSHOME_End' % i_step
            ls_step['Value']['descr_short'] = 'list files in Home directory'
            i_step += 1
