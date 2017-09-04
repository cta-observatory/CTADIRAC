# This file should be sourced from each example

# The example should work (on Linux) without the following environment variables:
unset HESSROOT
export HESSROOT
unset LD_LIBRARY_PATH
export LD_LIBRARY_PATH

# Top-level path under which we normally installed everything
if [ ! -z "${CTA_PATH}" ]; then
   cd "${CTA_PATH}" || exit 1
fi

# This preparation step is actually just needed once 
# (and it requires write permissions to various directories).
# Since now done at build time, we could actually skip it here.
# For the time being we keep it to check that things are in place.
#./prepare_for_examples || exit 1
###JCT ./prepare_for_examples $1 || exit 1 

#if [ ! -x corsika-run/corsika ]; then
#   echo "No CORSIKA program found."
#   exit 1
#fi

#if [ ! -x sim_telarray/bin/sim_telarray ]; then
if [ ! -x $1/bin/sim_telarray ]; then   
   echo "No sim_telarray program found."
   exit 1
fi

# Top-level path under which we normally installed everything
if [ -z "${CTA_PATH}" ]; then
   export CTA_PATH="$(pwd -P)"
fi

# Paths to software, libraries, configuration files (read-only)
#export CORSIKA_PATH="$(cd ${CTA_PATH}/corsika-run && pwd -P)"
#export SIM_TELARRAY_PATH="$(cd ${CTA_PATH}/sim_telarray && pwd -P)"
export SIM_TELARRAY_PATH="$(cd ${CTA_PATH}/$1 && pwd -P)" 
export HESSIO_PATH="$(cd ${CTA_PATH}/$1 && pwd -P)" 
export LD_LIBRARY_PATH="${HESSIO_PATH}/lib"
export PATH="${HESSIO_PATH}/bin:${SIM_TELARRAY_PATH}/bin:${PATH}"

# Sim_telarray configuration paths are normally compiled-in but you can set
#   SIM_TELARRAY_CONFIG_PATH : replace compiled-in paths with this
#   SIMTEL_CONFIG_PATH : precede compiled-in or replaced paths with this
# while any '-I...' options to sim_telarray precedes any of these paths.
# Recent versions of the generic_run.sh script may also fill in the
# values of environment values SIM_TELARRAY_DEFINES and SIM_TELARRAY_INCLUDES
# early into the sim_telarray command line. 

# Paths where data gets written to. Normally everything goes into
# a sub-directory/symlink 'Data' but you can either set MCDATA_PATH
# or CTA_DATA to direct the output elsewhere.
if [ -z "${MCDATA_PATH}" ]; then
   if [ ! -z "${CTA_DATA}" ]; then
      export MCDATA_PATH="${CTA_DATA}"
   else
      export MCDATA_PATH="${CTA_PATH}/Data"
   fi
fi
# CORSIKA is run in a 'run......' sub-directory of this path:
#export CORSIKA_DATA="${MCDATA_PATH}/corsika"
# Sim_telarray output goes into config dependent sub-directory of this path:
export SIM_TELARRAY_DATA="${MCDATA_PATH}/sim_telarray"

#printenv | egrep '^(CTA_PATH|CORSIKA_PATH|SIM_TELARRAY_PATH|SIM_TELARRAY_CONFIG_PATH|SIMTEL_CONFIG_PATH|HESSIO_PATH|CTA_DATA|MCDATA_PATH|CORSIKA_DATA|SIM_TELARRAY_DATA|HESSROOT|LD_LIBRARY_PATH|PATH|RUNPATH)=' | sort

