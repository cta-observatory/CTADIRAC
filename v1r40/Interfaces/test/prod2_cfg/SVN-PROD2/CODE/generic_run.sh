#!/bin/sh

# ------------------------------------------------------------------------------
#
# General script for running sim_telarray simulations for 
#       a HESS 4-telescope system (phase 1 variants), or
#       the HESS-2 5-telescope system in hybrid mode, or
#       a CTA benchmark array (9 telescopes), or
#       a system of 41 HESS-1 telescopes, or
#       the 97 telescope array made up of super-HESS-1 and super-HESS-2 types, or
#       various variants of CTA study layouts.
#
# There are two typical operation modes:
#   a) The Corsika data is stored on disk and the run parameters are registered
#      in the database.
#      In this case the run number will be assumed to be the first parameter
#      and the pointing direction and primary id are used from the database.
#   b) The Corsika data comes in standard input, with some environment
#      variables set up by the multipipe_corsika program.
#      In this case the run number, pointing direction and primary id are
#      taken from the CORSIKA_* enviroment variables:
#          CORSIKA_RUN
#          CORSIKA_THETA
#          CORSIKA_PHI
#          CORSIKA_PRIMARY (e.g. '1' for gamma rays)
#          CORSIKA_OBSLEV
#          CORSIKA_TELESCOPES
#
# There are also ways to override settings in either mode through variables
#   offset:       Offset angle to (center of) generated shower direction. [deg]
#   conv_depth:   Atmospheric depth at which telescope directions should converge.
#   transmission: Name of atmospheric transmission table.
#   pixels:       Number of pixels required for trigger (note: '3' means '2.5').
#   threshold:    Pixel threshold in millivolts.
#   zenith_angle: In deg.
#   azimuth_angle:In deg.
#   primary:      Name of primary particle type (´gamma´ for gamma rays).
#   nsb:          nighsky background p.e. rate [GHz]
#   reprocessing: Old output files should be regenerated.
#   repro_older_than: If reprocessing=1 then files older than this will be
#       regenerated while for existing newer output files no action is taken.
#
# Defaults are for parallel viewing (conv_depth=0) towards the generated 
# direction (or center of direction cone or range, if applicable) with
# desert haze model (base of boundary layer at HESS site). The NSB rate
# defaults to 100 MHz (i.e. 0.1) for vertical, slightly increasing with
# zenith angle.
#
# For each variant a specific main configuration file will be used. 
# All other parameters will be passed on the command line.
#
# ------------------------------------------------------------------------------

export LC_ALL=C

echo "$$ starting"

if [ -z "$cfg" ]; then
   cfg="$(basename $0 | sed 's/^\(.*\)_run.sh$/\1/' | sed 's/^\(.*\).sh$/\1/')"
fi

if [ -z "${SIM_TELARRAY_PATH}" ]; then
   SIM_TELARRAY_RUN_PATH="$(cd $(dirname $0) && pwd -P)"
   if [ "${SIM_TELARRAY_RUN_PATH}" = "." ]; then
      SIM_TELARRAY_RUN_PATH="$(pwd -P)"
   fi
   if [ "$(dirname ${SIM_TELARRAY_RUN_PATH})" = "sim_telarray" ]; then
      SIM_TELARRAY_PATH="$(dirname ${SIM_TELARRAY_RUN_PATH})"
   else
      SIM_TELARRAY_PATH="${SIM_TELARRAY_RUN_PATH}"
   fi
fi

progpath="${SIM_TELARRAY_PATH}"

cd "$progpath" || exit 1

echo "Working directory is `/bin/pwd`"

if [ ! -z "${MCDATA_PATH}" ]; then
   echo "MCDATA_PATH=${MCDATA_PATH}"
   mcdatapath="${MCDATA_PATH}"
else
   mcdatapath="Data"
fi
if [ ! -d "${mcdatapath}" ]; then
   if [ ! -z "${HESSMCDATA}" ]; then
      echo "HESSMCDATA=${HESSMCDATA}"
      mcdatapath="${HESSMCDATA}"
   else
      if [ -e "${HOME}/mcdata" ]; then
         mcdatapath="${HOME}/mcdata"
      fi
   fi
fi

echo "Data path is ${mcdatapath}"

if [ -z "$offset" ]; then
   echo "Offset angle is missing"
   exit 1
fi

echo '' >&2
echo "Relevant environment variables include:" >&2
printenv | egrep '^((CORSIKA|SIM_TELARRAY|SIMTEL|SIM_HESSARRAY|HESS|HESSIO|CTA|MCDATA|LD)_|PATH=)' | sort >&2
echo '' >&2

if [ -z "$reprocessing" ]; then
   reprocessing=0
fi
if [ -z "$repro_older_than" ]; then
   repro_older_than="2006-07-01 00:00:00"
fi

if [ "x$1" = "x--test" ]; then
   testonly=1
   shift
fi

if [ ! -z "$maxtel" ]; then
   MAX_TEL="$maxtel"
fi

if [ -z "$transmission" ]; then
   # All default transmission models assume tropical profile, desert aerosols (no wind)
   transtype="_desert" 
   transcode=10
   profilecode=1
   if [ ! -z "${CORSIKA_OBSLEV}" ]; then
      altitude="${CORSIKA_OBSLEV}"
   fi
   if [ -z "$altitude" ]; then
      case "$cfg" in
         phase[123]*) altitude=1800 ;;
         hess*) altitude=1800 ;;
         cta0) altitude=0 ;;
         cta|cta1|cta2) altitude=2000 ;;
         cta-he*|cta-ultra*|cta-prod*|cta-trg-test*) altitude=2000 ;;
         cta35) altitude=3500 ;;
         cta37) altitude=3700 ;;
         cta5) altitude=5000 ;;
         *) altitude=1800 ;;
      esac
   fi
   transmission="atm_trans_${altitude}_${profilecode}_${transcode}_0_0_${altitude}.dat"
fi

if [ -z "$conv_depth" ]; then
   conv_depth="0"
fi

if [ -z "${CORSIKA_RUN}" ]; then
   runnum="$1"
   shift
else
   runnum="${CORSIKA_RUN}"
fi

more_config="$extra_config $*"
echo "$$: Run number: $runnum, conv_depth=$conv_depth, offset=$offset"
echo "$$: Config options: $more_config"

export PATH=$PATH:${SIM_TELARRAY_PATH}/bin:.

if [ -z $zenith_angle ]; then
   zenith_angle="${CORSIKA_THETA}"
fi
zenith_angle2="`echo ${zenith_angle}'+'${offset} | bc -l`"
if [ -z $azimuth_angle ]; then
   azimuth_angle="${CORSIKA_PHI}"
fi
if [ "$azimuth_angle" = "360" ]; then
   azimuth_angle="0"
fi

plidx="2.68"

if [ -z $primary ]; then
   primary_id="${CORSIKA_PRIMARY}"
   primary="unknown"
   case $primary_id in
      1)
         primary="gamma" 
	 plidx="2.50" ;;
      2)
         primary="positron"
	 plidx="3.30" ;;
      3)
         primary="electron"
	 plidx="3.30" ;;
      [56])
         primary="muon" ;;
      14)
         primary="proton" ;;
      402)
         primary="helium" ;;
      1206)
         primary="carbon" ;;
      1407)
         primary="nitrogen" ;;
      1608)
         primary="oxygen" ;;
      2412)
         primary="magnesium" ;;
      2814)
         primary="silicon" ;;
      4020)
         primary="calcium" ;;
      5626)
         primary="iron" ;;
   esac
fi

if [ "$primary" = "unknown" ]; then
   echo "Fatal: Cannot identify primary particle type $primary for run $runnum."
   exit 1
fi

if [ "$primary" != "gamma" ]; then
   if [ "$offset" != "0.0" ]; then
      echo "Fatal: Refusing to run non-zero offset simulations for $primary primaries."
      exit 1
   fi
fi

usecone="unknown"
nonzerocone="0"
if [ ! -z "${CORSIKA_CONE}" ]; then
   usecone=$(echo "${CORSIKA_CONE} > 1.5" | bc)
   nonzerocone=$(echo "${CORSIKA_CONE} > 0.01" | bc)
   if [ "$offset" != "0.0" ]; then
      if [ "$usecone" = "1" ]; then
         echo "Fatal: Refusing to run non-zero offset simulations with large viewing cone."
         exit 1
      fi
   fi
fi

if [ -z $nsb ]; then
   case "$cfg" in
      phase1*|hess_41tel|41tel)
         nsbraw="`echo '0.1/e(l(c('${zenith_angle}'/180.*3.1416))*0.25)' | bc -l`"
         nsb="`printf '%5.3f\n' $nsbraw`"
	 ;;
   esac
fi

output_ext="simtel"
prgname="sim_telarray"
case "$cfg" in
   phase1*|phase2*)
      output_ext="simhess"
      prgname="sim_hessarray"
      ;;
esac

case "$cfg" in
   phase1*|hess_41tel)
      if [ -z $pixels ]; then
         pixels="3"
       fi
       if [ -z $threshold ]; then
          threshold="112"
       fi
       ;;
   cta-he*)
      if [ -z $pixels ]; then
         pixels="3"
       fi
       if [ -z $threshold ]; then
          if [ "$pixels" = "2" ]; then
             threshold="280"
	  else
	     threshold="75"
	  fi
       fi
       ;;
   cta-ultra3i-[fmrs])
      if [ "$cfg" = "cta-ultra3i-f" ]; then
         extra_defs="-DPROD1F"
         extra_suffix="-prod1f"
      fi
      if [ "$cfg" = "cta-ultra3i-m" ]; then
         extra_defs="-DPROD1M"
         extra_suffix="-prod1m"
      fi
      if [ "$cfg" = "cta-ultra3i-r" ]; then
         extra_defs="-DPROD1R"
         extra_suffix="-prod1r"
      fi
      if [ "$cfg" = "cta-ultra3i-s" ]; then
         extra_defs="-DPROD1S"
         extra_suffix="-prod1s"
      fi
       ;;
   cta-ultra*)
       ;;
   cta-prod*)
       ;;
   cta-trg-test*)
       ;;
   cta)
      if [ -z $pixels ]; then
         pixels="3"
       fi
       if [ -z $threshold ]; then
          threshold="98"
       fi
       ;;
   cta-4m-dcsst)
       ;;
   cta-sc-mst)
       ;;
esac

cfgnm="$cfg"
case "$cfg" in
   cta) cfgnm="cta1" ;;
   hess_41tel) cfgnm="hess41" ;;
esac
if [ "$conv_depth" = "0" ]; then
   basecfg="${cfgnm}${rotflag}"
   basecfgpath="${cfgnm}"
else
   basecfg="${cfgnm}${rotflag}conv${conv_depth}"
   basecfgpath="${cfgnm}conv"
fi

if [ -z "$CORSIKA_RUN" ]; then
   inputpath="${mcdatapath}/corsika"

   indir=run`printf '%06d' "$runnum"`

   if [ -d "${inputpath}/${indir}/tmp" ]; then
      inputs="`ls ${inputpath}/${indir}/tmp/run*.corsika* 2>/dev/null | tail -1`"
   else
      inputs="`ls ${inputpath}/${indir}/run*.corsika* 2>/dev/null | tail -1`"
   fi

   if [ "x${inputs}" = "x" ]; then
      echo "No input file for run $runnum"
      exit 1
   fi
else
#  Standard input:
   inputs="-"
fi

outputpath="${mcdatapath}/${prgname}/${basecfgpath}/${offset}deg"
outprefix="${primary}_${zenith_angle}deg_${azimuth_angle}deg_run${runnum}"

# If desired we could create the output path as needed:
if [ ! -d ${outputpath} ]; then
   mkdir -p ${outputpath}/Data
   mkdir -p ${outputpath}/Log
   mkdir -p ${outputpath}/Histograms
fi

if [ "$offset" = "0.0" ]; then
   output_name="${outprefix}_${pixels}_${threshold}_${basecfg}${transtype}${extra_suffix}"
else
   output_name="${outprefix}_${pixels}_${threshold}_${basecfg}${transtype}${extra_suffix}_off${offset}"
fi

if [ "$primary" = "gamma" -a "$nonzerocone" = "1" ]; then
   output_name="${output_name}_cone${CORSIKA_CONE}"
fi

output_file="${outputpath}/Data/${output_name}.${output_ext}.gz"
hdata_file="${outputpath}/Histograms/${output_name}.hdata.gz"
log_file="${outputpath}/Log/${output_name}.log"

if [ -f "${output_file}" ]; then
   if [ "$reprocessing" = "1" ]; then
      if [ -f "${output_file}.old" ]; then
         echo "Reprocessing requested but ${output_file}.old already exists. Nothing done."
	 exit 1;
      else
         touch --date "$repro_older_than" "${output_file}.test"
	 if [ "${output_file}" -nt "${output_file}.test" ]; then
	    echo "Reprocessing requested but ${output_file} is a new file. Nothing done."
	    rm "${output_file}.test"
	    exit 1;
	 fi
	 rm "${output_file}.test"
         mv "${output_file}" "${output_file}.old" || exit 1
	 echo "Reprocessing requested: file ${output_file} renamed to ${output_file}.old."
	 mv -f ${hdata_file} ${hdata_file}.old
	 mv -f ${log_file} ${log_file}.old
	 mv -f ${log_file}.gz ${log_file}.gz.old
      fi
   fi
fi

if [ -f "${output_file}" ]; then
   echo "Output file ${output_file} already exists. Nothing done."
   exit 1;
fi

iobufmx=200000000
defs="${SIM_TELARRAY_DEFINES} ${SIM_TELARRAY_INCLUDES} ${extra_defs}"
cfgfile="unknown-config"
if [ -z "${CORSIKA_TELESCOPES}" ]; then
   maxtel=1
   numtel=0
else
   maxtel="${CORSIKA_TELESCOPES}"
   numtel="${CORSIKA_TELESCOPES}"
fi
defs="${defs} -DNUM_TELESCOPES=${numtel}"
extraopt=""
case "$cfgnm" in
   phase1)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE1=1"
       cfgfile="hess_bestguess.cfg"
       maxtel=4
       ;;
   phase1a)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE1A=1"
       cfgfile="hess_bestguess.cfg"
       maxtel=4
       ;;
   phase1b)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE1B=1"
       cfgfile="hess_bestguess.cfg"
       maxtel=4
       ;;
   phase1c)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE1C=1"
       cfgfile="hess_bestguess.cfg"
       maxtel=4
       ;;
   phase1c1)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE1C1=1"
       cfgfile="hess_bestguess.cfg"
       maxtel=4
       ;;
   phase1c2)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE1C2=1"
       cfgfile="hess_bestguess.cfg"
       maxtel=4
       ;;
   phase1c3)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE1C3=1"
       cfgfile="hess_bestguess.cfg"
       maxtel=4
       ;;
   phase1d)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE1D=1"
       cfgfile="hess_bestguess.cfg"
       maxtel=4
       ;;
   phase2)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE2=1 -DHESS2_SECTOR=1"
       cfgfile="hess-phase2.cfg"
       maxtel=5
       ;;
   phase2a100)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE2A100=1 -DHESS2_SECTOR=1"
       cfgfile="hess-phase2.cfg"
       maxtel=5
       ;;
   phase2a80)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE2A80=1 -DHESS2_SECTOR=1"
       cfgfile="hess-phase2.cfg"
       maxtel=5
       ;;
   phase2a50)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE2A50=1 -DHESS2_SECTOR=1"
       cfgfile="hess-phase2.cfg"
       maxtel=5
       ;;
   phase3)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE3=1"
       cfgfile="hess3.cfg"
       maxtel=97
       iobufmx=800000000
       ;;
   hess41)
       defs="${defs} -DNORMAL_MODE=1 -DPHASE1A=1"
       cfgfile="hess_bestguess.cfg"
       maxtel=41
       iobufmx=500000000
       ;;
   cta-he1-7025)
       defs="${defs} -DCTA_HE_1 -DCTA_HE_CAM_7025"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-HE.cfg"
       maxtel=75
       iobufmx=800000000
       ;;
   cta-he1-8030)
       defs="${defs} -DCTA_HE_1 -DCTA_HE_CAM_8030"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-HE.cfg"
       maxtel=75
       iobufmx=800000000
       ;;
   cta-he1*)
       defs="${defs} -DCTA_HE_1"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-HE.cfg"
       maxtel=75
       iobufmx=800000000
       ;;
   cta-he2-7025)
       defs="${defs} -DCTA_HE_2 -DCTA_HE_CAM_7025"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-HE.cfg"
       maxtel=75
       iobufmx=800000000
       ;;
   cta-he2-8030)
       defs="${defs} -DCTA_HE_2 -DCTA_HE_CAM_8030"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-HE.cfg"
       maxtel=75
       iobufmx=800000000
       ;;
   cta-he2*)
       defs="${defs} -DCTA_HE_2"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-HE.cfg"
       maxtel=75
       iobufmx=800000000
       ;;
   cta-ultra1*)
       # Call with 'extra_defs="-DHIGHEST_QE" extra_suffix="-hQE"' for high-QE version.
       defs="${defs} -DCTA_ULTRA1"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-ULTRA1.cfg"
       maxtel=275
       iobufmx=2000000000
       ;;
   cta-ultra2*)
       # Call with 'extra_defs="-DHIGHEST_QE" extra_suffix="-hQE"' for high-QE version.
       defs="${defs} -DCTA_ULTRA2"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-ULTRA2.cfg"
       maxtel=275
       iobufmx=200000000
       ;;
   cta-ultra3-LST)
       defs="${defs} -DCTA_ULTRA3"
       extraopt="-Icfg/CTA -C trigger_telescopes=1"
       cfgfile="cfg/CTA/CTA-ULTRA3-LST.cfg"
       maxtel=1
       iobufmx=200000000
       ;;
   cta-ultra3-LST4)
       defs="${defs} -DCTA_ULTRA3"
       extraopt="-Icfg/CTA -C trigger_telescopes=2"
       cfgfile="cfg/CTA/CTA-ULTRA3-LST.cfg"
       maxtel=4
       iobufmx=200000000
       ;;
   cta-ultra3-MST)
       defs="${defs} -DCTA_ULTRA3"
       extraopt="-Icfg/CTA -C trigger_telescopes=1"
       cfgfile="cfg/CTA/CTA-ULTRA3-MST.cfg"
       maxtel=1
       iobufmx=200000000
       ;;
   cta-ultra3-MST4)
       defs="${defs} -DCTA_ULTRA3"
       extraopt="-Icfg/CTA -C trigger_telescopes=2"
       cfgfile="cfg/CTA/CTA-ULTRA3-MST.cfg"
       maxtel=4
       iobufmx=200000000
       ;;
   cta-ultra3i*)
       # Use 'extra_defs="-DPROD1F" extra_suffix="-prod1f"' for fast sampling
       # Use 'extra_defs="-DPROD1M" extra_suffix="-prod1m"' for medium sampling
       # Use 'extra_defs="-DPROD1R" extra_suffix="-prod1r"' for FlashCam raw sampling
       # Use 'extra_defs="-DPROD1S" extra_suffix="-prod1r"' for FlashCam slow effective sampling
       defs="${defs} -DCTA_ULTRA3i"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-ULTRA3i.cfg"
       maxtel=77
       iobufmx=800000000
       ;;
   cta-ultra3*|cta-prod1)
       # Call with 'extra_defs="-DHIGHEST_QE" extra_suffix="-hQE"' for high-QE version.
       defs="${defs} -DCTA_ULTRA3"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-ULTRA3.cfg"
       maxtel=275
       iobufmx=800000000
       ;;
   cta-ultra5*|cta-prod2|cta-4m-dcsst)
       # Call with 'extra_defs="-DHIGHEST_QE" extra_suffix="-hQE"' for high-QE version.
       defs="${defs} -DCTA_ULTRA5"
       extraopt="-Icfg/CTA"
       export SIM_TELARRAY_CONFIG_PATH="cfg/hess:cfg/common:cfg/CTA"
       cfgfile="cfg/CTA/CTA-ULTRA5.cfg"
       maxtel=168
       iobufmx=2000000000
       # We may have some additional positions for MSTs/SCTs
       if [ ${CORSIKA_TELESCOPES}0 > 1680 ]; then
         defs="${defs} -DEXTRA_MST_SCT"
         maxtel=197
       fi
       if [ ${cfgnm} = "cta-4m-dcsst" ]; then
	   extraopt="-I${SVNPROD2}/CONFIG/4mDC "
	   cfgfile="${SVNPROD2}/CONFIG/4mDC/CTA-ULTRA5.cfg"
	   #cfgfile="cfg/CTA/CTA-ULTRA5.cfg"
       fi
       ;;
    cta-sc-mst)
	defs="${defs} -DCTA_SC2 -DEXTRA_MST_SCT"
	maxtel=111
	iobufmx=800000000
	export SIM_TELARRAY_CONFIG_PATH="cfg/hess:cfg/common:cfg/CTA"
	extraopt="-I${SVNPROD2}/CONFIG/SC-MST "
	cfgfile="cfg/CTA/CTA-ULTRA5-medium-sc.cfg"
       ;;
   cta-prod1s)
       defs="${defs} -DCTA_PROD1S"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-PROD1S.cfg"
       maxtel=232
       iobufmx=800000000
       ;;
   cta-trg-test-LST)
       defs="${defs} -DCTA"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-trg-test-LST.cfg"
       maxtel=1
       iobufmx=200000000
       ;;
   cta-trg-test-LST4)
       defs="${defs} -DCTA"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-trg-test-LST4.cfg"
       maxtel=4
       iobufmx=200000000
       ;;
   cta-trg-test-MST)
       defs="${defs} -DCTA"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-trg-test-MST.cfg"
       maxtel=1
       iobufmx=200000000
       ;;
   cta-trg-test-MST4)
       defs="${defs} -DCTA"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA-trg-test-MST4.cfg"
       maxtel=4
       iobufmx=200000000
       ;;
   cta-trg-test-prod2-LST)
       defs="${defs} -DCTA"
       extraopt="-Icfg/CTA -C trigger_telescopes=1"
       cfgfile="cfg/CTA/CTA-ULTRA5-large.cfg"
       maxtel=1
       iobufmx=200000000
       ;;
   cta-trg-test-prod2-MST)
       defs="${defs} -DCTA"
       extraopt="-Icfg/CTA -C trigger_telescopes=1"
       cfgfile="cfg/CTA/CTA-ULTRA5-medium.cfg"
       maxtel=1
       iobufmx=200000000
       ;;
   cta-trg-test-prod2-DC-SST)
       defs="${defs} -DCTA"
       extraopt="-Icfg/CTA -C trigger_telescopes=1"
       cfgfile="cfg/CTA/CTA-ULTRA5-small-dc.cfg"
       maxtel=1
       iobufmx=200000000
       ;;
   cta-trg-test-prod2-SC-SST)
       defs="${defs} -DCTA"
       extraopt="-Icfg/CTA -C trigger_telescopes=1"
       cfgfile="cfg/CTA/CTA-ULTRA5-small-sc.cfg"
       maxtel=1
       iobufmx=200000000
        ;;
   cta-trg-test-prod2-LST4)
       defs="${defs} -DCTA -DPROD2"
       extraopt="-Icfg/CTA -C trigger_telescopes=2"
       cfgfile="cfg/CTA/CTA-ULTRA5-large.cfg"
       maxtel=4
       iobufmx=200000000
       ;;
   cta-trg-test-prod2-MST4)
       defs="${defs} -DCTA -DPROD2"
       extraopt="-Icfg/CTA -C trigger_telescopes=2"
       cfgfile="cfg/CTA/CTA-ULTRA5-medium.cfg"
       maxtel=4
       iobufmx=200000000
       ;;
   cta-trg-test-prod2-DC-SST4)
       defs="${defs} -DCTA -DPROD2"
       extraopt="-Icfg/CTA -C trigger_telescopes=2"
       cfgfile="cfg/CTA/CTA-ULTRA5-small-dc.cfg"
       maxtel=4
       iobufmx=200000000
       ;;
   cta-trg-test-prod2-SC-SST4)
       defs="${defs} -DCTA -DPROD2"
       extraopt="-Icfg/CTA -C trigger_telescopes=2"
       cfgfile="cfg/CTA/CTA-ULTRA5-small-sc.cfg"
       maxtel=4
       iobufmx=200000000
        ;;
   cta)
       defs="${defs} -DCTA"
       extraopt="-Icfg/CTA"
       cfgfile="cfg/CTA/CTA.cfg"
       maxtel=9
       iobufmx=800000000
       ;;
   *)
       echo 'Unknown sim_telarray configuration set'
       exit 1
       ;;
esac

if [ ! -z "${MAX_TEL}" ]; then
   maxtel="${MAX_TEL}"
fi

# Some setups are available for different altitudes
# (note: must match the atmospheric transmission file).
if [ ! -z "${altitude}" ]; then
   extraopt="${extraopt} -C Altitude=${altitude}"
fi

# If the NSB is not used as given in the config file, set it here:
if [ ! -z "${nsb}" ]; then
   extraopt="${extraopt} -C nightsky_background=all:${nsb}"
fi
# For HESS phase1 configurations we also used to set the
# trigger conditions explicitly
if [ ! -z "${pixels}" ]; then
   extraopt="${extraopt} -C trigger_pixels=${pixels}"
fi
if [ ! -z "${threshold}" ]; then
   extraopt="${extraopt} -C discriminator_threshold=${threshold}"
fi

# Show what is supposed to be done:

echo "$$: Starting: " ./bin/${prgname} -c "${cfgfile}" \
        ${defs} ${extraopt} \
	-C "iobuf_maximum=${iobufmx}" \
	-C "maximum_telescopes=${maxtel}" \
	-C "atmospheric_transmission=$transmission" \
        -C "altitude=${altitude}" \
	-C "telescope_theta=${zenith_angle2}" \
	-C "telescope_phi=${azimuth_angle}" \
	-C "power_law=${plidx}" \
	-C "histogram_file=${hdata_file}" \
	-C "output_file=${output_file}" \
	-C "random_state=auto" \
	$more_config \
	-C "show=all" \
	${inputs} "| gzip > ${log_file}.gz"

if [ "$testonly" = 1 ]; then
   echo "This was just a script test and nothing actually run."
   exit 1;
fi

if [ ! -x "./bin/${prgname}" ]; then
   echo "Cannot run ./bin/${prgname}: no such file or not executable"
   exit 1
fi

# Now start to do the real work:

./bin/${prgname} -c "${cfgfile}" \
        ${defs} ${extraopt} \
	-C "iobuf_maximum=${iobufmx}" \
	-C "maximum_telescopes=${maxtel}" \
	-C "atmospheric_transmission=$transmission" \
        -C "altitude=${altitude}" \
	-C "telescope_theta=${zenith_angle2}" \
	-C "telescope_phi=${azimuth_angle}" \
	-C "power_law=${plidx}" \
	-C "histogram_file=${hdata_file}" \
	-C "output_file=${output_file}" \
	-C "random_state=auto" \
	$more_config \
	-C "show=all" \
	${inputs} \
	 2>&1 | gzip > "${log_file}.gz"

# If reprocessing was successful, the old file is obsolete now.
if [ "$reprocessing" = "1" ]; then
   if [ -f "${output_file}.old" ]; then
      mv -f "${output_file}.old" "${output_file}.obsolete"
   fi
fi

# If the dbtools are available we might as well fill the database values now.

if [ -z "NO_SIMHESS_STAT" ]; then
   if [ -x "${HESSROOT}/bin/read_simhess_stat" ]; then
      if [ -f ${HOME}/.dbtoolsrc ]; then
	 fgrep '[simulation]' ${HOME}/.dbtoolsrc >/dev/null && \
            ${HESSROOT}/bin/read_simhess_stat "${output_file}"
      fi
   fi
fi
