#!/bin/bash

# This file is intended for re-processing of CORSIKA data files for CTA Prod-2
# through the following sets of simulations
#   "Normal" prod-2 
#   SC-MST only simulation (note: requires specially compiled sim_telarray version && matching hessio library)

##JCT this seems wrong for us source ~/.bashrc

if [ -x ${HOME}/bin/init_batch_job ]; then
   . ${HOME}/bin/init_batch_job
fi

here="$(/bin/pwd)"
sc3_path=""
if [ -d "${here}-sc3" ]; then
   sc3_path="${here}-sc3"
fi
if [ ! -z "${SC3_PATH}" ]; then
   sc3_path="${SC3_PATH}"
fi
export HESSROOT=""
export LD_LIBRARY_PATH=""
if [ -d hessioxxx ]; then
   export LD_LIBRARY_PATH="${here}/hessioxxx/lib"
   export PATH="${here}/hessioxxx/bin:${PATH}"
elif [ -d hessio ]; then
   export LD_LIBRARY_PATH="${here}/hessio/lib"
   export PATH="${here}/hessio/bin:${PATH}"
fi
export PATH="${here}/sim_telarray/bin:${PATH}"
export HESSROOT=""
export HESSMCDATA=""
if [ -z "${MCDATA_PATH}" ]; then
   export MCDATA_PATH="$(cd ${here}/Data && /bin/pwd)"
fi

if [ ! -z "${REQNAME}" ]; then
   f=$(basename "${REQNAME}" | sed 's/\.sh//')
else
   f=$(basename $0 | sed 's/\.sh//')
fi

n=$(echo $f | sed 's/^[^_]*//' | sed 's/_//g')

if [ ! -z "$1" ]; then
   n="$1"
fi

echo "Reprocessing CTA Prod-2 simulation run $n"

if [ ! -f $1 ]; then
   echo "No such input file:  $1"
   exit 1
fi

for v in $(sim_telarray/bin/extract_corsika_tel --header-only --only-telescopes 1-229 \
	$1 2>/dev/null | egrep '^CORSIKA_'); do
#    echo "$v"
    export $v
done

echo "Available CORSIKA environment variables:"
printenv | egrep '^CORSIKA_'

# export testonly=1

if [ "$2" = "STD" ]; then
echo ""
echo "Reprocessing with normal prod-2 configuration (note: SC-SST threshold is too low)"
zcat $1 | \
  ( cd sim_telarray && env offset="0.0" cfg="cta-prod2" extra_defs="-DCTA_PROD2 -DCTA_ULTRA5" SIM_TELARRAY_INCLUDES="-I$SVNPROD2/$SVNTAG/CONFIG/cfg/CTA" ./generic_run.sh -c $SVNPROD2/$SVNTAG/CONFIG/cfg/CTA/CTA-ULTRA5.cfg)
fi

if [ "$2" = "NORTH" ]; then
echo ""
echo "Reprocessing with prod-2 configuration for North sites"
zcat $1 | \
  ( cd sim_telarray && env offset="0.0" cfg="cta-prod2n" extra_defs="-DCTA_PROD2 -DCTA_ULTRA5" SIM_TELARRAY_INCLUDES="-I$SVNPROD2/$SVNTAG/CONFIG/cfg/CTA" ./generic_run.sh -c $SVNPROD2/$SVNTAG/CONFIG/cfg/CTA/CTA-ULTRA5N.cfg)
fi

if [ "$2" = "NSBX3" ]; then
echo ""
echo "Reprocessing with prod-2 configuration using NSB increased by a factor of three and adapted thresholds"
zcat $1 | \
  ( cd sim_telarray && env offset="0.0" cfg="cta-prod2" extra_defs="-DCTA_PROD2 -DCTA_ULTRA5 -DTHREEFOLD_NSB" extra_suffix="-nsb-x3" SIM_TELARRAY_INCLUDES="-I$SVNPROD2/$SVNTAG/CONFIG/cfg/CTA" ./generic_run.sh -c $SVNPROD2/$SVNTAG/CONFIG/cfg/CTA/CTA-ULTRA5.cfg)
fi

if [ "$2" = "SCMST" ]; then
echo ""
echo "Reprocessing with sc3 configuration"
  sim_telarray/bin/extract_corsika_tel --only-telescopes 6-59,169,174-229 $1 | \
   ( cd sim_telarray && env CORSIKA_TELESCOPES=111 offset="0.0" cfg="cta-prod2-sc3" SIM_TELARRAY_INCLUDES="-I$SVNPROD2/$SVNTAG/CONFIG/SC-MST" ./generic_run.sh -c $SVNPROD2/$SVNTAG/CONFIG/cfg/CTA/CTA-ULTRA5-medium-sc.cfg)
fi

if [ "$2" = "4MSST" ]; then
echo ""
echo "Reprocessing 4 m DC-SST telescopes only."
echo "Prepending configuration from "$SVNPROD2/$SVNTAG"/CONFIG/4mDC"
zcat $1 | \
  ( cd sim_telarray && env offset="0.0" cfg="cta-prod2-4m-dc" extra_config="-C ignore_telescopes=1,2,3,4,5,6,10,14,15,16,17,18,19,20,21,22,23,24,25,26,33,40,41,42,43,44,45,46,53,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197" SIM_TELARRAY_INCLUDES="-I${SVNPROD2}/${SVNTAG}/CONFIG/4mDC" ./generic_run.sh -c $SVNPROD2/$SVNTAG/CONFIG/cfg/CTA/CTA-ULTRA5-small-4m-dc.cfg)
fi

if [ "$2" = "SCSST" ]; then
echo ""
echo "Reprocessing normal SC-SST telescopes only with fixed threshold and mono trigger."
zcat $1 | \
  ( cd sim_telarray && env offset="0.0" cfg="cta-prod2-sc-sst" extra_defs="-DNEW_MAJORITY_THRESHOLDS -DSUBARRAY_EXTRACTED" extra_config="-C maximum_telescopes=197" ./generic_run.sh -c $SVNPROD2/$SVNTAG/CONFIG/cfg/CTA/CTA-ULTRA5-small-sc.cfg)
fi

if [ "$2" = "ASTRI" ]; then
echo ""
echo "Reprocessing ASTRI SC-SST telescopes only."
zcat $1 | \
  ( cd sim_telarray && env offset="0.0" cfg="cta-prod2-astri" extra_config="-C ignore_telescopes=1,2,3,4,5,6,10,14,15,16,17,18,19,20,21,22,23,24,25,26,33,40,41,42,43,44,45,46,53,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197" SIM_TELARRAY_INCLUDES="-I$SVNPROD2/$SVNTAG/CONFIG/SC-SST-ASTRI" ./generic_run.sh -c $SVNPROD2/$SVNTAG/CONFIG/cfg/CTA/CTA-ULTRA5-small-astri.cfg)
fi


echo ""
echo "All done."
echo ""

