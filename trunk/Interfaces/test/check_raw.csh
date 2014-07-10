#! /bin/csh

echo "=============== Checks of the Raw Data ======================"

set CHECKFILE="Open_Raw.log"

set val = '"DONE"'
set Success=`cat ${CHECKFILE}  | grep "SUCCESS" | wc | awk '{print $1}'`
if ( $Success == 0 ) then
  echo "  ### FATAL ERROR: FAILED during the Raw file check"
  set val = '"FAILED"'
else
  echo ""
  echo "  > RawData done and checked"
  echo "\n >> SUCCESS"
endif
echo ">>> Success $Success - Val $val"

if ($val == '"DONE"') then
exit 0
else if ($val == '"FAILED"') then
exit 1
endif 







