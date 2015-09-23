#! /bin/sh                                                                                                                         
echo "go for sim_telarray"
export CORSIKA_IO_BUFFER=800MB 
zcat $1 | run_sim_cta-ultra5                                                                                                                     
