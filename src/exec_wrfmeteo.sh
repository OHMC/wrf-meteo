#!/bin/bash
WRFOUT=$HOME"/Development/temp/wrfout_A_d01_2020-02-06_18:00:00"
OUTDIR_PRODUCTOS="../geotiff/"
OUTDIR_TABLA="temp/"

time python wrfmeteo.py ${WRFOUT} ${OUTDIR_PRODUCTOS} ${OUTDIR_TABLA} 