#!/bin/bash
WRFOUT=$HOME"/Development/temp/wrfout_A_d01_2020-02-06_18:00:00"
OUTDIR_PRODUCTOS="../geotiff/"
OUTDIR_TABLA="temp/"

ray start --head --port=6380 --num-cpus=4

time python wrfmeteo.py ${WRFOUT} ${OUTDIR_PRODUCTOS} ${OUTDIR_TABLA} 

time python wrfzonas.py --path "../geotiff/2020_02/06/CBA_A_18_*" --target zonas --shapefile shapefiles/Zonas_Cobertura.shp

ray stop