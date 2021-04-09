#!/bin/bash
WRFOUT=$HOME"/Development/temp/wrfout_A_d01_2020-02-06_18:00:00"
OUTDIR_PRODUCTOS="../geotiff/"
OUTDIR_TABLA="temp/"

ray start --head --port=6380 --num-cpus=4

time python wrfmeteo.py ${WRFOUT} ${OUTDIR_PRODUCTOS} ${OUTDIR_TABLA} 

time python wrfzonas.py --path "../geotiff/2020_02/06/*" --target zonas --shapefile shapefiles/zonas.shp

time python ingestor.py --path csv/WRF_${}_T2P_18.csv


ray stop