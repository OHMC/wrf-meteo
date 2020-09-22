# wrf-meteo
Este paquete genera archivos GeoTIFF a partir de archivos de salida del modelo WRF
Desarrollado para el Observatorio Hidro-Meteorológico de Córdoba

## Parametros de entrada
| Variable     | Descripción      |
|--------------|:----------------:|
| wrfout        | Archivo en formato NetCDF  |
| path geotiff  | Path donde se guardan los geotiff generados |
| path tablas   | Path donde se guardan los csv generados (TBI) |

## Ejemplo
´´´Bash
time python wrfmeteo.py wrfout_A_d01_2020-02-06_18:00:00 ../geotiff/ temp/
´´´


