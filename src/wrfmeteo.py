import numpy as np
import argparse
import datetime
import re
import rasterio
import time
import os
import wrf
# import geopandas as gpd
# import pandas as pd
# from glob import glob
from config.logging_conf import (METEO_LOGGER_NAME,
                                 get_logger_from_config_file)
from config.constantes import (CBA_EXTENT, WRF_EXTENT, WRF_VARIABLES,
                               WRFOUT_REGEX, KM_PER_DEGREE, RESOLUTION,
                               PROG_VERSION)
from osgeo import gdal_array, gdal, osr
from pathlib import Path
from netCDF4 import Dataset
from affine import Affine
# from rasterstats import zonal_stats


logger = get_logger_from_config_file(METEO_LOGGER_NAME)


def get_configuracion(wrfout) -> (str, datetime.datetime):
    """Retorna la parametrizacion y el timestamp a partir del
    nombre del archivo wrfout"""
    m = re.match(WRFOUT_REGEX, wrfout)
    if not m:
        logger.critical("No se pudo obtener la configuracion, proporcione"
                        "una desde los parametros de ejecición.")
        raise ValueError
    m_dict = m.groupdict()
    param = m_dict.get('param')
    timestamp = datetime.datetime.strptime(m_dict.get('timestamp'),
                                           '%Y-%m-%d_%H:%M:%S')
    return param, timestamp


def getGeoT(extent, nlines, ncols):
    # Compute resolution based on data dimension
    resx = (extent[2] - extent[0]) / ncols
    resy = (extent[3] - extent[1]) / nlines
    return [extent[0], resx, 0, extent[3], 0, -resy]


# TodDo: ray to this
def cambiar_projection(in_array: np.ndarray):
    """Convert Grid to New Projection.
        Parameters
        ----------
        in_array

    """
    # WRF Spatial Reference System
    source_prj = osr.SpatialReference()
    source_prj.ImportFromProj4('+proj=lcc +lat_0=-32.500008 +lon_0=-62.7 '
                               '+lat_1=-60 +lat_2=-30 +x_0=0 +y_0=0 +R=6370000'
                               ' +units=m +no_defs')
    # Lat/lon WSG84 Spatial Reference System
    target_prj = osr.SpatialReference()
    target_prj.ImportFromProj4('+proj=longlat +ellps=WGS84 '
                               '+datum=WGS84 +no_defs')

    # se configura la matriz destino
    sizex = int(((CBA_EXTENT[2] - CBA_EXTENT[0]) * KM_PER_DEGREE) / RESOLUTION)
    sizey = int(((CBA_EXTENT[3] - CBA_EXTENT[1]) * KM_PER_DEGREE) / RESOLUTION)

    out_array = np.ndarray(shape=(int(in_array.coords['Time'].count()),
                           sizey, sizex))

    for t in range(in_array.coords['Time'].size):
        # loar gdal array y se le asigna la projección y transofrmación
        raw = gdal_array.OpenArray(np.flipud(in_array[t].values))
        raw.SetProjection(source_prj.ExportToWkt())
        raw.SetGeoTransform(getGeoT(WRF_EXTENT,
                                    raw.RasterYSize,
                                    raw.RasterXSize))

        grid = gdal.GetDriverByName('MEM').Create("tmp_ras",
                                                  sizex, sizey, 1,
                                                  gdal.GDT_Float32)
        # Setup projection and geo-transformation
        grid.SetProjection(target_prj.ExportToWkt())
        grid.SetGeoTransform(getGeoT(CBA_EXTENT,
                                     grid.RasterYSize,
                                     grid.RasterXSize))

        # reprojectamos
        gdal.ReprojectImage(raw,
                            grid,
                            source_prj.ExportToWkt(),
                            target_prj.ExportToWkt(),
                            gdal.GRA_NearestNeighbour,
                            options=['NUM_THREADS=ALL_CPUS'])

        out_array[t] = grid.ReadAsArray()

    return out_array, grid.GetGeoTransform(), grid.GetProjection()


def guardar_tif(geoTransform: list, target_prj: str,
                arr: np.ndarray, out_path: str):
    nw_ds = rasterio.open(out_path, 'w', driver='GTiff',
                          height=arr.shape[0],
                          width=arr.shape[1],
                          count=1, dtype=str(arr.dtype),
                          crs=target_prj,
                          transform=Affine.from_gdal(*geoTransform))
    nw_ds.write(arr, 1)
    nw_ds.close()


def generar_imagenes(ncwrf: Dataset, configuracion: str, path_gtiff: str):
    """
    """
    try:
        os.makedirs(os.path.dirname(path_gtiff))
    except OSError:
        pass

    for variable in WRF_VARIABLES:
        var = wrf.getvar(ncwrf, variable, timeidx=wrf.ALL_TIMES)
        if variable == 'T2':
            var.values = var.values - 273.15

        var_proj, geoTransform, target_prj = cambiar_projection(var)
        base_path = f"{path_gtiff}{configuracion}_{variable}"

        for t in range(ncwrf.dimensions['Time'].size):
            date = str(var.coords['Time'].values[t])[:16]
            guardar_tif(geoTransform,
                        target_prj,
                        var_proj[t],
                        f"{base_path}_{date}")


''''
def integrar_en_zonas(out_path: str,
                      configuracion: str) -> gpd.GeoDataFrame:
    """
    This functions opens a geotiff with ppn data, converts to a raster,
    integrate the ppn into cuencas and returns a GeoDataFrame object.

    Parameters:
        cuencas_shp: Path to shapefile
    Returns:
        cuencas_gdf_ppn (GeoDataFrame): a geodataframe with cuerncas and ppn
    """
    zonas_gdf = gpd.read_file(SHAPE_ZONAS)
    tmp_gdf = zonas_gdf[['Name', 'geometry']]

    base_path = f"{out_path}{configuracion}_T2"
    lista_tiff = sorted(glob(f'{base_path}*'), key=os.path.getmtime)

    for gtiff in lista_tiff:
        df_zs = pd.DataFrame(zonal_stats(SHAPE_ZONAS, gtiff, all_touched=True))
        df_zs = df_zs.rename(columns={'mean': gtiff[65:]})
        tmp_gdf = pd.concat([tmp_gdf, df_zs[gtiff[65:]]], axis=1)

    return tmp_gdf
'''


def generar_producto_meteo(wrfout: str, outdir_productos: str, outdir_csv: str,
                           configuracion=None):
    wrfout_path = Path(wrfout)
    param, rundate = get_configuracion(wrfout_path.name)

    if not configuracion:
        configuracion = f"CBA_{param}_{rundate.hour:02d}"

    ncwrf = Dataset(wrfout)

    start = time.time()
    path_gtiff = (f'{outdir_productos}/'
                  f'{rundate.strftime("%Y_%m/%d/")}')

    generar_imagenes(ncwrf, configuracion, path_gtiff)
    logger.info(f"Tiempo genear_img_prec = {time.time() - start}")
    ncwrf.close()

    start = time.time()
    # zonas_gdf_ppn = integrar_en_zonas(path_gtiff, configuracion)


def main():
    parser = argparse.ArgumentParser(prog="WRF Meteo")
    parser.add_argument("wrfout",
                        help="ruta al wrfout de la salida del WRF")
    parser.add_argument("outdir_productos",
                        help="ruta donde se guardan los productos")
    parser.add_argument("outdir_tabla",
                        help="ruta donde se guardan las tablas de datos")
    parser.add_argument("-c", "--configuracion",
                        help="configuracion de las parametrizaciones",
                        default='')
    parser.add_argument('-v', '--version',
                        action='version', version=f'%(prog)s {PROG_VERSION}')

    args = parser.parse_args()

    generar_producto_meteo(args.wrfout, args.outdir_productos,
                           args.outdir_tabla, args.configuracion)


if __name__ == "__main__":
    main()
