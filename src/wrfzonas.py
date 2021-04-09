import argparse
import glob
import ray
import pandas as pd
import geopandas as gpd
import numpy as np
import time
from config.logging_conf import (METEO_LOGGER_NAME,
                                 get_logger_from_config_file)
from rasterstats import zonal_stats
from datetime import datetime

logger = get_logger_from_config_file(METEO_LOGGER_NAME)

ray.init(address='localhost:6380', redis_password='5241590000000000')


def getInfo(filename: str):
    """Retorna la parametrizacion y el timestamp a partir del
    nombre del archivo wrfout
    CBA_A_18_TSK_2020-02-08T20:00
    """

    filename = filename.split('/')[-1]
    region, temp = filename.split('_', 1)
    param, temp = temp.split('_', 1)
    run, temp = temp.split('_', 1)
    var, timestamp = temp.split('_', 1)
    date = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M")

    return param, run, date, var


def getList(regex: str):
    return glob.glob(regex, recursive=True)


def getT2product(dfT2, dfTSK, awsname, param):
    """ Obtiene un pronostico de temperatura a partir de las variables
    T2 y TSK
    """
    mask = dfTSK['mean'].values - dfT2['mean'].values
    mask = mask > 0
    maskinverted = np.invert(mask)

    fieldname = f"T2P_{awsname}_{'param'}"
    dfT2 = dfT2.rename(columns={'mean': fieldname})
    dfTSK = dfTSK.rename(columns={'mean': fieldname})

    append = dfT2[mask].append(dfTSK[maskinverted], sort=True)
    append.sort_index(inplace=True)

    return append


def genT2P(target: str, param: str, run: str):
    # Open generated CSV
    # WRF_{param}_{run}_{target}_{var}_all.csv
    data_T0_file = f'csv/WRF_{param}_TSK_{run}.csv'
    data_T2_file = f'csv/WRF_{param}_T2_{run}.csv'
    data_T0 = pd.read_csv(data_T0_file, header=None)
    data_T2 = pd.read_csv(data_T2_file, header=None)

    data_T0["name"] = data_T0[1]
    data_T0["mean"] = data_T0[2]
    data_T0["date"] = pd.to_datetime(data_T0[3])
    data_T0 = data_T0[['name', 'mean', 'date']]
    data_T2["name"] = data_T2[1]
    data_T2["mean"] = data_T2[2]
    data_T2["date"] = pd.to_datetime(data_T2[3])
    data_T2 = data_T2[['name', 'mean', 'date']]

    # Get unique values of zones
    zonas = data_T0.name.unique()
    # select by zone
    for zona in zonas:
        zona_T0 = data_T0.loc[data_T0['name'] == zona]
        zona_T2 = data_T2.loc[data_T2['name'] == zona]

        zona_T0 = zona_T0.sort_values(by='date')
        zona_T2 = zona_T2.sort_values(by='date')

        data = getT2product(zona_T2, zona_T0, zona, 'T2P')
        file_out = f'csv/WRF_{param}_T2P_{run}.csv'
        data.to_csv(file_out, mode='a', header=None, encoding='utf-8')


def integrate_shapes(filename: str, shapefile: str,
                     target: str) -> gpd.GeoDataFrame:
    """
    This functions opens a geotiff with desired data, converts to a raster,
    integrate the data into polygons and returns a GeoDataFrame object.

    Parameters:
        cuencas_shp: Path to shapefile
    Returns:
        cuencas_gdf_ppn (GeoDataFrame): a geodataframe with cuerncas and ppn
    """

    cuencas_gdf: gpd.GeoDataFrame = gpd.read_file(shapefile, encoding='utf-8')
    df_zs = pd.DataFrame(zonal_stats(shapefile, filename, all_touched=True))

    cuencas_gdf_ppn = pd.concat([cuencas_gdf,
                                 df_zs], axis=1).dropna(subset=['mean'])
    if target == "zonas":
        COLUM_REPLACE = {'Name': 'zona'}
        cuencas_gdf_ppn = cuencas_gdf_ppn.rename(columns=COLUM_REPLACE)
        return cuencas_gdf_ppn[['zona', 'geometry', 'mean']]

    return None


@ray.remote
def zonalTransfor(filename: str, shapefile: str, target: str):
    param, run, date, var = getInfo(filename)
    if (var != 'T2' and var != 'TSK'):
        print(f"No processing: {var}")
        return
    else:
        print(f'Processing {filename}')

    zonas = pd.DataFrame()

    zonas_gdf = integrate_shapes(filename, shapefile, target)
    zonas_gdf = zonas_gdf[['zona', 'mean']]
    if var == 'TSK':
        zonas_gdf['mean'] = zonas_gdf['mean'] - 273.15
    zonas_gdf['date'] = date
    zonas = zonas.append(zonas_gdf, ignore_index=True)
    filename = f"csv/WRF_{param}_{var}_{run}.csv"
    logger.info(f"Saving {filename} - {time.time()}")
    zonas.to_csv(filename, mode='a', header=False, encoding='utf-8')


def getZones(filelist: list, shapefile: str, target: str):

    filelist.sort()

    it = ray.util.iter.from_items(filelist, num_shards=4)
    if target == "zonas":
        proc = [zonalTransfor.remote(filename, shapefile, target) for filename in it.gather_async()]
        ray.get(proc)

        param, run, date, var = getInfo(filelist[1])
        genT2P(target, param, run)


def wrfzonas(regex: str, shapefile: str, target: str):

    filelist = getList(regex)
    if not filelist:
        logger.critical("ERROR: No geotiff file matched in path")
        print("ERROR: No geotiff files matched")
        return
    getZones(filelist, shapefile, target)


def main():
    parser = argparse.ArgumentParser(
                description=('wrfzonas.py --path=geotiff/*.tiff '
                             '--shapefile=shapefiles/basisn.shp'),
                epilog="Process a list of rasters based on a shapefile")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with geoti", required=True)

    parser.add_argument("--target", type=str, dest="target",
                        help="zonas or basins", required=True)

    parser.add_argument("--shapefile", type=str, dest="shapefile",
                        help="if it's gfs or gefs", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    wrfzonas(args.path, args.shapefile, args.target)


if __name__ == "__main__":
    main()
