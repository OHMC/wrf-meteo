import requests
import argparse
import pandas as pd
from datetime import datetime
from config.constantes import base_url, Token, variables
from config.logging_conf import (METEO_LOGGER_NAME,
                                 get_logger_from_config_file)

logger = get_logger_from_config_file(METEO_LOGGER_NAME)


def get_config(filename: str):
    """ Retorna la parametrizacion, la variable y el RUN a partir del nombre del csv """

    filename = filename.split('/')[-1]
    wrf, temp = filename.split('_', 1)
    param, temp = temp.split('_', 1)
    var, run = temp.split('_', 1)

    return param, run, var


def buildList(wrf_var: pd.DataFrame, aws_zones: list, param: str, run: str, var: str):

    headers = {'Authorization': 'Token ' + Token}

    wrf_info = {'name': 'WRF', 'param': f'{param}', 'run': f'{run}',
                'version': '4.2',
                'grid_resolution': '[4000.0]x[4000.0]', 'global_model': 'GFS'}

    for aws in aws_zones:
        temp = wrf_var.loc[wrf_var['zona'] == aws['nombre']]
        temp = temp.sort_values('date')
        uid = aws['id']

        # AAAAMMDDTHHMMSSZ
        registers_list = []
        i = 0
        for line in temp.iterrows():
            dict = {'fecha': datetime.strftime(line[1]['date'], '%Y%m%dT%H%M%SZ'),
                    variables[var]: {'data': line[1]['T2P'], 'forecast': i, 'info': wrf_info}
                    }
            i = i + 1
            registers_list.append(dict)

        # post vía apirest
        json_ = {'id': uid, 'lista_registros': registers_list,
                 'usa_claves': True, 'replace_existing': True}

        response = requests.post(base_url+'datos/', headers=headers, json=json_)
        if response.ok:
            logger.info(f"POST RESPONSE: {response.json()}")
        else:
            logger.info(f"POST RESPONSE: {response.content}")


def getCsvVar(path: str, var: str):
    # 'name', 'mean', 'date'
    logger.info(f"Opening: {path}")
    wrf_var = pd.read_csv(path, header=None, encoding='utf-8')
    wrf_var[f'{var}'] = wrf_var[2]
    wrf_var['date'] = pd.to_datetime(wrf_var[2])
    wrf_var['zona'] = wrf_var[1]
    wrf_var = wrf_var[['date', f'{var}', 'zona']]

    return wrf_var


def getAWS_Zonal():
    # get full list
    headers = {'Authorization': 'Token ' + Token}
    url = base_url + 'estaciones/'
    response = requests.get(url, headers=headers).json()
    aws_list = response['aws_list']

    aws_zones = []
    for estacion in aws_list:
        if estacion['metadata']['red'] == 'EPEC':
            aws_zones.append(estacion)
        if estacion['id'] == '300000000000000000367':
            aws_zones.append(estacion)

    return aws_zones


def ingestor(path: str):
    param, run, var = get_config(path)
    aws_zones = getAWS_Zonal()
    wrf_var = getCsvVar(path, var)
    buildList(wrf_var, aws_zones, param, run, var)


def main():
    parser = argparse.ArgumentParser(
                description='ingestor.py --path=csv_var_file',
                epilog="Parse and POST to BBDHM API")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with csv to ingest", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    ingestor(args.path)


if __name__ == "__main__":
    main()
