PROG_VERSION = 'v0.1.0'

WRF_VARIABLES = ['T2', 'TSK', 'wspd10', 'wdir10']

WRFOUT_REGEX = r"wrfout_(?P<param>[A-Z])_[a-z0-9]{3,4}_(?P<timestamp>\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2})"

CBA_EXTENT = [-68.91031,
              -37.408794,
              -56.489685,
              -27.518177]

WRF_EXTENT = [-538001.0623448786,
              -538000.0000000792,
              537998.9376551214,
              537999.9999999208]

# recorte para graphs de ppnaccum
RECORTE_EXTENT = [-66.07031,
                  -35.168794,
                  -61.579685,
                  -29.328177]

KM_PER_DEGREE = 111.32
RESOLUTION = 4

SHAPE_ZONAS = ('/home/sagus/Development/temp/shapefiles/'
               'Epec/Zona_A_ET_Corbertura_20200922.shp')
