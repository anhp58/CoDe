import math as math
import numpy as np
from osgeo import gdal
import sys
import os
import os.path
from shutil import *
from utilities import *
from gdalconst import *
from geo_utilities import *

def Convert10to25Res(src_pxs, expand_xs, remain_xs, expand_pxs, remain_pxs):

    #if os.path.isfile(expand_pxs) and os.path.isfile(remain_pxs):
        #print "file exists: {0}, {1}".format(expand_pxs, remain_pxs)
        #return
    print "start converting expand/remain images from 10m to 2.5m..."
    # open expand/remain xs datasets
    expand_xs_set, expand_xs_band, expand_xs_data = Open1BandTif(expand_xs, GA_ReadOnly)
    remain_xs_set, remain_xs_band, remain_xs_data = Open1BandTif(remain_xs, GA_ReadOnly)
    print "ok"

    xs_geotransform = remain_xs_set.GetGeoTransform()
    xs_invgeotransform = gdal.InvGeoTransform(xs_geotransform)

    # copy and open pxs dataset
    copyfile(src_pxs, expand_pxs)
    copyfile(src_pxs, remain_pxs)

    pxs_remain_set, pxs_remain_band, pxs_remain_data = Open4BandTif(remain_pxs, GA_Update)
    pxs_extend_set, pxs_extend_band, pxs_extend_data = Open4BandTif(expand_pxs, GA_Update)

    pxs_geotransform = pxs_remain_set.GetGeoTransform()
    pxs_invgeotransform = gdal.InvGeoTransform(pxs_geotransform)
    nodata = pxs_remain_band[0].GetNoDataValue()

    # get X, Y size of pxs image
    YSize = pxs_remain_set.RasterYSize
    XSize = pxs_remain_set.RasterXSize

    YSizeXS = remain_xs_set.RasterYSize
    XSizeXS = remain_xs_set.RasterXSize
    # processing
    x = 0
    y = 0

    print "start matching..."
    for row in range(0,YSize):
        for col in range(0,XSize):
            # get corresponding pixel of xs image
            geo_x, geo_y = gdal.ApplyGeoTransform(pxs_geotransform, x, y)
            pixel, line = gdal.ApplyGeoTransform(xs_invgeotransform, geo_x, geo_y)

            if pixel < XSizeXS and line < YSizeXS:
                if expand_xs_data[int(line),int(pixel)] != 1:
                    pxs_extend_data[0][y,x] = nodata
                    pxs_extend_data[1][y,x] = nodata
                    pxs_extend_data[2][y,x] = nodata
                    pxs_extend_data[3][y,x] = nodata
        
                if remain_xs_data[int(line),int(pixel)] != 1 :
                    pxs_remain_data[0][y,x] = nodata
                    pxs_remain_data[1][y,x] = nodata
                    pxs_remain_data[2][y,x] = nodata
                    pxs_remain_data[3][y,x] = nodata

            x = x + 1
        x = 0
        y = y + 1
    
    for i in range(4):
        pxs_remain_band[i].WriteArray(pxs_remain_data[i])
        pxs_extend_band[i].WriteArray(pxs_extend_data[i])

    DelTif(expand_xs_data, expand_xs_band, expand_xs_set)
    DelTif(remain_xs_data, remain_xs_band, remain_xs_set)
    DelTif(pxs_remain_data, pxs_remain_band, pxs_remain_set)
    DelTif(pxs_extend_data, pxs_extend_band, pxs_extend_set)
if __name__ == '__main__':
    src_pxs = sys.argv[1]
    expand_xs = sys.argv[2]
    remain_xs = sys.argv[3]
    expand_pxs = sys.argv[4]
    remain_pxs = sys.argv[5]
    Convert10to25Res(src_pxs, expand_xs, remain_xs, expand_pxs, remain_pxs)







