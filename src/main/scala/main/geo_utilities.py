import math as math
from osgeo import osr, gdal
from gdalconst import *
import ntpath
import os
import os.path
import re

LANDSAT8, LANDSAT7, LANDSAT5, LANDSAT4, SPOT4 = range(5)
SPOT_E = [1843, 1568, 1052, 233]

def step_range(start, end, step):
	while start < end:
		yield start
		start += step

def deg2rad(deg):
	return (deg * math.pi / 180)

def get_basename(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

# open one-band geotiff file read|write
def Open1BandTif(filename, flag):
        dset= None
        band = None
        data = None
        dset = gdal.Open(filename, flag)
        if dset is not None:
                band = dset.GetRasterBand(1)
                data = band.ReadAsArray()
        
        return dset, band, data

# update data in geotiff
def Update1Tif(band, data):
        if band is not None:
                band.WriteArray(data)

# open four-band geotiff file readonly
def Open4BandTif(filename, flag):
        dset = gdal.Open(filename, flag)
        band = []
        data = []
        if dset is not None:    
                # band[0,1,2,3] = [Blue,Green,Red,NIR]
                for i in range(4):
                        band.append(0)
                        data.append(0)
                        band[i] = dset.GetRasterBand(i+1)
                        data[i] = band[i].ReadAsArray()
                return dset, band, data
        return None, None, None

# update data in four-band geotiff
def Update4Tif(band, data):
        for i in range(0, 4):
                if band[i] is not None:
                        band[i].WriteArray(data[i])

# delete geotiff dataset
def DelTif(data, band, dset):
        if dset is not None:
                del data, band, dset

def utmToLatLng(zone, easting, northing, northernHemisphere=True):
    if not northernHemisphere:
        northing = 10000000 - northing

    a = 6378137
    e = 0.081819191
    e1sq = 0.006739497
    k0 = 0.9996

    arc = northing / k0
    mu = arc / (a * (1 - math.pow(e, 2) / 4.0 - 3 * math.pow(e, 4) / 64.0 - 5 * math.pow(e, 6) / 256.0))

    ei = (1 - math.pow((1 - e * e), (1 / 2.0))) / (1 + math.pow((1 - e * e), (1 / 2.0)))

    ca = 3 * ei / 2 - 27 * math.pow(ei, 3) / 32.0

    cb = 21 * math.pow(ei, 2) / 16 - 55 * math.pow(ei, 4) / 32
    cc = 151 * math.pow(ei, 3) / 96
    cd = 1097 * math.pow(ei, 4) / 512
    phi1 = mu + ca * math.sin(2 * mu) + cb * math.sin(4 * mu) + cc * math.sin(6 * mu) + cd * math.sin(8 * mu)

    n0 = a / math.pow((1 - math.pow((e * math.sin(phi1)), 2)), (1 / 2.0))

    r0 = a * (1 - e * e) / math.pow((1 - math.pow((e * math.sin(phi1)), 2)), (3 / 2.0))
    fact1 = n0 * math.tan(phi1) / r0

    _a1 = 500000 - easting
    dd0 = _a1 / (n0 * k0)
    fact2 = dd0 * dd0 / 2

    t0 = math.pow(math.tan(phi1), 2)
    Q0 = e1sq * math.pow(math.cos(phi1), 2)
    fact3 = (5 + 3 * t0 + 10 * Q0 - 4 * Q0 * Q0 - 9 * e1sq) * math.pow(dd0, 4) / 24

    fact4 = (61 + 90 * t0 + 298 * Q0 + 45 * t0 * t0 - 252 * e1sq - 3 * Q0 * Q0) * math.pow(dd0, 6) / 720

    lof1 = _a1 / (n0 * k0)
    lof2 = (1 + 2 * t0 + Q0) * math.pow(dd0, 3) / 6.0
    lof3 = (5 - 2 * Q0 + 28 * t0 - 3 * math.pow(Q0, 2) + 8 * e1sq + 24 * math.pow(t0, 2)) * math.pow(dd0, 5) / 120
    _a2 = (lof1 - lof2 + lof3) / math.cos(phi1)
    _a3 = _a2 * 180 / math.pi

    latitude = 180 * (phi1 - fact1 * (fact2 + fact3 + fact4)) / math.pi

    if not northernHemisphere:
        latitude = -latitude

    longitude = ((zone > 0) and (6 * zone - 183.0) or 3.0) - _a3

    return (latitude, longitude)

# get UTM zone
def GetUTMZone(image):
	ds=gdal.Open(image)
	prj=ds.GetProjection()

	srs=osr.SpatialReference(wkt=prj)
	if srs.IsProjected:
    		t = srs.GetAttrValue('projcs')
	t = t.split("/")[1]
	zone = int(re.search(r'\d+', t).group())
	return zone

# get coordinates of corners of geotiff image
def GetLatLon(image):
	dset, dband, ddata = Open1BandTif(image, GA_ReadOnly)
	XSize = dset.RasterXSize
	YSize = dset.RasterYSize
	geotransform = dset.GetGeoTransform()
	minx = geotransform[0]
	miny = geotransform[3] + XSize*geotransform[4] + YSize*geotransform[5]
	maxx = geotransform[0] + XSize*geotransform[1] + YSize*geotransform[2]
	maxy = geotransform[3]
	DelTif(ddata, dband, dset)
	zone = GetUTMZone(image)
	minx, maxy = utmToLatLng(zone, minx, maxy, northernHemisphere=True)
	maxx, miny = utmToLatLng(zone, maxx, miny, northernHemisphere=True)
	return minx, maxx, miny, maxy
        
