import osr
import math as math
import numpy as np
import gdal
import sys
import os
import re

# get date from filename
def GetDateFromFName(filename):
        return int(re.search(r'\d+', filename).group())

# convert to date
def ConvertToTimestamp(filename):
	date = str(GetDateFromFName(filename))
	year = date[0:4]
	month = date[4:6]
	day = date[6:8]
	times = year + "-" + month + "-" + day
	return times

#print ConvertToTimestamp("VNR20140715.tif")
# create directory
def CreateDirectory(filepath):
	if not os.path.exists(filepath):
		os.makedirs(filepath)

#GetDateFromFName("oTOA_V20150117_XS.tif")           
# define cloud mask
def SetMaskName(basedir, orgname, suffix):
        cloud = basedir + orgname[:-4] + "_" + suffix + ".tif"
        return cloud


# distance from a point to a line
def DisPoint2Line(ax, by, c, x, y):
        nom = abs(ax*x + by*y + c)
        denom = math.sqrt(ax*ax + by*by)
        return nom/denom

# return maximum distance of (Blue,Nir) and (Red,Green)
def MaxDistance(B1, B2, B3, B4):
        b1 = B1 - B2
        b2 = B1 - B3
        n1 = B4 - B2
        n2 = B4 - B3

        band1 = B1
        band2 = B2
        
        maxp = b1
        if b2 > maxp:
                maxp = b2
                band1 = B1
                band2 = B3
        if n1 > maxp:
                maxp = n1
                band1 = B4
                band2 = B2
        if n2 > maxp:
                maxp = n2
                band1 = B4
                band2 = B3

        return band1/band2

# return min of three number
def Min(B1, B2, B3, B4):
        min = B1
        if B2 < min:
                min = B2
        if B3 < min:
                min = B3
        if B4 < min:
                min = B4
        return min

# return max of three number
def Max(B1, B2, B3, B4):
        max = B1
        if max < B2:
                max = B2
        if max < B3:
                max = B3
        if max < B4:
                max = B4
        return max

# Mean Absolute Error
def MeanAbEr3(B1, B2, B3):
        bright = float((B1+B2+B3)/3)
        return (abs(bright-B1)+abs(bright-B2)+abs(bright-B3))/3

# Mean Absolute Error
def MeanAbEr(B1, B2, B3, B4):
        bright = float((B1+B2+B3+B4)/4)
        return (abs(bright-B1)+abs(bright-B2)+abs(bright-B3)+abs(bright-B4))/4
# Brightness
def Brightness(B1, B2, B3, B4):
        return float((B1+B2+B3+B4)/4)
# Green/Red relationship
def DivisionIndex(B1, B2):
        return float(B1/B2)

# NDVI index
def NDIndex(B1, B2):
	return float((B1 - B2)/(B1 + B2))

def create_raster(output_filename, datal, cols, rows, band_type, geotransform, zone=49, hemisphere=1):

	driver = gdal.GetDriverByName('GTiff')
	number_of_bands = 1

	srs = osr.SpatialReference()
	srs.SetUTM(zone, hemisphere)
	srs.SetWellKnownGeogCS('WGS84')

	dataset = driver.Create(output_filename, cols, rows, number_of_bands, band_type)
	if dataset is None:
		print "Output dataset is none"
		exit(1)

	# write projection and geo transform metadata
	dataset.SetProjection(srs.ExportToWkt())
	dataset.SetGeoTransform(geotransform)

	band = dataset.GetRasterBand(1)

	# write data to file
	band.WriteArray(datal, 0, 0)
	band.SetNoDataValue(-9999)
	band.FlushCache()

	# clean up
	dataset = None

def value_at(self, x, y):
	"""x: line, y: pixel"""
	data = self.band.ReadAsArray(x, y, 1, 1)
	return float(data[0, 0])
	
def geo_to_pixel(self, geo_x, geo_y):
	"""geo_x: easting/long, geo_y:northing/lat, return: sample, line"""
	if self.dataset is None:
		print 'Couldnt open file'
		return -1, -1

	pixel, line = gdal.ApplyGeoTransform(self.invgeotransform, geo_x, geo_y)
		
	pixel = int(pixel)
	line = int(line)

	return pixel, line

def pixel_to_geo(self, pixel = 0, line = 0):
	"""geo_x: easting/long, geo_y:northing/lat"""
	if self.dataset is None:
		print 'Couldnt open file'
		return -1, -1

	geo_x, geo_y = gdal.ApplyGeoTransform(self.geotransform, pixel, line)

	return geo_x, geo_y

def step_range(start, end, step):
	while start < end:
		yield start
		start += step
