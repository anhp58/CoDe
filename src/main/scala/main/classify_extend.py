import math as math
import numpy as np
from osgeo import gdal
import sys
import os
import os.path
from utilities import *
from shutil import *
from gdalconst import *
from sklearn import svm
from geo_utilities import *

def ClassificationExtend(imgb0, imgb1, imgb2, imgb3, changemask, outname, train, test):

    #if os.path.isfile(outname):
        #print "file exists: {0}".format(outname)
        #return
    print "start classification of extend part..."
    train_feature = []
    train_label = []
    train_data = np.loadtxt(train, delimiter = ",")
    for data in train_data:
        B1, B2, B3, B4, Class = float(data[0]), float(data[1]), float(data[2]), float(data[3]), float(data[4])
        train_feature.append([B1,B2,B3,B4])
        train_label.append(Class)

    cls = svm.SVC(kernel='rbf', C=524288, gamma=8)
    cls.fit(train_feature, train_label)

    test_feature = []
    test_label = []
    test_data = np.loadtxt(test, delimiter = ",")
    for data in test_data:
        B1, B2, B3, B4, Class = float(data[0]), float(data[1]), float(data[2]), float(data[3]), float(data[4])
        test_feature.append([B1,B2,B3,B4])
        test_label.append(Class)
    
    predict = cls.predict(test_feature)

    count = 0
    for i in range(0, len(test_label)):
        label = test_label[i]
        predict_label = predict[i]
        if label == predict_label:
            count = count + 1

    print "overall accuracy:", float((count*100)/len(test_label)), "%"

    copyfile(changemask, outname)
    # pxs_set, pxs_band, pxs_data = Open4BandTif(image, GA_ReadOnly)
    pxsb0_set, pxsb0_band, pxsb0_data = Open1BandTif(imgb0, GA_Update)
    pxsb1_set, pxsb1_band, pxsb1_data = Open1BandTif(imgb1, GA_Update)
    pxsb2_set, pxsb2_band, pxsb2_data = Open1BandTif(imgb2, GA_Update)
    pxsb3_set, pxsb3_band, pxsb3_data = Open1BandTif(imgb3, GA_Update)

    classify_set, classify_band, classify_data = Open1BandTif(outname, GA_Update)

    YSize = pxs_set.RasterYSize
    XSize = pxs_set.RasterXSize

    for y in range(0, YSize):
        for x in range(0, XSize):
            if pxsb0_data[y][x] > 0.0:
                b1 = pxsb0_data[y][x]
                b2 = pxsb1_data[y][x]
                b3 = pxsb2_data[y][x]
                b4 = pxsb3_data[y][x]
                predict_label = cls.predict([[b1,b2,b3,b4]])
                classify_data[y][x] = predict_label
            else:
                classify_data[y][x] = 3

    classify_band.WriteArray(classify_data)
    DelTif(pxs_data, pxs_band, pxs_set)
    DelTif(classify_data, classify_band, classify_set)

    if __name__ == '__main__':
        print "start classified processing"
        imgb0 = sys.argv[1]
        imgb1 = sys.argv[2]
        imgb2 = sys.argv[3]
        imgb3 = sys.argv[4]
        changemask = sys.argv[5]
        outname = sys.argv[6]
        train = sys.argv[7]
        test = sys.argv[8]
        ClassificationRemain(imgb0, imgb1, imgb2, imgb3, changemask, outname, train, test)

    




