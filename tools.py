import osr, gdal
import numpy as np 
import os

class RasterHander():
    def __init__(self):
        pass

    # load raster file to memory
    def LoadRaster(self, rasterFile, bandNum=1):           
  
        # Open the file:
        dataset = gdal.Open(rasterFile)
        band = dataset.GetRasterBand(bandNum).ReadAsArray()

        geoTrans = dataset.GetGeoTransform()
        proj = dataset.GetProjection()
        return band, proj, geoTrans

    # write draster
    def SaveRaster(self, fileName, proj, geoTrans, data):

        # type
        if 'int8' in data.dtype.name:
            datatype = gdal.GDT_Byte
        elif 'int16' in data.dtype.name:
            datatype = gdal.GDT_UInt16
        else:
            datatype = gdal.GDT_Float32

        # check shape of array
        if len(data.shape) == 3:
            im_bands, im_height, im_width = data.shape
        else:
            im_bands, (im_height, im_width) = 1, data.shape 

        # create file
        driver = gdal.GetDriverByName("GTiff")
        dataset = driver.Create(fileName, im_width, im_height, im_bands, datatype)

        dataset.SetGeoTransform(geoTrans)
        dataset.SetProjection(proj)

        if im_bands == 1:
            dataset.GetRasterBand(1).WriteArray(data)
        else:
            for i in range(im_bands):
                dataset.GetRasterBand(i+1).WriteArray(data[i])

    # shp cut
    def ShpCut(self, shpFile, rasterFile, resultRaster):

        options = gdal.WarpOptions(cutlineDSName=shpFile, cropToCutline=True)
        outBand = gdal.Warp(srcDSOrSrcDSTab=rasterFile, destNameOrDestDS=resultRaster, options=options)
        outBand= None
    
    # resize img
    def Resize(self, rasterFile, resultTaster, xSize, ySize):
        gdal.Warp(resultTaster, rasterFile, xRes=xSize, yRes=ySize)

