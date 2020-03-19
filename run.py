import os
import json
from urllib.parse import quote

import netCDF4
from netCDF4 import Dataset
import osr, gdal
import numpy as np
import datetime, time
from dateutil.parser import parse

from tools import RasterHander
from TaskTimer import TaskTimer

# this this is used to clean some bad files, record process date
class AnalysisConfig():

    def __init__(self, inFile="config.json"):
        with open(inFile, 'r', encoding='utf8') as fp:
            self.configJson = json.load(fp)
            #j = 0
            self.HandTxt()

    def HandTxt(self):
        if os.path.exists('time.txt'):
            os.remove('time.txt')
        
        if os.path.exists('badDataset.txt'):
            with open('badDataset.txt', 'r', encoding='utf8') as fp:
                for line in fp.readlines():
                    if os.path.exists(line):
                        os.remove(line)        
            os.remove('badDataset.txt')

# this class performs query, download and processing
class QueryP5(RasterHander):

    def __init__(self, inFile='config.json'):
        with open(inFile, 'r', encoding='utf8') as fp:
            self.config = json.load(fp)

            if self.config['endPosition'] == '*':

                newBeginTime = self.config['beginPosition']
                newEndTime = parse(newBeginTime) + datetime.timedelta(seconds = (int(86400*float(self.config['runningT']))))
                self.config['beginPosition'] = newBeginTime + ' TO ' + newEndTime.strftime('%Y-%m-%dT%H:%M:%S') + '.000Z'
                self.config['endPosition'] = self.config['beginPosition']
                os.chdir(self.config['savePath'])

                if not os.path.exists('time.txt'):
                    with open('time.txt', 'w', encoding='utf8') as f:
                        f.write(self.config['beginPosition'])
                else:
                    with open('time.txt', 'r', encoding='utf8') as f:
                        #self.config['beginPosition'] = 

                        newBeginTime = f.read().split()[2]
                        newEndTime = parse(newBeginTime) + datetime.timedelta(seconds = (int(86400*float(self.config['runningT']))))
                        self.config['beginPosition'] = newBeginTime + ' TO ' + newEndTime.strftime('%Y-%m-%dT%H:%M:%S')+ '.000Z'
                        self.config['endPosition'] = self.config['beginPosition']
                        print(self.config['beginPosition'])
                        with open('time.txt', 'w', encoding='utf8') as f:
                            f.write(self.config['beginPosition'])
            else:
                self.config['beginPosition'] = self.config['beginPosition'] + ' TO ' + self.config['endPosition']
                self.config['endPosition'] = self.config['beginPosition']

    def HandleDir(self, dir):
        if (not os.path.exists(dir)):
            os.mkdir(dir)    

    def ReadNC(self, fileName, datasetName, x = -1, y = -1):
        nc_obj = Dataset(fileName)        
        data = np.squeeze(np.array(nc_obj.groups['PRODUCT'][datasetName]))
        nc_obj.close()
        if x == -1 and y == -1:
            return data
        else:
            return data[:, x, y]

    def GeoLocation(self, fileName, geoFile, fillValue = -9999999):
        
        if (os.path.exists(geoFile)) and (os.path.getsize(geoFile) > 10):
            return

        #fileName = r'C:\Users\DF\Desktop\S5P_NRTI_L2__NO2____20200229T235605_20200301T000105_12337_01_010302_20200301T004406.nc'
        #self.geoDataName = 'nitrogendioxide_tropospheric_column'
        data = self.ReadNC(fileName, self.geoDataName)
        lat = self.ReadNC(fileName, 'latitude')
        lon = self.ReadNC(fileName, 'longitude')
        #data[data > 100000] = 0
        #data *= 1e6

        res = 7.0/100
        transform = [np.min(lon), res, 0, np.max(lat), 0, -res]
        self.cols = int((np.max(lon) - np.min(lon) ) / res) + 1
        self.rows = int((np.max(lat) - np.min(lat)) / res) + 1
        self.raster = np.zeros( (self.rows, self.cols) )
        self.raster[:, :] = fillValue
        y, x = (-transform[0] + lon) / transform[1],  (-transform[3] + lat) / transform[5]
        self.raster[x.astype('int'), y.astype('int')] = data

        spei_ds = gdal.GetDriverByName('Gtiff').Create(geoFile, self.cols, self.rows,1,gdal.GDT_Float32, options=['COMPRESS=LZW']) 
        spei_ds.SetGeoTransform(transform)

        srs = osr.SpatialReference() 
        srs.ImportFromEPSG(4326) 
        spei_ds.SetProjection(srs.ExportToWkt()) 

        spei_ds.GetRasterBand(1).WriteArray(self.raster.astype('float32'))
        spei_ds.FlushCache() 
        spei_ds = None 

    def Remove(self, fileName):
        if os.path.exists(fileName):
            try:
                os.remove(fileName)
            except Exception as e:
                print('bad delete')            

    def RemoveFile(self, fileName):
        if isinstance(fileName, list):
            for ii in fileName:
                self.Remove(ii)
        else:
            self.Remove(fileName)

    def Fill3(self, fileName, resultRaster, fillValue = -9999999):
        band, proj, geoTrans = self.LoadRaster(fileName)
        
        rows = band.shape[0]
        cols = band.shape[1]
        step = 5
        for row in range(0, rows):
            for col in range(0, cols):
                if band[row, col] == fillValue:

                    rowBegin, rowEnd = row - step, row + step
                    colBegin, colEnd = col - step, col + step
                    if rowBegin < 0: rowBegin = 0
                    if colBegin < 0: colBegin = 0
                    if rowEnd > rows: rowEnd = rows
                    if colEnd > cols: colEnd = cols
                    bTmp = band[rowBegin:rowEnd, colBegin: colEnd]

                    bTmp[bTmp > 9999999] = fillValue
                    bTmp = bTmp[bTmp != fillValue]

                    if bTmp.size != 0:
                        band[row, col] = np.mean(bTmp)
        band[band > 9999999] = fillValue
        band[band != fillValue] = band[band != fillValue] * 1e6
        self.SaveRaster(resultRaster, proj, geoTrans, band)

    def DownloadProcess(self, saveDir, fillValue = -9999999):
        with open('query_results.txt', 'r', encoding='utf8') as fp:
            j = json.load(fp)

            if j['feed']['opensearch:totalResults'] == '0': return                

            self.dateStr = {}       
            #mosaicList = []  # get nc from server            
            for entry in j['feed']['entry']:

                url = entry['link'][0]['href']
                fileName = saveDir + os.sep + entry['title'] + '.nc'
                geoFile = saveDir + os.sep + entry['title'] + '.tif'

                dateStr = entry['date'][1]['content'][0:10]
                if dateStr not in self.dateStr:
                    self.dateStr[dateStr] = []
                    self.dateStr[dateStr].append(geoFile)
                else:
                    self.dateStr[dateStr].append(geoFile)

                self.removeFileList.append(geoFile)
                self.removeFileList.append(fileName)
                com = 'wget --content-disposition --continue --user=' + self.config['user'] +  ' --password=' + self.config['password'] + ' -P '+ saveDir + ' -O ' + fileName + ' ' + url

                if (not os.path.exists(fileName)) or  (os.path.exists(fileName) and (os.path.getsize(fileName) < 10)):
                    try:
                        ret = os.system(com)
                    except Exception as e:
                        with open('badDataset.txt', 'w', encoding='utf8') as fp:
                            fp.write(fileName + '\n')

                # process                
                try:
                    self.GeoLocation(fileName, geoFile)
                except Exception as e:
                    with open('badDataset', 'w', encoding='utf8') as fp:
                        fp.write(fileName + '\n')
            
            for key in self.dateStr:
                dateStr = key
                mosaicList = self.dateStr[key]
                # mosaic and subsize and add 1e6
                with open('mosaic.txt', 'w', encoding='utf8') as fp:
                    for strLine in mosaicList:
                        fp.write(strLine + '\n')
                
                # run mosaic            
                dailyFile = saveDir + os.sep + self.producttype + dateStr + '_mosaic.tif'
                self.removeFileList.append(dailyFile)
                com = 'python gdal_merge.py -o ' + dailyFile + ' -q -v --optfile mosaic.txt  -co COMPRESS=LZW -n ' + str(fillValue) + ' -init ' + str(fillValue) + ' -a_nodata ' + str(fillValue)
                try:
                    ret = os.system(com)
                except Exception as e:
                    ret = os.system(com)
                
                # run subset
                tmpRaster = saveDir + os.sep + self.producttype + dateStr + '_subset.tif'
                self.ShpCut('./shp/china.shp', dailyFile, tmpRaster)
                self.removeFileList.append(tmpRaster)

                # run fill
                fillRaster = saveDir + os.sep + self.producttype + dateStr + '_fill.tif'          
                self.Fill3(tmpRaster, fillRaster, fillValue)
                self.removeFileList.append(fillRaster)

                tmpRaster = saveDir + os.sep + self.producttype + dateStr + '.tif'
                self.ShpCut('./shp/china.shp', fillRaster, tmpRaster, fillValue)            

    def QueryOne(self, url = 'https://s5phub.copernicus.eu/dhus/search?q='):

        exeName = 'wget.exe'

        if os.path.exists('query_results.txt'):
            os.remove('query_results.txt')
        
        self.removeFileList = []

        for dataset in self.config['dataset']:
            #producttype = dataset['producttype']
            self.producttype = dataset['producttype']
            self.geoDataName = dataset['geoDataName']
            urlR = url + '(beginPosition:[' + self.config['beginPosition'] + '] AND endPosition:[' + \
              self.config['endPosition'] + ']) AND (platformname:' +     self.config['platformname'] + \
               ' AND producttype:' + self.producttype + ' AND processinglevel:' + \
                self.config['processinglevel'] + ') AND (footprint:\\\"' + self.config['footprint'] + \
                 '\\\")&format=' + self.config['format'] + '&rows=' + str(self.config['row'])

            print(urlR)

            com = ' --no-check-certificate --user=' + self.config['user'] + ' --password='+ self.config['password'] +' --output-document=query_results.txt'
            com = exeName + ' ' + com + ' "' + urlR + '"'
            
            try:
                ret = os.system(com)
            except Exception as e:
                ret = os.system(com)       

            # check dir
            saveDir = self.config['savePath'] + os.sep + self.producttype
            self.HandleDir(saveDir)

            # download and process
            try:
                self.DownloadProcess(saveDir)
            except Exception as e:
                self.DownloadProcess(saveDir)   

        # clean
        self.RemoveFile(self.removeFileList)

def func():
    try:
        query = QueryP5()
        query.QueryOne()
    finally:
        return

# clean some bad files, record process date
ana = AnalysisConfig()
itrval = float(ana.configJson['runningT']) * 86400

# do query and process
if ana.configJson['endPosition'] == '*':
    query = QueryP5()   # forever mode
    query.QueryOne()

    timer = TaskTimer()
    timer.join_task(func, [], interval = itrval)
    timer.start()
else:
    query = QueryP5()   # once mode
    query.QueryOne()