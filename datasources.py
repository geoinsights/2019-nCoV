#coding=utf-8
import json
import re
import requests
from bs4 import BeautifulSoup
import csv
import time

def data_from_dxy():
    r = requests.get('https://3g.dxy.cn/newh5/view/pneumonia')
    r.encoding = 'utf-8'
    html = r.text
    soup = BeautifulSoup(html)
    getAreaStat = soup.find("script", {"id": "getAreaStat"}).text
    i = re.search('getAreaStat = ',getAreaStat).end()
    j = re.search('catch\(e\)',getAreaStat).start()
    data = getAreaStat[i:j-1]
    china = json.loads(data)


    global_data = soup.find("script", {"id": "getListByCountryTypeService2"}).text
    i = re.search('getListByCountryTypeService2 = ',global_data).end()
    j = re.search('catch\(e\)',global_data).start()
    global_data = global_data[i:j-1]
    global_data = json.loads(global_data)
    data = {'china':china, 'global':global_data}
    return data

def geocoder_tdt(placename):
    tdturl = '''http://api.tianditu.gov.cn/geocoder?ds={{"keyWord":"{0}"}}&tk=66898f5d0348468eb7c9432c47958c6a'''
    r = requests.get(tdturl.format(placename))
    locjson = json.loads(r.text)
    lon, lat = locjson['location']['lon'], locjson['location']['lat']
    return lon, lat

def geocoder_geonames(placename):
    url = '''http://api.geonames.org/searchJSON?q={}&maxRows=10&username=verusloo'''
    r = requests.get(url.format(placename))
    locjson = json.loads(r.text)
    #print(locjson)
    lonlat = locjson['geonames'][0]['lng'], locjson['geonames'][0]['lat']
    return lonlat
    
def geocoder_nominatim(placename):
    url = '''https://nominatim.openstreetmap.org/?addressdetails=1&q={}&format=json&limit=1'''
    r = requests.get(url.format(placename))
    locjson = json.loads(r.text)
    #print(locjson)
    lonlat = locjson[0]['lon'], locjson[0]['lat']
    return lonlat

def load_data():
    data = {}
    with open('2019ncov_city.csv') as ifile:
        reader = csv.DictReader(ifile)
        for row in reader:
            placename = row['province']+row['city']
            data[placename] = row
    return data

def update_data():
    data = load_data()
    new_data = data_from_dxy()
    new_china = new_data['china']
    new_global = new_data['global']
    province_data = []
    city_data = []
    city_external = []
    global_data = []
    for dt in new_china:
        province = dt['provinceName']
        print(province)
        if province == '待明确地区':
            continue
        pconf = dt['confirmedCount']
        psusp = dt['suspectedCount']
        pcured = dt['curedCount']
        pdead = dt['deadCount']
        
        plonlat = None
        while plonlat is None:
            try:
                plonlat = geocoder_tdt(province)
            except:
                plonlat = None
        pdata = '{},{},{},{},{},{},{}'.format(province, pconf, psusp, pcured, pdead, plonlat[0], plonlat[1])
        print(pdata)
        province_data.append({'province':province, 'confirmed':pconf, 'suspected':psusp, 'cured':pcured, 'dead':pdead, 'lon':plonlat[0], 'lat':plonlat[1]})
        for ct in dt['cities']:
            city = ct['cityName']
            #print(city)
            placename = province + city
            if placename in data:
                data[placename].update({'confirmed':ct['confirmedCount'],
                                        'suspected':ct['suspectedCount'],
                                        'cured':ct['curedCount'],
                                        'dead':ct['deadCount']})
            else:
                if city.startswith('外地') or city.startswith('未') or city.startswith('待'):
                    city_external.append({  'province':province, 
                                            'city':city,
                                            'confirmed':ct['confirmedCount'],
                                            'suspected':ct['suspectedCount'],
                                            'cured':ct['curedCount'],
                                            'dead':ct['deadCount']})
                    continue
                


                print(u'新增城市:'+placename)
                lonlat = None
                while lonlat is None:
                    try:
                        lonlat = geocoder_tdt(placename)
                    except:
                        lonlat = None
                data[placename] = { 'province':province, 
                                    'city':city,
                                    'confirmed':ct['confirmedCount'],
                                    'suspected':ct['suspectedCount'],
                                    'cured':ct['curedCount'],
                                    'dead':ct['deadCount'],
                                    'lon':lonlat[0],
                                    'lat':lonlat[1]}

            city_data.append(data[placename])

    for gdt in new_global:
        countryname = gdt['provinceName']
        if countryname =='阿联酋':
            countryname = '阿拉伯联合酋长国'
        lonlat = None
        while lonlat is None:
            try:
                lonlat = geocoder_nominatim(countryname)
            except:
                lonlat = None
        print(countryname, lonlat)
        gdt['lon'], gdt['lat'] = lonlat
        dit = {
            'continent':gdt['continents'], 
            'country':gdt['provinceName'],
            'confirmed':gdt['confirmedCount'],
            'suspected':gdt['suspectedCount'],
            'cured':gdt['curedCount'],
            'dead':gdt['deadCount'],
            'lon':lonlat[0],
            'lat':lonlat[1]
        }
        global_data.append(dit)

    print('write out to csv....')
    timestamp = time.strftime('%Y-%m-%d-%H',time.localtime())
    with open('data/2019ncov_province_{}.csv'.format(timestamp),'w') as ofile:
        writer = csv.DictWriter(ofile, list(province_data[0].keys()))
        writer.writeheader()
        writer.writerows(province_data)

    with open('data/2019ncov_city_{}.csv'.format(timestamp),'w') as ofile:
        writer = csv.DictWriter(ofile, list(city_data[0].keys()))
        writer.writeheader()
        writer.writerows(city_data)

    with open('data/2019ncov_city_unknown_{}.csv'.format(timestamp),'w') as ofile:
        writer = csv.DictWriter(ofile, list(city_external[0].keys()))
        writer.writeheader()
        writer.writerows(city_external)

    with open('data/2019ncov_global_{}.csv'.format(timestamp),'w') as ofile:
        writer = csv.DictWriter(ofile, list(global_data[0].keys()))
        writer.writeheader()
        writer.writerows(global_data)

    with open('2019ncov_city.csv','w') as ofile:
        writer = csv.DictWriter(ofile, list(city_data[0].keys()))
        writer.writeheader()
        writer.writerows(city_data)

    with open('2019ncov_province.csv','w') as ofile:
        writer = csv.DictWriter(ofile, list(province_data[0].keys()))
        writer.writeheader()
        writer.writerows(province_data)

    with open('2019ncov_global.csv','w') as ofile:
        writer = csv.DictWriter(ofile, list(global_data[0].keys()))
        writer.writeheader()
        writer.writerows(global_data)

from osgeo import ogr
def update_area(csvfn, gjsonfn):
    global_pt = []
    with open(csvfn) as ifile:
        reader = csv.DictReader(ifile)
        for row in reader:
            global_pt.append(row)

    ds = ogr.Open(gjsonfn,1)
    lyr = ds.GetLayerByIndex(0)
    for gpt in global_pt:
        pt = ogr.Geometry(ogr.wkbPoint)
        pt.AddPoint(float(gpt['lon']), float(gpt['lat']))
        mindis = 9999999999
        toupdate = None 
        lyr.ResetReading()
        for feat in lyr:
            geom = feat.GetGeometryRef()
            dis = geom.Distance(pt)
            if dis < mindis:
                toupdate = feat
                mindis = dis
        
        toupdate.SetField('confirmed', gpt['confirmed'])
        toupdate.SetField('suspected', gpt['suspected'])
        toupdate.SetField('cured', gpt['cured'])
        toupdate.SetField('dead', gpt['dead'])
        #print(gpt['country'], gpt['confirmed'])
        lyr.SetFeature(toupdate)
        
    ds = None

if __name__ == '__main__':
    update_data()
    '''
    gdata = data_from_dxy()['global']
    for gd in gdata:
        print(gd['provinceName'],
        geocoder_nominatim(gd['provinceName']))'''

    update_area('2019ncov_province.csv', 'geo/provinces.geojson')
    update_area('2019ncov_global.csv', 'geo/world.geojson')
    #print(geocoder_nominatim('菲律宾'))
    
            

    