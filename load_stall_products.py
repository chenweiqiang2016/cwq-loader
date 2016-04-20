# -*- coding: utf-8 -*-

"""
    将抓取的数据load进stall_products表
"""
import re
import codecs
import MySQLdb
import datetime
import ConfigParser

cf = ConfigParser.ConfigParser()
cf.readfp(open('load.cfg'))

def get_config(section, option):
    result = ''
    try:
        result = cf.get(section, option)
    except:
        print 'Cannot find [%s].%s in config file' %(section, option)
    return result

class Db:
    def __init__(self):
        self.connect = MySQLdb.connect(host=get_config("localhost", "host"),
                                       user=get_config("localhost", "user"),
                                       passwd=get_config("localhost", "passwd"),
                                       db=get_config("localhost", "db"),
                                       charset='utf8')
        self.cursor = self.connect.cursor()
        self.connect.autocommit(True)
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connect:
            self.connect.close()

class stall_product:
    def __init__(self, **args): 
        self.level1_category = args['level1_category']
        self.level2_category = args['level2_category']
        self.level3_category = args['level3_category']
        self.sku_id = args['sku_id']         
        self.new_id = args['new_id']         
        self.product_name = args['product_name']
        self.product_price = args['product_price']
        self.product_url = args['product_url']    
        self.img_url = args['img_url']        
        self.imgs = args['imgs']         
        self.size = args['size']           
        self.color = args['color']           
        self.on_shelf_time = args['on_shelf_time']  
        self.store_name = args['store_name']    
        self.store_url = args['store_url']   
        self.store_address = args['store_address']
        self.qq = args['qq']        
        self.telephone = args['telephone']       
        self.supplier_name = args['supplier_name'] 
        self.city = args['city']         
        self.reviews = args['reviews']
        
class Loader:
    def __init__(self):
        self.db = Db()
        self.stall_products = {}
    
    def run(self, filename):
        #重复代码
        pattern = "(\w+)_(\d{2}-\d{2}-\d{4})_productInfo.csv"
        merchant = re.findall(pattern, filename)[0][0]
        self.export_stall_products(merchant)
        self.load_file_data(filename)
        
    def export_stall_products(self, merchant):
        sql = """select level1_category, level2_category, level3_category, 
                        sku_id, new_id, product_name, product_price, 
                        product_url, img_url, imgs, size, color, 
                        on_shelf_time, store_name, store_url, store_address, 
                        qq, telephone, supplier_name, city, reviews, MOQ
                from stall_products where merchant=%s"""
        self.db.cursor.execute(sql, merchant)
        fields = ['level1_category', 'level2_category', 'level3_category', \
                  'sku_id', 'new_id', 'product_name', 'product_price',\
                  'product_url', 'img_url', 'imgs', 'size', 'color',\
                  'on_shelf_time', 'store_name', 'store_url', 'store_address',\
                  'qq', 'telephone', 'supplier_name', 'city', 'reviews', 'MOQ']
        for one_product in self.db.cursor.fetchall():
            datas = {}
            for index, key in enumerate(fields):
                datas[key] = one_product[index]
            self.stall_products[datas['new_id']] =datas
    
    def load_file_data(self, filename):
        pattern = "(\w+)_(\d{2}-\d{2}-\d{4})_productInfo.csv"
        merchant, date_str = re.findall(pattern, filename)[0]
        on_shelf_time = datetime.datetime.strptime(date_str, '%m-%d-%Y').date()
        fr = open(filename)
        first_line = fr.readline().replace('\n', '').strip()
        if first_line.startswith(codecs.BOM_UTF8):
            first_line = first_line[len(codecs.BOM_UTF8):]
        headers = first_line.split('\t')
        while True:
            line = fr.readline().strip()
            if not line:
                break
            fields = line.split('\t')
            datas = {'merchant': merchant, 'on_shelf_time': on_shelf_time}
            for index, header in enumerate(headers):
                datas[header] = fields[index]
            self.updateOrInsert(datas)
        self.db.close()
        
    def updateOrInsert(self, datas):
        sql = """insert into stall_products (merchant, level1_category, level2_category, level3_category,\
                 sku_id, new_id, product_name, product_price, product_url,\
                 img_url, imgs, size, color, MOQ, on_shelf_time, store_name,\
                 store_url, store_address, qq, telephone, supplier_name,\
                 city, reviews, add_time) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        if not self.stall_products.has_key(datas['sku_id']):
            self.db.cursor.execute(sql, self.format_datas(datas))
            self.stall_products[datas['sku_id']] = {} #简单加入进去
        else:
            #应该进行update操作
            pass
        
    def format_datas(self, datas):
        merchant = datas['merchant']
        level1_category = self.format_data(datas, ['level1_category', 'category'])
        level2_category = self.format_data(datas, ['level2_category'])
        level3_category =self.format_data(datas, ['level3_category'])
        sku_id = self.format_data(datas, ['sku_id'])
        new_id = self.format_data(datas, ['sku_id'])
        product_name = self.format_data(datas, ['product_name', 'name'])
        product_price = self.format_data(datas, ['product_price', 'price'])
        product_url = self.format_data(datas, ['product_url', 'url'])
        img_url = self.format_data(datas, ['img_url', 'img'])
        imgs = self.format_data(datas, ['img_urls', 'imgs'])
        size = self.format_data(datas,['size'])
        color = self.format_data(datas,['color'])
        MOQ = self.format_data(datas, ['MOQ'])
        if datas.has_key('update_time'): #为vvic服务
            update_time = datas['update_time']
            month, year = re.findall('\d+', update_time)[0]
        else:
            on_shelf_time = datas['on_shelf_time']
        store_name = self.format_data(datas, ['store_name', 'merchant_name', 'merchant'])
        store_url =  self.format_data(datas, ['store_url', 'merchant_url'])
        store_address = self.format_data(datas,['store_address'])
        qq = self.format_data(datas, ['qq'])
        telephone = self.format_data(datas,['telephone'])
        supplier_name = self.format_data(datas,['supplier_name'])
        city = self.format_data(datas,['city'])
        reviews = int(datas['reviews'])
        return (merchant, level1_category, level2_category, level3_category, sku_id, new_id, product_name, product_price, product_url, img_url, imgs, size, color, MOQ, on_shelf_time, store_name, store_url, store_address, qq, telephone, supplier_name, city, reviews, datetime.datetime.now())
    
    def format_data(self, datas, keyList):
        result = None
        try:
            for key in keyList:
                result = datas[key]
                if result is not None:
                    break
        except:
            result = ''
        return result
                
if __name__ == '__main__':
    loader = Loader()
    loader.run("1688_04-14-2016_productInfo.csv")
    