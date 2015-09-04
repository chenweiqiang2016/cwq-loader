# -*- coding: utf-8 -*-

import lockfile
import sys
import ConfigParser
import logging
import MySQLdb
import re
import datetime
import os
import glob
import time
import codecs
import json
 

config = ConfigParser.ConfigParser()
fr = open("load.cfg")
config.readfp(fr)

def get_config(section, option):
    result = ""
    try:
        result = config.get(section, option)
    except:
        logging.debug("Cannot find [%s].%s in config file" %(section, option))
    return result;

data_dir = get_config("all", "data.dir")
save_dir = get_config("all", "save.dir")


def get_file_path(path, filename):
    return os.path.join(path, filename)

FILE_RE_PATTERN = "*_productInfo.csv"
CAPTURE_RE_PATTERN = "([\w-]+)_(\d\d)-(\d\d)-(\d\d\d\d)_productInfo.csv"


class Db:
    def __init__(self, SSCURSOR_MODE=False):
        if SSCURSOR_MODE:
            self.conn = MySQLdb.connect(host=get_config("db", "host"),
                                    user=get_config("db", "user"),
                                    passwd=get_config("db", "passwd"),
                                    cursorclass = MySQLdb.cursors.SSCursor);
        else:
            self.conn = MySQLdb.connect(host=get_config("db", "host"),
                                        user=get_config("db", "user"),
                                        passwd=get_config("db", "passwd"))
        self.conn.select_db("aims")
        self.conn.autocommit(True)
        self.cursor = self.conn.cursor()
        

class Merchant:
    def __init__(self, mId, name, scoring_fields):
        self.mId = mId
        self.name = name
        self.scoringFields = scoring_fields.split(",")
        
    def __str__(self):
        return "%s(%s)%s" %(self.name, self.mId, self.scoringFields)
    
    __repr__ = __str__

date_re_str = "(\d\d)-(\d\d)-(\d\d\d\d)" 

class Loader:
    def __init__(self):
        self.merchants = {}
        self.loadMerchantsFromDb(); #对merchants进行赋值
        self.load_records = {}
        self.loadRecordsFromConfig(); #对load_records进行赋值
        self.files_distribution = {"outOfDate": [], "successfulLoaded":[], "failLoaded":[]}
 
    def loadMerchantsFromDb(self):
        db = Db();
        sql = """select id, name, scoring_fields from merchants"""
        db.cursor.execute(sql)
        while True:
            row = db.cursor.fetchone()
            if not row:
                break
            merchant = Merchant(row[0], row[1], row[2])
            self.merchants[merchant.name] = merchant
        logging.debug("%d merchants of basic info loaded from Db..." %len(self.merchants))
    
    def loadRecordsFromConfig(self):
        for merchantName in self.merchants.iterkeys():
            dateStr = get_config("loadrecords", merchantName)
            dateObj = self.parseDate(dateStr)
            if dateObj:
                self.load_records[merchantName] = dateObj
        logging.warning("%d merchants load records got from config file..." %len(self.load_records))

    def parseDate(self, str1):
        result = re.findall(date_re_str, str1)
        if result:
            return datetime.date(int(result[0][2]), int(result[0][0]), int(result[0][1]))
        
    def run(self):
        captures = self.find_new_captures()
        for capture in captures:
#             try:      
            captureLoader = CaptureLoader(capture)
            captureLoader.load()
            self.files_distribution["successfulLoaded"].append(capture.filename)
#             except Exception, e:
#                 print e
#                 logging.error("exception while loading file %s" %capture.filename)
#                 print "exception while loading file %s" %capture.filename
#                 self.files_distribution["failloaded"].append(capture.filename)
        output = """file process results:
        %s: %s
        %s: %s
        %s: %s
        """ \
        %("successfulLoaded", self.files_distribution["successfulLoaded"],
          "failLoaded", self.files_distribution["failLoaded"],
          "outOfDate", self.files_distribution["outOfDate"])
        print output
        

    def find_new_captures(self):
        full_path_pattern = get_file_path(data_dir, FILE_RE_PATTERN)
        fileList = glob.glob(full_path_pattern)
        result = []
        for file in fileList:
            capture = self.parseCapture(file)
            if not capture:
                self.files_distribution["failLoaded"].append(file) #文件名不符合规范
            elif not self.isMerchantSupported(capture.merchantName):
                logging.warning("merchant '%s' has not been add into aims.merchants..." %capture.merchantName)
                print "merchant '%s' has not been added into aims.merchants..." %capture.merchantName
                self.files_distribution["failLoaded"].append(capture.filename) #商户没有添加
            elif not self.isNewCapture(capture):
                self.files_distribution["outOfDate"].append(capture.filename)
            else:
                capture.merchant = self.merchants.get(capture.merchantName)
                result.append(capture)
        return result  
    
    def parseCapture(self, filename):
        result = re.findall(CAPTURE_RE_PATTERN, filename)
        if not result:
            logging.warning("filename '%s' failed to parse capture info!" % filename)
            return
        capture = Capture(filename, result[0][0], datetime.date(int(result[0][3]), int(result[0][1]), int(result[0][2])))
        return capture
    
    def isMerchantSupported(self, merchantName):
        if self.merchants.has_key(merchantName):
            return True
        return False
    
    def isNewCapture(self, capture):
        if self.load_records.has_key(capture.merchantName):
            latest_date = self.load_records.get(capture.merchantName)
            return (capture.date - latest_date).days > 0
        return True #没有任何记录 就认为是True
 
class Capture: #写成了中文冒号
    def __init__(self, filename, merchantName, date):
        self.filename = filename
        self.merchantName = merchantName
        self.date = date
        self.merchant = None #存放一个merchant对象
    
    def get_data_file(self):
        return self.filename

class CaptureLoader:
    def __init__(self, capture):
        self.capture = capture
        
    def load(self):
        self.db = Db()
#         try:
        self.initProductParser()
            
        self.record_captures() #在captures表中插入最新的记录
        
#         self.load_product_cache()
#         
        self.load_category_cache()
#         
        self.load_categories() #从文件中读出所有品类
#         
        self.load_products() #从文件中读出所有商品
#         except Exception, e:
#             print e
    
    def initProductParser(self):
        filename = self.capture.get_data_file()
        try:
            fr = open(filename, "r")
            first_line = fr.readline()
            self.fileds = self.read_headers(first_line)
        except Exception, e:
            logging.error(e)
        finally:
            fr.close()
    
    def read_headers(self, line):
        if line.startswith(codecs.BOM_UTF8):
            line = line[len(codecs.BOM_UTF8):]
        return line.split('\t')
        
    def record_captures(self):
        
        sql = """ select count(1) from captures 
              where merchant_id = '%s' and capture_date = '%s'
              """
        self.db.cursor.execute(sql %(self.capture.merchant.mId, self.capture.date))
        row = self.db.cursor.fetchone()
        if row[0]== 0:
            self.db.cursor.execute("""insert into captures (merchant_id, capture_date, status) values('%s', '%s', 0)""" %(self.capture.merchant.mId, self.capture.date))
    
    def load_product_cache(self):
        db = Db(SSCURSOR_MODE=True)
        self.productCache = ProductCache("sku_id") #self.getKeyField()
        sql = """select id, name, merchant_id, category_id, sku_id, url,
        img_url, price, reviews, sell_status, sellstart_date, latest_capture_date
        from products where merchant_id='%s' and ct_status>=0
        """
        db.cursor.execute(sql %(self.capture.merchant.mId,))
        while True:
            row = db.cursor.fetchone()
            if not row:
                break
            print row
            values = {
                      "id": row[0],
                      "name": row[1],
                      "merchant_id": row[2],
                      "category_id": row[3],
                      "sku_id": row[4],
                      "url": row[5],
                      "img_url": row[6],
                      "price": row[7],
                      "reviews": row[8],
                      "sell_status": row[9],
                      "sellstart_date": str(row[10]),
                      "latest_capture_date": str(row[11])}
            keyIndex = 4 if "sku_id" else 5
            if self.productCache.cache.has_key(row[keyIndex]):
#                 print "aaaaaaaaaaaa"
                print json.loads(self.productCache.cache.get(row[keyIndex])).get("id"), values.get("id")
            else:
                values_str = json.dumps(values)
#                 print values_str
                self.productCache.cache[row[keyIndex]] = values_str
                print len(self.productCache.cache)
        
    
    def load_category_cache(self):
        self.categoryCache = CategoryCache()
        sql="""
        select id, name, level, parent_id from categories where merchant_id=%s order by level, parent_id
        """
        self.db.cursor.execute(sql, self.capture.merchant.mId)
        while True:
            row = self.db.cursor.fetchone()
            if not row:
                break
            category = Category(row[0], row[1], row[2])
            parentCategoryId = row[3]
            self.categoryCache.cache_category(category, parentCategoryId)
    
    def load_categories(self):
        pass

    
    def load_products(self):
        pass
    
#     def getScoringField(self):
#         if self.capture.merchant.scoringFields==["likes"] or "reviews" not in self.fields:
#             index =  self.fields.index("likes")
#             self.fields[index] = "reviews"
#             print "scoring field is: like, store as reviews"
    
    def getKeyField(self):
        return "sku_id" if "sku_id" in self.fields else "product_url"
    
class ProductParser:
    def __init__(self, fields):
        self.fields = fields
    
    def parse(self, line):
        line = line.strip()
        values = line.split("\t")
        if not len(values) == len(self.fields):
            return
        args = {}
        for i, value in enumerate(values):
            args[self.fields[i]] = value
        return Product(**args)
    
class ProductCache:
    def __init__(self, cacheType="product_url"):
        self.cacheType = cacheType
        self.cache = {} #TODO: 改为使用kyotocabinet
    
    def put(self):
        pass
        

class CategoryCache:
    def __init__(self):
        self.rootCategory = Category(0, "", 0)
        self.cache = {0: self.rootCategory}
    
    def cache_category(self, category, parent_category_id):
        parent_category = self.cache.get(parent_category_id)
        parent_category.add_child(category) #这是引用, 操作对字典里的原数据会起作用
        category.parent = parent_category
        self.cache[category.cId] = category
        

class Product:
    def __init__(self, **args):
        pass

class Category:
    def __init__(self, cId, name, level):
        self.cId = cId
        self.name = name
        self.level = level
        self.parent = None
        self.child = []
        
    def __str__(self):
        return "%s-->%s(%s)-->%s" %(self.parent, self.name, self.cId, self.child)

    def add_child(self, category):
        self.child.append(category)

def main():
    lock = lockfile.FileLock("load")
    try:
#         lock.acquire(10)
#         try:
        loader = Loader()
        loader.run()
#         except:
#             print >> sys.stderr, "Unhandled exception occured when loading..."  
    except Exception, e:
        print >> sys.stderr, "%r" %e
    finally:
        lock.release()

if __name__ == '__main__':
    main()