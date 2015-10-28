# -*- coding: utf-8 -*-


"""
10/28/2015 Chen Weiqiang 全部商品更新cm_picked为1

10/26/2015 Chen Weiqiang 基本完成  部分字段需要完善

10/23/2015 Chen Weiqiang 测试初步通过的版本
"""

import lockfile
import MySQLdb
import shutil
import MySQLdb.cursors
import ConfigParser
import glob
import os
import re
import datetime
import codecs
import pickle
import time
import logging
from logging import error, warning, info, debug

config = ConfigParser.ConfigParser()
fp = open('load.cfg')
config.readfp(fp)


today_date = datetime.date.today()
logFilename = "./logs/load_" + str(today_date) + ".log"

logging.basicConfig(filename=logFilename,
                    level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%m/%d/%Y %H:%M:%S")


def get_config(section, option):
    result = ''
    try:
        result = config.get(section, option)
    except:
        debug('Cannot find [%s].[%s] in config file...' %(section, option))
    finally:
        return result

data_dir = get_config('all', 'data.dir')
save_dir = get_config('all', 'save.dir')

DATA_GLOB_PATTERN = "*_productInfo.csv"
DATA_RE_PATTERN = "([\w-]+)_(\d\d)-(\d\d)-(\d\d\d\d)_productInfo.csv"

def get_file_path(parentDir, filename):
    return os.path.join(parentDir, filename)

date_pattern = '(\d\d)-(\d\d)-(\d\d\d\d)'

def parse_date(str1):
    result = re.findall(date_pattern, str1)
    if result:
        return datetime.date(int(result[0][2]), int(result[0][0]), int(result[0][1]))

def parse_capture(filename):
    results = re.findall(DATA_RE_PATTERN, filename)
    merchantName = results[0][0]
    captureDate = datetime.date(int(results[0][3]), int(results[0][1]), int(results[0][2]))
    return Capture(merchantName, captureDate, filename)

def read_headers(line):
    if line.startswith(codecs.BOM_UTF8):
        line = line[len(codecs.BOM_UTF8):]
    return line.split("\t")

class Merchant:
    def __init__(self, merchantId, name, scoring_fields):
        self.merchantId = merchantId
        self.merchantName = name
        self.scoringFields = scoring_fields.split(",")
        
    def __str__(self):
        return "%s(%s)%s" %(self.merchantName, self.merchantId, str(self.scoringFields))
    
    __repr__ = __str__

class Capture():
    def __init__(self, merchantName, date, filename):
        self.merchantName = merchantName
        self.date = date
        self.filename = filename
        self.merchant = None
    
    def get_filename(self):
        return self.filename
    
    def __str__(self):
        return self.get_filename()
    
    __repr__ = __str__
    

class Db:
    def __init__(self, useServerCursor=False):
        if useServerCursor:
            self.conn = MySQLdb.connect(host=get_config('localhost', 'host'),
                                        user=get_config('localhost', 'user'),
                                        passwd=get_config('localhost', 'passwd'),
                                        cursorclass=MySQLdb.cursors.SSCursor)
        else:
            self.conn = MySQLdb.connect(host=get_config('localhost', 'host'),
                                        user=get_config('localhost', 'user'),
                                        passwd=get_config('localhost', 'passwd'))
        
        self.cursor = self.conn.cursor()
        self.conn.autocommit(False)
        self.conn.select_db('aims')
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

class Loader():
    def __init__(self):
        self.merchants = {} #选取的商户是enable_ct为1的商户
        
        self.load_records = {}
        
        self.loadMerchantsFromDb()
        
        self.loadLoadingRecordsFromConfig()
        
        self.loaded_files = {}
    
    def loadMerchantsFromDb(self):
        db = Db()
        sql="""SELECT
                   id, name, scoring_fields
               FROM
                   merchants
               WHERE
                   enable_ct = 1
            """
        db.cursor.execute(sql)
        for row in db.cursor.fetchall():
            merchantName = row[1]
            merchant = Merchant(row[0], row[1], row[2])
            self.merchants[merchantName] = merchant
        db.close()
    
    def loadLoadingRecordsFromConfig(self):
        for merchantName in self.merchants.keys():
            date_str = get_config("loadrecords", merchantName)
            date_obj = parse_date(date_str) #获取datetime.date对象
            if date_obj:
                self.load_records[merchantName] = date_obj

    def run(self):
        new_captures = self.get_new_captures()
        info("[%s] New Captures: %s" %(datetime.datetime.now().isoformat(), new_captures))
        print "[%s] New Captures: %s" %(datetime.datetime.now().isoformat(), new_captures)
        for capture in new_captures:
            try:
                captureLoader = CaptureLoader(capture)
                captureLoader.load()
                self.saveDataFile(capture.get_filename())
                self.saveDataFile(capture.get_filename().replace("csv", "xls"))
                self.loaded_files[capture.get_filename()] = True
            except Exception, e:
                print e

    def get_new_captures(self):
        result = []
        files = glob.glob(get_file_path(data_dir,DATA_GLOB_PATTERN))
        for filename in files:
            if self.loaded_files.has_key(filename):
                continue
            capture = parse_capture(filename)
            merchant = self.merchants.get(capture.merchantName) #aims.merchants表中是否存在该商户
            if not merchant:
                warning("merchant %s is not added into merchants table..." %capture.merchantName)
                print "merchant %s is not added into merchants table..." %capture.merchantName
                continue
            capture.merchant = merchant
            #要么是new capture的文件, 要么是过时(时间早于最近一次load记录)的文件
            if self.isNewCaptured(capture):
                result.append(capture)
            else:
                self.loaded_files[capture.get_filename()] = True
        return result
    
    def isNewCaptured(self, capture):
        date_obj = self.load_records.get(capture.merchantName)
        if date_obj:
            if (capture.date - date_obj).days <= 0:
                return False         
        return True
    
    def saveDataFile(self, filename):
        bakFile = os.path.join(save_dir, os.path.basename(filename))
        if not os.path.exists(filename):
            print "%s does not exist. Do nothing." %filename
            return 
        if os.path.exists(bakFile):
            print "%s have already existed. Remove it." %bakFile
            os.remove(bakFile)
        info("move %s to %s...", filename, bakFile)
        print "move %s to %s..." %(filename, bakFile)
        shutil.move(filename, bakFile)

class CaptureLoader():
    def __init__(self, capture):
        self.capture = capture

    def load(self):
        
        info("\nloading file: %s...", self.capture.get_filename())
        print "\nloading file: %s..." %os.path.basename(self.capture.get_filename())

        self.db = Db()
                
        self.parseHeaders()
         
        self.initProductParser()
         
        self.record_load()
         
        #将数据库中已有的categories信息放入缓存
        self.load_categories_cache()
#         #可以对已经存在的品类做一个统计
#         self.stat_categories()
        #将数据库中已有的products信息放入缓存
        self.load_products_cache()
         
        self.load_categories()
         
        self.load_products()
        
        self.record_load_success()
        
        info("end.\n")
        print("end.\n")

    def parseHeaders(self):
        fr = open(self.capture.get_filename(), 'r')
        line = fr.readline().strip()
        self.fields = read_headers(line)
        info("fields: %s" %self.fields)
        print "fields: %s" %self.fields
        info("key field: %s" %self.getKeyField())
        print "key field: %s" %self.getKeyField()
        fr.close()
        
    def getKeyField(self):
        return 'sku_id' if 'sku_id' in self.fields else 'product_url'
    
    def initProductParser(self):
        fields = self.fields
        if "likes" in self.capture.merchant.scoringFields or "reviews" not in self.fields:
            idx = fields.index("likes")
            fields[idx] = "reviews"
            info("regard the field 'likes' as 'reviews'")
            print "regard the field 'likes' as 'reviews'"
            info("new fields are: %s" %fields)
            print "new fields are: %s" %fields
        self.productParser = ProductParser(fields)
        
    
    def record_load(self):
        sql = """
                  select * from captures where merchant_id=%s and capture_date=%s
              """
        sql2 = """
                  insert into captures (merchant_id, capture_date, status)
                  values (%s, %s, %s)
               """
        self.db.cursor.execute(sql, (self.capture.merchant.merchantId, self.capture.date))
        result = self.db.cursor.fetchone()
        self.db.cursor.close()
        self.db.cursor = self.db.conn.cursor()
        if result:
            pass
        else:
            self.db.cursor.execute(sql2, (self.capture.merchant.merchantId, self.capture.date, 0))
        self.db.conn.commit()

    def load_categories_cache(self):
        info("loading categories in db into cache...")
        print 'loading categories in db into cache...'
        self.categoryCache = CategoryCache()
        sql = """
                 SELECT
                    id, name, level, parent_id 
                 FROM
                    categories
                 WHERE
                    merchant_id=%s
                 ORDER BY 
                    level, parent_id

              """
        self.db.cursor.execute(sql, self.capture.merchant.merchantId)
        for row in self.db.cursor.fetchall():
            parentCategoryId = row[3]
            category = Category(row[0], row[1], row[2])
            self.categoryCache.cache_category(category, parentCategoryId)
        info("%s have been loaded." %self.categoryCache.getCategoriesCount())
        print "%s have been loaded." %self.categoryCache.getCategoriesCount()
        self.db.close()
    
    def load_products_cache(self):
        """只从数据库中读出ct_status>0的商品
        """
        info("loading products in db into cache...")
        print "loading products in db into cache..."
        db = Db(useServerCursor=True)
        #暂不关注的字段ct_status, cm_picked, add_time, status_update_time, cm_pick_time
        #         stock_status, first_capture_date, first_reviews, last_capture_date,
        #         last_reviews, merchant_id
        sql = """SELECT
                    id, name, category_id,
                    sku_id, url, img_url, price, reviews,
                    category_index, sell_status, 
                    sellstart_date, latest_capture_date
                 FROM
                    products
                 WHERE
                    merchant_id=%s and ct_status >=0
              """
        self.productCache = ProductCache(self.getKeyField())
        db.cursor.execute(sql, self.capture.merchant.merchantId)
        while True:
            row = db.cursor.fetchone()
            if not row:
                break
            keyFieldIdx = 3 if self.getKeyField() == 'sku_id' else 4
            self.productCache.put(key=row[keyFieldIdx],
                                  aimsId=row[0],
                                  name=row[1],
                                  category_id=row[2],
                                  img_url=row[5],
                                  price=row[6],
                                  reviews=row[7],
                                  category_index=row[8],
                                  sell_status=row[9],
                                  sellstart_date=row[10],
                                  latest_capture_date=row[11])
        db.close()
        info("%s products have been loaded.", self.productCache.size())
        print "%s products have been loaded." %self.productCache.size()
    
    def load_categories(self):
        info("loading categories from file...")
        print 'loading categories from file...'
        add_final_count = 0 #本函数类调用insert_category一次加1
        self.add_alllevel_count = 0 #每调用一次insert_category方法 加1
        self.db = Db()
        filename = self.capture.get_filename()
        fr = open(filename, "r")
        fr.readline() #跳过表头
        while True:
            line = fr.readline()
            if not line:
                break
            product = self.productParser.parse(line)
            if not product: #字段长度不符合 得不到product
                continue
            categoryPathList = product.getCategoryPath()
            category = self.categoryCache.find_category(categoryPathList)
            if category:
                pass
            else:
                self.insert_category(categoryPathList)
                add_final_count += 1
        self.db.close()
        info("%s final categories added.", add_final_count)
        info("%s all sorts of categories added.", self.add_alllevel_count)
        print "%s final categories added." %add_final_count
        print "%s all sorts of categories added." % self.add_alllevel_count
    
    def insert_category(self, categoryPath):
        self.add_alllevel_count += 1 #每调用方法一次 次数加一
        if self.add_alllevel_count % 10 == 0: #不能放到后面 否则出现重复打印的BUG
            info('new added %s', self.add_alllevel_count)
            print 'new added %s' %self.add_alllevel_count
        if len(categoryPath) > 1:
            parentCategory = self.categoryCache.find_category(categoryPath[:-1])
            if not parentCategory:
                parent_id = self.insert_category(categoryPath[:-1])
            else:
                parent_id = parentCategory.categoryId
        else:
            parent_id = 0
        categoryName = categoryPath[-1]
        categoryLevel = len(categoryPath)
        sql = """INSERT INTO
                    categories (name, level, parent_id, merchant_id)
                values
                    (%s, %s, %s, %s)
              """
        self.db.cursor.execute(sql, (categoryName, categoryLevel, parent_id, self.capture.merchant.merchantId))
        
        category_id = self.db.cursor.lastrowid
        #新生成的category也放入CategoryCache
        new_category = Category(category_id, categoryName, categoryLevel)
        self.categoryCache.cache_category(new_category, parent_id)
        level1_category_id = self.categoryCache.getLevel1CategoryId(category_id)
        if not level1_category_id or level1_category_id < 0:
            error("fail to get level1_category_id of category path %s" %categoryPath)
            print "fail to get level1_category_id of category path %s" %categoryPath
            raise Exception
        sql2 = """UPDATE 
                     categories
                  SET
                     level1_category_id = %s
                  WHERE
                     id = %s
               """
        self.db.cursor.execute(sql2, (level1_category_id, category_id))
        self.db.conn.commit()
        return category_id #供迭代时使用

    def load_products(self):
        info("loading products from file...")
        print "loading products from file..."
        self.db = Db()
        filename = self.capture.get_filename()
        fr = open(filename, 'r')
        fr.readline() #跳过表头
        count, insert, update, keep, error = 0, 0, 0, 0, 0
        reviews_update, price_update, imgUrl_update, sellStartDate_update, categoryIndex_update = 0, 0, 0, 0, 0
        while True:
            try:
                line = fr.readline().strip()
                if not line:
                    count -= 1
                    break #break之后仍然会执行finally
                product = self.productParser.parse(line)
                if not product: #此处的line应该记录为？
                    error += 1
                    continue
                status, details = self.productCache.getProductStatus(product)
                if status =='new':
                    categoryPath = product.getCategoryPath()
                    #品类书已经完善, 求出新品的category id
                    product['category_id'] = self.categoryCache.find_category(categoryPath).categoryId
                    self.insert_product(product)
                    insert += 1
                elif status == 'update':
                    if 'reviews' in details:
                        self.updateReviews(product)
                        reviews_update += 1
                    if 'price' in details:
                        self.updatePrice(product)
                        price_update += 1
                    if 'img_url' in details:
                        self.updateImgUrl(product)
                        imgUrl_update += 1
                    if 'sellstart_date' in details:
                        self.updateSellStartDate(product)
                        sellStartDate_update += 1
                    if 'category_index' in details:
                        self.updateCategoryIndex(product)
                        categoryIndex_update += 1
                    update += 1
                else:
                    keep += 1
                #三种情形都要执行 可能造成数据不吻合 error增多
                self.insert_product_ranks(product)
                self.updateLatestCaptureDate(product)
                #配合最近推行的直接抓取部分商品 全部推送
                self.setCmPicked(product)
            except Exception, e:
                print e
                error += 1
            finally:
                count += 1
                if count %100 == 0:
                    self.db.conn.commit()
        self.db.conn.commit()
        output = """
totally loaded: %d
        insert: %d
          keep: %d
         error: %d
        update: %d
            reviews: %d
              price: %d
            img_url: %d
     category_index: %d
     sellstart_date: %d
        """
        print output %(count, insert,  keep, error, update, reviews_update, price_update, imgUrl_update, categoryIndex_update, sellStartDate_update)
    
    def insert_product(self, product):
        #23个字段
        #"id" "add_time"系统自动生成
        #"cm_picked" "cm_pick_time", "status_update_time" 系统后期处理                    
        #"last_capture_date" "last_reviews" 至少两次抓取才不为None
        #"stock_status"暂时不考虑
        sql = """insert into products
                (name, merchant_id, category_id, sku_id, 
                url, img_url, price, reviews, category_index,
                sell_status, sellstart_date,
                first_capture_date, first_reviews,
                latest_capture_date, ct_status)
                values
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        #not all arguments converted during string formatting
        
        self.db.cursor.execute(sql,(product['name'],
                                    self.capture.merchant.merchantId,
                                    product['category_id'],
                                    product['sku_id'],
                                    product['product_url'],
                                    product['img_url'],
                                    product['price'],
                                    product['reviews'],
                                    product.getCategoryIndex(),
                                    product.getSellStatus(),
                                    product.getSellStartDate(),
                                    self.capture.date,
                                    product['reviews'],
                                    self.capture.date,
                                    0))
        product_id = self.db.cursor.lastrowid
        product['id'] = product_id #不加发生bug
        #新增完毕后  放入缓存
        self.productCache.put(product[self.getKeyField()],
                              product_id,
                              product['name'],
                              product['category_id'],
                              product['img_url'],
                              product['price'],
                              product['reviews'],
                              product.getCategoryIndex(),
                              product.getSellStatus(),
                              product.getSellStartDate(),
                              self.capture.date
                              )
        self.insert_product_changes(product)
        
    def updateReviews(self, product):
        #将之前最新的reviews和latest_capture_date更新为last存在一定的隐患
        cachedValues = self.productCache.findByProduct(product)
        sql = """replace into products (id, last_reviews, last_capture_date, reviews)
                 values (%s, %s, %s, %s)
              """
        self.db.cursor.execute(sql, (cachedValues['aimsId'], cachedValues['reviews'], \
                              cachedValues['latest_capture_date'], product['reviews']))
        
        self.insert_product_changes(product)
        
    def insert_product_changes(self, product):
        cachedValues = self.productCache.findByProduct(product)
        sql = """replace into 
                 product_changes(product_id, capture_date, merchant_id, reviews, price)
                 values(%s, %s, %s, %s, %s)
              """
        self.db.cursor.execute(sql, (cachedValues['aimsId'], self.capture.date, self.capture.merchant.merchantId,\
                        product['reviews'], product['price']))
        
    def updatePrice(self, product):
        cachedValues = self.productCache.findByProduct(product)
        sql = """update products set price = %s where id = %s
              """
        self.db.cursor.execute(sql, (product['price'], cachedValues['aimsId']))
    
    def updateImgUrl(self, product):
        cachedValues = self.productCache.findByProduct(product)
        sql = """update products set img_url = %s where id = %s
              """
        self.db.cursor.execute(sql, (product['img_url'], cachedValues['aimsId']))
    
    def updateSellStartDate(self, product):
        cachedValues = self.productCache.findByProduct(product)
        sql = """update products set sellstart_date = %s where id = %s
              """
        self.db.cursor.execute(sql, (product.getSellStartDate(), cachedValues['aimsId']))
    
    def updateCategoryIndex(self, product):
        sql = """update products set category_index = %s where id = %s
              """
        self.db.cursor.execute(sql, (product.getCategoryIndex(), product['id']))
    
    def insert_product_ranks(self, product):
        sql = """replace into products_sammydress 
                 (product_id, capture_date, price, price_old, reviews, \
                  facebook_likes, google_likes, vk_likes, page, position)
                  values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              """
        self.db.cursor.execute(sql, (product['id'],
                                     self.capture.date,
                                     product['price'],
                                     product['price_old'],
                                     product['reviews'],
                                     product['facebook_likes'],
                                     product['google_likes'],
                                     product['vk_likes'],
                                     product['page'],
                                     product['position']))
    
    def updateLatestCaptureDate(self, product): #是否使用该方法值得商榷
        sql = """update products set latest_capture_date = %s where id = %s 
              """
        self.db.cursor.execute(sql, (self.capture.date, product['id']))
        
    def setCmPicked(self, product):
        #不是使用and 使用,
        sql = """update products set cm_picked = 1, cm_pick_time=%s
                 where id = %s
              """
        self.db.cursor.execute(sql, (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), product['id']))

    def record_load_success(self):
        config.set("loadrecords", self.capture.merchant.merchantName, self.capture.date.strftime("%m-%d-%Y"))
        fw = open("load.cfg", 'w')
        config.write(fw)

        sql = """
                  update captures set status = 1 where merchant_id = %s and capture_date = %s
              """
        self.db.cursor.execute(sql, (self.capture.merchant.merchantId, self.capture.date))
        sql2 = """
                   update merchants set latest_capture_date = %s where id = %s
               """
        self.db.cursor.execute(sql2, (self.capture.date, self.capture.merchant.merchantId))
        self.db.conn.commit()

class ProductParser:
    def __init__(self, fields):
        self.fields = fields
        
    def parse(self, line):
        values = line.split("\t")
        if len(values) != len(self.fields):
            return
        attrs = {}
        for index, value in enumerate(values):
            attrs[self.fields[index]] = value
        return Product(**attrs)
    
class CategoryCache:
    def __init__(self):
        self.rootCategory = Category(0,'',0) #虚拟根节点
        self.categoriesMap = {0: self.rootCategory}
    
    lastCategoryPath = None
    lastCategory = None

    def cache_category(self, category, parentCategoryId):
        parent_category = self.categoriesMap.get(parentCategoryId)
        if not parent_category:
            time.sleep(10)
        parent_category.add_child(category)
        self.categoriesMap[category.categoryId] = category
        
        self.lastCategoryPath = None
        self.lastCategory = None
    
    def find_category(self, categoryPath):
        if categoryPath == self.lastCategoryPath:
            return self.lastCategory
        current_category = None
        iterCategory = self.rootCategory
        for categoryName in categoryPath:
            current_category = iterCategory.find_child(categoryName)
            if not current_category:
                break
            iterCategory = current_category
        return current_category
    
    def getLevel1CategoryId(self, category_id):
        if not self.categoriesMap.has_key(category_id): #检查的category还没有加入map
            return -1
        category = self.categoriesMap.get(category_id)
        while True:
            if not category.parent or category.parent.categoryId == 0:
                return category.categoryId
            category = category.parent
    
    def getCategoriesCount(self):
        return len(self.categoriesMap) - 1

class ProductCache:
    def __init__(self, cacheType):
        self.cacheType = cacheType
        self.cache = {} #暂时使用字典 需要修改为kch存储
    
    def put(self, key, aimsId=None, name=None, category_id=None,\
                    img_url=None, price=None, reviews=None,\
                    category_index=None,sell_status=None,\
                    sellstart_date=None, latest_capture_date=None):
        values = {"aimsId": aimsId,
                  "name": name,
                  "category_id": category_id,
                  "img_url": img_url,
                  "price": price,
                  "reviews": int(reviews),
                  "category_index": category_index,
                  "sell_status": sell_status,
                  "sellstart_date": sellstart_date,
                  "latest_capture_date": latest_capture_date}
        values_obj = pickle.dumps(values)
        if self.cache.has_key(key): #测试代码
            print key
        self.cache[key] = values_obj
    
    def find(self, key):
        values_obj = self.cache.get(key)
        return pickle.loads(values_obj)

    def findByProduct(self, product):
        key = product[self.cacheType]
        return self.find(key)

    def getProductStatus(self, product):
        if self.cache.has_key(product[self.cacheType]): #cache中(数据库中)是存在的
            details = []
            cached_values = self.find(product[self.cacheType])
            product['id'] = cached_values['aimsId'] #不知道结果能不能返回 能！
            if product.getReviews() > int(cached_values['reviews']): #后加入的reviews为字符串
                details.append('reviews')
            if product['price'] and product['price'] != cached_values['price']:
                details.append("price")
            if product['img_url'] and product['img_url'] != cached_values['img_url']:
                details.append("img_url")
            if product.getCategoryIndex() > 0 and product.getCategoryIndex() != cached_values['category_index']:
                details.append('category_index')
            if product.getSellStartDate() and product.getSellStartDate() != cached_values['sellstart_date']:
                details.append('sellstart_date')
            #产品的三种情形
            if not details:
                return "keep", []
            else:
                return "update", details
        else:
            return 'new', []
    
    def size(self):
        return len(self.cache)

class Category:
    def __init__(self, categoryId, name, level):
        self.categoryId = categoryId
        self.name = name
        self.level = level
        self.parent = None #父节点只有一个
        self.child = [] #子节点可能有若干个
    
    def add_child(self, child_category):
        self.child.append(child_category)
        child_category.parent = self
    
    def find_child(self, childName):
        for category in self.child:
            if category.name == childName:
                return category
    
    def __str__(self):
        result = self.name
        category = self
        while True:
            if not category.parent or category.parent.categoryId == 0:
                break
            else:
                category = category.parent
                result = category.name + " > " + result
        return result
    
    __repr__ = __str__
                                

class Product:
    def __init__(self, **args):

        self.level1_category = ''
        self.level2_category = ''
        self.level3_category = ''
        self.name = ''
        self.url = ''
        self.sku_id = ''
        self.img_url = ''
        self.reviews = ''
        self.category_index = ''
        self.price = ''

        self.attrs = {}
        for key, value in args.items():
            self[key] = value
            
    
    def __setitem__(self, key, value):
        if self.__dict__.has_key(key):
            self.__dict__[key] = value
        else:
            self.attrs[key] = value
    
    def __getitem__(self, key):
        if self.__dict__.has_key(key):
            return self.__dict__[key]
        else:
            return self.attrs.get(key)
        
    def getCategoryPath(self):
        result = []
        if self['level1_category']: # if ''为False
            result.append(self['level1_category'])
            if self['level2_category']:
                result.append(self['level2_category'])
                if self['level3_category']:
                    result.append(self['level3_category'])
        return result
    
    def getSellStatus(self):
        if self['sell_status'] == 'NORMAL':
            return 1
        elif self['sell_status'] == 'CLEARANCE':
            return 11
        elif self['sell_status'] == 'SPECIAL':
            return 12
        elif self['sell_status'] == 'SOLD OUT':
            return 21
        elif self['sell_status'] == "OUT OF STOCK":
            return 22
        else:
            print self['sell_status'], self.sku_id, "other sell status"
            return 0

    def getSellStartDate(self):
        if self['add_date']: #2015/2/2
            fields = self['add_date'].split("/")
            if len(fields) == 2:
                return datetime.date(int(fields[0]), int(fields[1]), 1)
            elif len(fields) == 3:
                return datetime.date(int(fields[0]), int(fields[1]), int(fields[2]))      
    
    def getReviews(self):
        return int(self['reviews'])
    
    def getCategoryIndex(self):
        # 60是手动数据  第二行怀疑存在隐蔽的bug
        if self["category_index"] or self["cate_idx"]:
            return int(self["category_index"]) if self["category_index"] else int(self["cate_idx"])
        if self['page_idx'] and self['num_idx']:
            return (int(self['page_idx']) - 1) * 60 + int(self['num_idx'])
        if self['page'] and self['position']:
            return (int(self['page']) - 1) * 60 + int(self['position'])
        return 0

def main():
    lock = lockfile.FileLock("load-lock")
    try:
        lock.acquire(10)
        loader = Loader()
        loader.run()
    except Exception, e:
        print e
    finally:
        lock.release()

if __name__ == '__main__':
    main()
