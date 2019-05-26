from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, MapType, ArrayType
from pyspark.sql.functions import *
from wixspark.jdbc import SqlLoader
from wixspark import SundukStorer , SundukLoader

import io
import tarfile
from wix_code_page_analyzer import PageAnalyzer as Pa
import os
import json


class SiteAnalyzer:

    @staticmethod
    def analyze_blob(site_blob):
        """
           Return dictionary {'file_name': file_str} per site
        """
        try:
            obj = {}
            file_like_object = io.BytesIO(site_blob)
            tar = tarfile.open(fileobj=file_like_object, mode='r:gz')
            for member in tar.getmembers():
                cfile = tar.extractfile(member)
                if cfile:
                    file_name = member.name
                    is_js = file_name.endswith(('.js','.jsw'))
                    if is_js:
                        try:
                            file_str = cfile.read().decode("utf-8")
                        except Exception as e:
                            print('ERROR IN DECODING FILE ' + ' : ' + str(e))
                            file_str = 'ERROR IN DECODING FILE'

                        obj[file_name] = file_str
                        cfile.close()
        except KeyError as e:
            print('analyze blob failed', e)

        return obj

    @staticmethod
    def pages_id(obj):
        """
            Return array [key_1,key_2,...] per site
        """
        arr = []
        for key in obj.keys():
            arr.append(key)
        return arr

    @staticmethod
    def site_str(key, obj: dict):
        return obj.get(key, '')

    @staticmethod
    def _analyze_file(file_name:str, file_str:str) -> dict:
        analysis = {'public': 0,
                    'backend': 0,
                    'has_code': False,
                    'has_router': 0,
                    'has_http_func': 0,
                    'has_data_hooks': 0,
                    'has_errors': True,
                    'complexity': 0,
                    'LOC': 0,
                    'depth': 0,
                    'AST_STR': '',
                    'PROPERTIES': {},
                    'URLs': {},
                    'import': {},
                    'JS_TYPE': {},
                    'AST_HASH': '',
                    'APIs': {}}

        try:
            file_analysis = Pa.analyze_js_file(file_str)
            analysis['public'] += 1 if file_name.startswith('./public/') else 0
            analysis['backend'] += 1 if file_name.startswith('./backend/') else 0
            analysis['has_router'] += 1 if file_name.endswith('./backend/routers.js') else 0

            analysis['has_http_func'] += 1 if file_name.endswith('./backend/http-functions.js') else 0
            analysis['has_data_hooks'] += 1 if file_name.endswith('./backend/data.js') else 0

            for k, v in file_analysis.items():
                analysis[k] = v
                if 'import_' in k:
                    analysis['import'][k] = 1
                else:
                    analysis[k] = v
        except Exception as e:
            print("Analyze js file failed", e)
        return analysis

    @staticmethod
    def _analyze_site(site_obj):
        analysis = {}
        for file_name, file_str in site_obj.items():
            analysis[file_name] = SiteAnalyzer._analyze_file(file_name, file_str)  ## json string
        res = json.dumps(analysis)
        return res

    @staticmethod
    def apis(site_json):
        return site_json['APIs']

    @staticmethod
    def js_type(site_json):
        return site_json['JS_TYPE']

    @staticmethod
    def urls(site_json):
        return site_json['URLs']

    @staticmethod
    def property(site_json):
        return site_json['PROPERTIES']

    @staticmethod
    def imports(site_json):
        return site_json['import']


class WixCodeRunner(object):

    def __init__(self, spark: SparkSession):
        self.__spark = spark
        self.__spark.sparkContext.addPyFile(os.path.join(os.path.dirname(__file__), 'wix_code_analyzer_v2.py'))
        self.__spark.sparkContext.addPyFile(os.path.join(os.path.dirname(__file__), 'wix_code_page_analyzer.py'))

        self.__spark.udf.register("analyze_blob", SiteAnalyzer.analyze_blob, MapType(StringType(), StringType()))
        self.__spark.udf.register("_analyze_file", SiteAnalyzer._analyze_file, MapType(StringType(), StringType()))
        # _analyze_file = self.__spark.udf.register(SiteAnalyzer._analyze_file, MapType(StringType(), StringType()))
        self.__spark.udf.register("analyze_js_file", Pa.analyze_js_file, MapType(StringType(), StringType()))

        self.__spark.udf.register("apis", SiteAnalyzer.apis, MapType(StringType(), StringType()))
        self.__spark.udf.register("js_type", SiteAnalyzer.js_type, MapType(StringType(), StringType()))
        self.__spark.udf.register("urls", SiteAnalyzer.urls, MapType(StringType(), StringType()))
        self.__spark.udf.register("property", SiteAnalyzer.property, MapType(StringType(), StringType()))
        self.__spark.udf.register("imports", SiteAnalyzer.imports, MapType(StringType(), StringType()))

    def execute(self, start_timestamp, end_timestamp, mode='overwrite'):
        SqlLoader(self.__spark, "wix_html_editor").load_table('''(
                    SELECT site_id
                            ,date_updated
                            ,REPLACE((JSON_EXTRACT(more_data, '$.wixCodeAppData.codeAppId')), '\"', '') as code_app_id
                    FROM site_headers
                    WHERE more_data like '%wixCodeAppData%'
                        and date_updated >= {start}
                        and date_updated < {end}
                    ) as T'''.format(start=start_timestamp, end=end_timestamp),partition_column="date_updated" \
                                               ,lower_bound=start_timestamp,upper_bound=end_timestamp,num_partitions=100) \
            .createOrReplaceTempView("msids")

        SqlLoader(self.__spark, "wix_code_db").load_table('''(
            SELECT id as code_app_id
                ,updated_ts
                ,data
            FROM blobs
            where id is not null
                and updated_ts >= FROM_UNIXTIME({start}/1000)
                and updated_ts < FROM_UNIXTIME({end}/1000)
            ) as T'''.format(start=start_timestamp, end=end_timestamp)).createOrReplaceTempView("blobs")




        if True:
            dff = self.__spark.sql('''SELECT   msids.code_app_id
                                            ,msids.site_id
                                            ,blobs.updated_ts
                                            ,analyze_blob(blobs.data) as blob_data
                                            
                                    FROM msids
                                    JOIN blobs
                                    ON msids.code_app_id = blobs.code_app_id
                                   '''

                             )

            dff.select('code_app_id'
                                ,'site_id'
                                , 'updated_ts'
                                , explode('blob_data').alias('page','site_str')).where(col("page").isNotNull()).createOrReplaceTempView(
                'pages_data')

            self.__spark.sql('''select code_app_id
                                             ,site_id
                                             ,updated_ts
                                             ,page
                                             ,site_str
                                             ,_analyze_file(page, site_str) as analyze_file
                                        from pages_data
            
                    ''').createOrReplaceTempView('analyze_site')

            df = self.__spark.sql('''select code_app_id
                                             ,site_id
                                             ,updated_ts
                                             ,page
                                             ,site_str
                                             ,analyze_file
                                             ,apis(analyze_file) as apis
                                             ,urls(analyze_file) as urls
                                             ,property(analyze_file) as property
                                             ,imports(analyze_file) as imports
                                             ,js_type(analyze_file)as js_type
                                from analyze_site
            
                            ''')

            # SundukStorer(user="wix", schema="tbl").store(table= "wix_code_blobs", data= df, mode=mode)

            # SundukLoader().load(table="wix_code_blobs", partition=100).createOrReplaceTempView('sunduk_data')



            SundukStorer(user="wix", schema="tbl") \
                .store(table="wix_code_site_analyzer_v23", data=df, mode=mode )
            # Accepted save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'.


        else:
            analyzed_site_df = self.__spark.sql('''
                        SELECT code_app_id
                                , updated_ts
                                , data 
                        FROM wix_code_sites
                        ''')

            for row in analyzed_site_df.rdd.collect():
                dct =   SiteAnalyzer.property(SiteAnalyzer._analyze_file(SiteAnalyzer.pages_id(SiteAnalyzer.analyze_blob(row['data'])), \
                                SiteAnalyzer.site_str(SiteAnalyzer.pages_id(SiteAnalyzer.analyze_blob(row['data'])), row['data'])))
                for k, v in dct.items():
                    print(SiteAnalyzer._analyze_file(k, v))
