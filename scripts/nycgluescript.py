import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "nyctriprecord", table_name = "triprecordtaxi__zone_lookup___pickup_csv", redshift_tmp_dir = args["TempDir"], transformation_ctx = "pickuplookup"]
## @return: pickuplookup
## @inputs: []
pickuplookup = glueContext.create_dynamic_frame.from_catalog(database = "nyctriprecord", table_name = "triprecordtaxi__zone_lookup___pickup_csv", redshift_tmp_dir = args["TempDir"], transformation_ctx = "pickuplookup")
## @type: DataSource
## @args: [database = "nyctriprecord", table_name = "triprecordtaxi__zone_lookup_drop_csv", redshift_tmp_dir = args["TempDir"], transformation_ctx = "droplookup"]
## @return: droplookup
## @inputs: []
droplookup = glueContext.create_dynamic_frame.from_catalog(database = "nyctriprecord", table_name = "triprecordtaxi__zone_lookup_drop_csv", redshift_tmp_dir = args["TempDir"], transformation_ctx = "droplookup")
## @type: DataSource
## @args: [database = "nyctriprecord", table_name = "triprecordfhvtaxitriprecords", redshift_tmp_dir = args["TempDir"], transformation_ctx = "fhvtriprecord"]
## @return: fhvtriprecord
## @inputs: []
fhvtriprecord = glueContext.create_dynamic_frame.from_catalog(database = "nyctriprecord", table_name = "triprecordfhvtaxitriprecords", redshift_tmp_dir = args["TempDir"], transformation_ctx = "fhvtriprecord")
## @type: DataSource
## @args: [database = "nyctriprecord", table_name = "triprecordgreentaxitriprecords", redshift_tmp_dir = args["TempDir"], transformation_ctx = "greentaxirecord"]
## @return: greentaxirecord
## @inputs: []
greentaxirecord = glueContext.create_dynamic_frame.from_catalog(database = "nyctriprecord", table_name = "triprecordgreentaxitriprecords", redshift_tmp_dir = args["TempDir"], transformation_ctx = "greentaxirecord")
## @type: DataSource
## @args: [database = "nyctriprecord", table_name = "triprecordfhvhvtaxitriprecords", redshift_tmp_dir = args["TempDir"], transformation_ctx = "fhvhvtriprecord"]
## @return: fhvhvtriprecord
## @inputs: []
fhvhvtriprecord = glueContext.create_dynamic_frame.from_catalog(database = "nyctriprecord", table_name = "triprecordfhvhvtaxitriprecords", redshift_tmp_dir = args["TempDir"], transformation_ctx = "fhvhvtriprecord")
## @type: DataSource
## @args: [database = "nyctriprecord", table_name = "triprecordyellowtaxitriprecords", redshift_tmp_dir = args["TempDir"], transformation_ctx = "yellowtaxirecord"]
## @return: yellowtaxirecord
## @inputs: []
yellowtaxirecord = glueContext.create_dynamic_frame.from_catalog(database = "nyctriprecord", table_name = "triprecordyellowtaxitriprecords", redshift_tmp_dir = args["TempDir"], transformation_ctx = "yellowtaxirecord")
## @type: Join
## @args: [keys1 = ["pulocationid"], keys2 = ["pulocationid"]]
## @return: yellowpickupjoin
## @inputs: [frame1 = yellowtaxirecord, frame2 = pickuplookup]
yellowpickupjoin = Join.apply(frame1 = yellowtaxirecord, frame2 = pickuplookup, keys1 = ["pulocationid"], keys2 = ["pulocationid"], transformation_ctx = "yellowpickupjoin")
## @type: Join
## @args: [keys1 = ["dolocationid"], keys2 = ["dolocationid"]]
## @return: yellowdropjoin
## @inputs: [frame1 = yellowpickupjoin, frame2 = droplookup]
yellowdropjoin = Join.apply(frame1 = yellowpickupjoin, frame2 = droplookup, keys1 = ["dolocationid"], keys2 = ["dolocationid"], transformation_ctx = "yellowdropjoin")
## @type: Join
## @args: [keys1 = ["pulocationid"], keys2 = ["pulocationid"]]
## @return: greenpickupjoin
## @inputs: [frame1 = greentaxirecord, frame2 = pickuplookup]
greenpickupjoin = Join.apply(frame1 = greentaxirecord, frame2 = pickuplookup, keys1 = ["pulocationid"], keys2 = ["pulocationid"], transformation_ctx = "greenpickupjoin")
## @type: Join
## @args: [keys1 = ["dolocationid"], keys2 = ["dolocationid"]]
## @return: greendropjoin
## @inputs: [frame1 = greenpickupjoin, frame2 = droplookup]
greendropjoin = Join.apply(frame1 = greenpickupjoin, frame2 = droplookup, keys1 = ["dolocationid"], keys2 = ["dolocationid"], transformation_ctx = "greendropjoin")
## @type: SelectFields
## @args: [paths = ["paths"], transformation_ctx = "finalgreentaxirecord"]
## @return: finalgreentaxirecord
## @inputs: [frame = greendropjoin]
finalgreentaxirecord = SelectFields.apply(frame =greendropjoin , paths = ["vendorid","lpep_pickup_datetime","lpep_dropoff_datetime","passenger_count","trip_distance","ratecodeid","store_and_fwd_flag","pulocationid","dolocationid","payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","congestion_surcharge","dropborough","dropzone","dropservice_zone","pickupborough","pickupzone","pickupservice_zone"], transformation_ctx = "finalgreentaxirecord")
## @type: SelectFields
## @args: [paths = ["paths"], transformation_ctx = "finalyellotaxirecord"]
## @return: finalyellotaxirecord
## @inputs: [frame = yellowdropjoin]
finalyellotaxirecord = SelectFields.apply(frame = yellowdropjoin, paths = ["vendorid","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance","ratecodeid","store_and_fwd_flag","locationid","dolocationid","payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","congestion_surcharge","dropborough","dropzone","dropservice_zone","pickupborough","pickupzone","pickupservice_zone"], transformation_ctx = "finalyellotaxirecord")
## @type: Join
## @args: [keys1 = ["pulocationid"], keys2 = ["pulocationid"]]
## @return: fhvpickup
## @inputs: [frame1 = fhvtriprecord, frame2 = pickuplookup]
fhvpickup = Join.apply(frame1 = fhvtriprecord, frame2 = pickuplookup, keys1 = ["pulocationid"], keys2 = ["pulocationid"], transformation_ctx = "fhvpickup")
## @type: Join
## @args: [keys1 = ["dolocationid"], keys2 = ["dolocationid"]]
## @return: fhvdropjoin
## @inputs: [frame1 = "fhvpickup", frame2 = "droplookup"]
fhvdropjoin = Join.apply(frame1 = fhvpickup, frame2 = droplookup, keys1 = ["dolocationid"], keys2 = ["dolocationid"], transformation_ctx = "fhvdropjoin")
## @type: SelectFields
## @args: [paths = ["paths"], transformation_ctx = "finalfhvrecords"]
## @return: finalfhvrecords
## @inputs: [frame = fhvdropjoin]
finalfhvrecords = SelectFields.apply(frame = fhvdropjoin, paths = ["dispatching_base_num","pickup_datetime","dropoff_datetime","pulocationid","dolocationid","dropborough","dropzone","dropservice_zone","pickupborough","pickupzone","pickupservice_zone","sr_flag"], transformation_ctx = "finalfhvrecords")
## @type: Join
## @args: [keys1 = ["pulocationid"], keys2 = ["pulocationid"]]
## @return: fhvhvpickjoin
## @inputs: [frame1 = fhvhvtriprecord, frame2 = pickuplookup]
fhvhvpickjoin = Join.apply(frame1 = fhvhvtriprecord, frame2 = pickuplookup, keys1 = ["pulocationid"], keys2 = ["pulocationid"], transformation_ctx = "fhvhvpickjoin")
## @type: Join
## @args: [keys1 = ["dolocationid"], keys2 = ["dolocationid"]]
## @return: fhvhvdropjoin
## @inputs: [frame1 = fhvhvpickjoin, frame2 = droplookup]
fhvhvdropjoin = Join.apply(frame1 = fhvhvpickjoin, frame2 = droplookup, keys1 = ["dolocationid"], keys2 = ["dolocationid"], transformation_ctx = "fhvhvdropjoin")
## @type: SelectFields
## @args: [paths = ["paths"], transformation_ctx = "finalfhvhvrecords"]
## @return: finalfhvhvrecords
## @inputs: [frame = fhvhvdropjoin]
finalfhvhvrecords = SelectFields.apply(frame = fhvhvdropjoin, paths = ["hvfhs_license_num","dispatching_base_num","pickup_datetime","dropoff_datetime","pulocationid","dolocationid","dropborough","dropzone","dropservice_zone","pickupborough","pickupzone","pickupservice_zone","sr_flag"], transformation_ctx = "finalfhvhvrecords")
## @type: DataSink
## @args: [catalog_connection = "redshift-connection", connection_options = {"dbtable": "greentaxitriprecords", "database": "triprecorddb"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = finalgreentaxirecord, catalog_connection = "redshift-connection", connection_options = {"dbtable": "greentaxitriprecords", "database": "triprecorddb"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")

## @type: DataSink
## @args: [catalog_connection = "redshift-connection", connection_options = {"database" : "triprecorddb", "dbtable" : "yellowtaxitriprecords"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = finalyellotaxirecord]
datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = finalyellotaxirecord, catalog_connection = "redshift-connection", connection_options = {"database" : "triprecorddb", "dbtable" : "yellowtaxitriprecords"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
## @type: DataSink
## @args: [catalog_connection = "redshift-connection", connection_options = {"database" : "triprecorddb", "dbtable" : "forhirevehicetriprecord"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink6"]
## @return: datasink6
## @inputs: [frame = finalfhvrecords]
datasink6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = finalfhvrecords, catalog_connection = "redshift-connection", connection_options = {"database" : "triprecorddb", "dbtable" : "forhirevehicetriprecord"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink6")

## @type: DataSink
## @args: [catalog_connection = "redshift-connection", connection_options = {"database" : "triprecorddb", "dbtable" : "forhirehighvolumetripreocord"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink7"]
## @return: datasink7
## @inputs: [frame = finalfhvhvrecords]
datasink7= glueContext.write_dynamic_frame.from_jdbc_conf(frame = finalfhvhvrecords, catalog_connection = "redshift-connection", connection_options = {"database" : "triprecorddb", "dbtable" : "forhirehighvolumetripreocord"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink7")


job.commit()