#!/usr/bin/python3
import os
from dotenv import load_dotenv
import subprocess
import logging
from logging.handlers import RotatingFileHandler
import datetime
import time
import json
from influxdb import InfluxDBClient as Local_InfluxDBClient
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

DIR = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(DIR, '.env')
load_dotenv(dotenv_path)

MEASUREMENT_NAME = os.getenv('LOCAL_DB_MEASUREMENT')

def setupLogs() :
  DIR = os.path.dirname(os.path.abspath(__file__))
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.INFO)
  f_handler = RotatingFileHandler(os.path.join(DIR, __file__ + '.log'), maxBytes=2000, backupCount=2)
  f_handler.setLevel(logging.INFO)
  f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  f_handler.setFormatter(f_format)
  logger.addHandler(f_handler)
  return logger


def getConditions() :
  DIRPATH = os.path.dirname(os.path.abspath(__file__))
  cmd = './sensor.py'
  output = subprocess.check_output(os.path.join(DIRPATH, cmd)).decode('utf-8')
  output = json.loads(output)
  return output

def formatData(fields):
  time = datetime.datetime.utcnow()
  print(time)
  body = [{
    "measurement": MEASUREMENT_NAME,
    "time": time,
    "fields": fields
  }]
  return body

def logToLocal(data) :
  ifuser = os.getenv('LOCAL_USER')
  ifpass = os.getenv('LOCAL_PW')
  ifhost = os.getenv('LOCAL_HOST')
  ifport = os.getenv('LOCAL_PORT')
  ifdb = os.getenv('LOCAL_DB')

  ifclient = Local_InfluxDBClient(ifhost, ifport, ifuser, ifpass, ifdb)
  ifclient.write_points(data)

def logToCloud(data) :
  org = os.getenv('ORG')
  bucket = os.getenv('BUCKET')
  token = os.getenv('TOKEN')
  url = os.getenv('URL')

  client = InfluxDBClient(url=url, token=token, org=org, debug=True)
  write_api = client.write_api(write_options=SYNCHRONOUS)
  write_api.write(bucket, org, data)

def logConditions() :
  data = getConditions()
  body = formatData(data)
  logToLocal(body)
  logToCloud(body)

try:
  while True:
    logConditions()
    time.sleep(300)
except Exception as inst:
  logger = setupLogs()
  logger.info('Exception:')
  logger.info(inst)
except KeyboardInterrupt:
  print('Exiting')