"""
loadconfigs.py: Configuration variables
- Read dwh.cfg sections values to variables
- Set & Write param values in dwh.cfg sections
"""

import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg') 

KEY                     = config.get('AWS', 'KEY')
SECRET                  = config.get('AWS', 'SECRET') 

DWH_CLUSTER_TYPE        = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES           = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE           = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER  = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB                  = config.get("DWH", "DWH_DB")
DWH_DB_USER             = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD         = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT                = config.get("DWH", "DWH_PORT")
DWH_IAM_ROLE_NAME       = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_ROLE_ARN            = config.get("DWH", "DWH_ROLE_ARN")
DWH_ENDPOINT            = config.get("DWH", "DWH_ENDPOINT")

LOG_DATA                = config.get("S3", "LOG_DATA")
LOG_JSONPATH            = config.get("S3", "LOG_JSONPATH")
SONG_DATA               = config.get("S3", "SONG_DATA")

def setConfigs(section, param, value):
    config.set(section, param, value)    

def writeConfigs(file, mode):
    config.write(open(file, mode))
