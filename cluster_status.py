#!/opt/conda/bin/python
"""
cluster_status.py: Check Redshift Cluster Status
- Create Pandas Dataframe
- Use redshift.describe_clusters to populate and display
"""
import boto3
import time
import loadconfigs as l
import pandas as pd

# Redshift Client
redshift = boto3.client (
           'redshift',
           region_name="us-west-2",
           aws_access_key_id=l.KEY,
           aws_secret_access_key=l.SECRET
)


# Def for Cluster status
def prettyRedshiftProps(props):
    # Pandas Dataframe for Cluster Status
    pd.DataFrame({"Param":
                      ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", 
                       "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER",
                       "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", 
                       "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                  "Value":
                      [l.DWH_CLUSTER_TYPE, l.DWH_NUM_NODES, 
                       l.DWH_NODE_TYPE, l.DWH_CLUSTER_IDENTIFIER,
                       l.DWH_DB, l.DWH_DB_USER, l.DWH_DB_PASSWORD, 
                       l.DWH_PORT, l.DWH_IAM_ROLE_NAME]
    })
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
        "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


# Display Status
try:
    myClusterProps = redshift.describe_clusters(
                     ClusterIdentifier=l.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print(prettyRedshiftProps(myClusterProps))
   
    # Keep checking status till Endpoint appears
    while('Endpoint' not in myClusterProps.keys()):
        time.sleep(5)
        print('#', end='')
        myClusterProps = redshift.describe_clusters(
                         ClusterIdentifier=l.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    else:
        print('\n\nCluster Endpoint AVAILABLE:')
        print(myClusterProps['Endpoint'])  
except Exception as e:
    print(e)
    



