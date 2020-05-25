#!/opt/conda/bin/python
"""
cluster_delete.py: Sample Adhoc query execution script
- Detach IAM Role
- Delete Cluster
"""
import boto3
import time
import loadconfigs as l
import pandas as pd

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
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes",
                  'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


# Redhift Client
redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=l.KEY,
                            aws_secret_access_key=l.SECRET
)


# IAM Client
iam = boto3.client('iam',aws_access_key_id=l.KEY,
                    aws_secret_access_key=l.SECRET,
                    region_name="us-west-2"
)



# Detaching IAM Role    
try:
    print('Detaching IAM Role')    
    iam.detach_role_policy(RoleName=l.DWH_IAM_ROLE_NAME, 
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
except Exception as e:
    print(e)



# Delete Cluster & IAM Role
try:
    print('Deleting Cluster')
    redshift.delete_cluster(ClusterIdentifier=l.DWH_CLUSTER_IDENTIFIER, 
                            SkipFinalClusterSnapshot=True)
except Exception as e:
    print(e)


# Display Cluster Status
try:    
    myClusterProps = redshift.describe_clusters(
    ClusterIdentifier=l.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print(prettyRedshiftProps(myClusterProps))    
    
    # Keep checking status till Deleting finishes appears
    print('\nDeleting Cluster...',end='')
    while(myClusterProps['ClusterStatus'] == 'deleting'):
        time.sleep(5)
        print('#', end='')
        myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=l.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
except Exception as e:
    print(e)
finally:
    print('DELETED. Cluster not present now.')


