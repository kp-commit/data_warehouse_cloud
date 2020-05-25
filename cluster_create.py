#!/opt/conda/bin/python
"""
create_cluster.py: Create Redshift Cluster
- Configuration loaded from dwh.cfg (Cluster settings, AWS KEY/SECRET, S3 resources)
- IAM role setup
- Policy attached
- Role arn generated
- TCP port opened in VPC
- Cluster Endpoint created
"""
import boto3
import time
import json
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
        "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


# Create clients for EC2, S3, IAM, and Redshift
ec2 = boto3.resource('ec2',
	                	region_name="us-west-2",
	                	aws_access_key_id=l.KEY,
	                	aws_secret_access_key=l.SECRET
)

s3 = boto3.resource('s3',
						region_name="us-west-2",
						aws_access_key_id=l.KEY,
						aws_secret_access_key=l.SECRET
)

iam = boto3.client('iam',
						aws_access_key_id=l.KEY,
						aws_secret_access_key=l.SECRET,
						region_name="us-west-2"
)

redshift = boto3.client('redshift',
	            			region_name="us-west-2",
	            			aws_access_key_id=l.KEY,
	              			aws_secret_access_key=l.SECRET
)


# Create the IAM role
try:
    print('Creating a new IAM Role...', end='')
    dwhRole = iam.create_role(
        Path='/',
        RoleName=l.DWH_IAM_ROLE_NAME,
        Description="Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
                           'Effect': 'Allow',
                           'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )
    print('Done')
except Exception as e:
    print(e)


# Attach Policy
try:
    print('Attaching Policy to IAM Role...', end='')

    iam.attach_role_policy(RoleName=l.DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print('Done')

    # Get and print the IAM role ARN
    print("IAM role ARN: ", end='')
    roleArn = iam.get_role(RoleName=l.DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(roleArn)

    # set Config for DWH_ROLE_ARN
    l.setConfigs('DWH', 'DWH_ROLE_ARN', roleArn)
    l.writeConfigs('dwh.cfg', 'wt')

except Exception as e:
    print(e)


# Create the Redshift cluster
try:
    print('Creating Cluster', end='')
    response = redshift.create_cluster(
        # HW
        ClusterType=l.DWH_CLUSTER_TYPE,
        NodeType=l.DWH_NODE_TYPE,
        NumberOfNodes=int(l.DWH_NUM_NODES),

        # Identifiers & Credentials
        DBName=l.DWH_DB,
        ClusterIdentifier=l.DWH_CLUSTER_IDENTIFIER,
        MasterUsername=l.DWH_DB_USER,
        MasterUserPassword=l.DWH_DB_PASSWORD,

        # Roles (for s3 access)
        IamRoles=[roleArn]
    )
    print('...in progress...')

except Exception as e:
    print(e)


# Display Cluster Status
try:
    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=l.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print(prettyRedshiftProps(myClusterProps))
    print('Cluster creating, waiting for endpoint...', end='')

    # Keep checking status till Endpoint appears
    while('Endpoint' not in myClusterProps.keys() or myClusterProps['ClusterStatus'] == 'creating'):
        time.sleep(5)
        print('#', end='')
        myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=l.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print()

    # Once Endpoint available, open TCP port in VPC
    try:
        while(myClusterProps['ClusterStatus']=='available'):
            print('Opening TCP port in VPC')
            vpc=ec2.Vpc(id=myClusterProps['VpcId'])
            defaultSg=list(vpc.security_groups.all())[0]
            print(defaultSg)
        
            defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(l.DWH_PORT),
            ToPort=int(l.DWH_PORT))
    except Exception as e:
        print(e)        

    endpoint = myClusterProps['Endpoint']
    DWH_ENDPOINT = endpoint['Address']

    # set Config for DWH_ENDPOINT
    l.setConfigs('DWH', 'DWH_ENDPOINT', DWH_ENDPOINT)
    l.writeConfigs('dwh.cfg', 'wt')

    # Cluster ready for access
    print('Endpoint AVAILABLE:', DWH_ENDPOINT)            

except Exception as e:
    print(e)