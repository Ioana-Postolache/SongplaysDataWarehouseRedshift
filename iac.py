import configparser
import json
import boto3
from botocore.exceptions import ClientError
import time
 


def createResource(key, secret, region, resource):
    return boto3.client(resource,
                       region_name=region,
                       aws_access_key_id=key,
                       aws_secret_access_key=secret
                    )

def createRole(iam, role):
    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=role,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e) 
    # Attaching Policy
    iam.attach_role_policy(RoleName=role,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    # Get the IAM role ARN
    iam.get_role(RoleName=role)['Role']['Arn']

    
def createCluster(redshift, clusterType, nodeType, numberOfNodes, dbName, clusterIdentifier, masterUsername, masterUserPassword, iamRoles):
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=clusterType,
            NodeType=nodeType,
            NumberOfNodes=numberOfNodes,

            #Identifiers & Credentials
            DBName=dbName,
            ClusterIdentifier=clusterIdentifier,
            MasterUsername=masterUsername,
            MasterUserPassword=masterUserPassword,

            #Roles (for s3 access)
            IamRoles=iamRoles
        )
    except Exception as e:
        print(e)

def openIncomingPort(ec2, myClusterProps, port):
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            #GroupName=defaultSg.group_name,
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=port,
            ToPort=port
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print("Permission already exists")
    else:
        print("Unexpected error: %s" % e)
        
def main():
    # Load DWH Params from a file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')    
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    
    
    # Create clients for EC2, S3, IAM, and Redshift
    ec2 = boto3.resource('ec2',
                       region_name='us-west-2',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    s3 = createResource(KEY, SECRET, "us-west-2", 's3')
    iam = createResource(KEY, SECRET, "us-west-2", 'iam')
    redshift = createResource(KEY, SECRET, "us-west-2", 'redshift')
    
    # Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    createRole(iam, DWH_IAM_ROLE_NAME)  
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME).get('Role').get('Arn')   
    print(roleArn)   
    
    # Add DWH_ROLE_ARN in config file
    config.set('IAM_ROLE','ARN', roleArn)
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
       
    
    # Create a RedShift Cluster
    createCluster(redshift, DWH_CLUSTER_TYPE,
            DWH_NODE_TYPE,
            int(DWH_NUM_NODES),
            DWH_DB,
            DWH_CLUSTER_IDENTIFIER,
            DWH_DB_USER,
            DWH_DB_PASSWORD,
            [roleArn] )
    
    for i in range(0, 3, 1):
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        state= myClusterProps.get('ClusterStatus')
        print('cluster status: '+ state)
        
        if(state == 'available'):
            print('The cluster has been created and it is available for use.')
            DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
            DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
            config.set('IAM_ROLE','ARN', roleArn)
            
            # Add configurations needed for create_tables.py in config file
            config.set('CLUSTER','host', DWH_ENDPOINT)
            config.set('CLUSTER','db_name', DWH_DB)
            config.set('CLUSTER','db_user', DWH_DB_USER)
            config.set('CLUSTER','db_password', DWH_DB_PASSWORD)
            config.set('CLUSTER','db_port', DWH_PORT)
            
            with open('dwh.cfg', 'w') as configfile:
                config.write(configfile)
                
            print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
            print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
            
            # Open an incoming TCP port to access the cluster ednpoint
            openIncomingPort(ec2, myClusterProps, int(DWH_PORT))
            break
        else:
            # Wait for 5 minutes
            time.sleep(300)
        

if __name__ == "__main__":
    main()