import configparser
import boto3
import time


def main():
    # Load DWH Params from a file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')    
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")  
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    
    
    # Create clients for Redshift, IAM
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name="us-west-2"
                  )
    
    # Delete cluster
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
    # Run this block several times until the cluster is really deleted
    for i in range(0, 5, 1):
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            state= myClusterProps.get('ClusterStatus')
            print('cluster status: '+ state)
            # Wait for 5 minutes
            time.sleep(300)
        except redshift.exceptions.ClusterNotFoundFault:
            # if we get ClusterNotFoundFault, then the cluster has been deleted
            print('The cluster has been deleted.')
            break
    
    # Delete role
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    print('The role has been deleted.')
        

if __name__ == "__main__":
    main()