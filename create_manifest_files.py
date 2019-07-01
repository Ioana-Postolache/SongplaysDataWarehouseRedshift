import boto3
import configparser
import json
import os, errno


def silentremove(filename):
    '''Source: https://stackoverflow.com/questions/10840533/most-pythonic-way-to-delete-a-file-which-may-not-exist'''
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise

def write_manifest(s3, bucket_name, prefix, filename, s3_bucket):
    manifest = {'entries': []}
    kwargs = {'Bucket': bucket_name, 'Prefix': prefix}
    
    
    while True:
        i = 0
        
        resp = s3.list_objects_v2(**kwargs)
        bucketContents = resp.get('Contents')
        for c in bucketContents:
            key = c.get('Key')
            if key.endswith('.json') and i<4:
                manifest['entries'].append({
                                    'url': '/'.join(['s3:/', bucket_name, key]),
                                    'mandatory': True
                                    })
                i += 1
                
            
        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        # Source: https://alexwlchan.net/2018/01/listing-s3-keys-redux/
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    # write manifest to a file (remove the previous manifest file if it already exists)
    silentremove('manifests/'+filename)

    with open('manifests/'+filename, 'w') as f:
        json.dump(manifest, f)
        
    s3.upload_file('manifests/'+filename, s3_bucket, filename )  
    

def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    S3_BUCKET              = config.get("S3","S3_PERSONAL_BUCKET")

    s3 = boto3.client('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )

    bucket_name = "udacity-dend"
    s3.download_file(bucket_name,'log_data/2018/11/2018-11-01-events.json','2018-11-01-events.json')
    # write manifests file for log-data and song-data
#     write_manifest(s3, bucket_name, 'log-data', 'log-data.manifest', S3_BUCKET)
#     write_manifest(s3, bucket_name, 'song-data', 'song-data.manifest', S3_BUCKET)
    

if __name__ == "__main__":
    main()