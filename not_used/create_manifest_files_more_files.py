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

def write_manifest(s3, bucket_name, s3_bucket):
    song_data = {'entries': []}
    log_data = {'entries': []}
    kwargs = {'Bucket': bucket_name}
    i=0
    
    while True:      
        resp = s3.list_objects_v2(**kwargs)
        bucketContents = resp.get('Contents')
        print(resp.get('Name'))
        for c in bucketContents: 
            key = c.get('Key')
            if key.endswith('.json'):
                i += 1
                print(i, key)
                if key.startswith('song'):
                    song_data['entries'].append({
                                        'url': '/'.join(['s3:/', bucket_name, key]),
                                        'mandatory': True
                                        })
                elif key.startswith('log'):
                    log_data['entries'].append({
                                        'url': '/'.join(['s3:/', bucket_name, key]),
                                        'mandatory': True
                                        })
          
                
            
        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        # Source: https://alexwlchan.net/2018/01/listing-s3-keys-redux/
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    # write manifest to a file (remove the previous manifest file if it already exists)
    silentremove('manifests/'+'song-data'+'.manifest')

    with open('manifests/'+'song-data'+'.manifest', 'w') as f:
        json.dump(song_data, f)
    s3.upload_file('manifests/'+'song-data'+'.manifest', s3_bucket, 'song-data'+'.manifest' )  
    
    silentremove('manifests/'+'log-data'+'.manifest')

    with open('manifests/'+'log-data'+'.manifest', 'w') as f:
        json.dump(log_data, f)
    s3.upload_file('manifests/'+'log-data'+'.manifest', s3_bucket, 'log-data'+'.manifest' )   
    

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
    

    # write manifests file for log-data and song-data
    write_manifest(s3, bucket_name, S3_BUCKET)

    
    

if __name__ == "__main__":
    main()