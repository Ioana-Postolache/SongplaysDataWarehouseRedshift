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

def write_manifest(s3, bucket_name, prefix, filename):
    manifest = {'entries': []}
    kwargs = {'Bucket': bucket_name, 'Prefix': prefix}
    
    while True:
        resp = s3.list_objects_v2(**kwargs)
        bucketContents = resp.get('Contents')
        for c in bucketContents:
            manifest['entries'].append({
                                'url': '/'.join(['s3:/', bucket_name, c.get('Key')]),
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
    silentremove('manifests/'+filename)
    print(json.dumps(manifest))
    with open('manifests/'+filename, 'w') as f:
        json.dump(manifest, f)


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    s3 = boto3.client('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                         )

    bucket_name = "udacity-dend"
    
    # write manifests file for log-data and song-data
    write_manifest(s3, bucket_name, 'log-data', 'log-data.json')
    write_manifest(s3, bucket_name, 'song-data', 'song-data.json')
        

if __name__ == "__main__":
    main()