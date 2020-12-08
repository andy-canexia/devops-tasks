# Ticket: https://contextualgenomics.atlassian.net/browse/SUP-518
#
# Author: Andy Bo Wu
# Date: Dec 7, 2020

import boto3
import re

# low-level aws service access
s3_client = boto3.client('s3')
# high-level, object-oriented API
s3_resource = boto3.resource('s3')

src_bucket_name = 'cg-rd'
level_one_folder = 'output/'

client_bucket = s3_resource.Bucket('cg-rd-for-sam')



def read_list_files():
    pass


def canexia_worker():
    read_list_files()



    result = s3_client.list_objects(Bucket=src_bucket_name,
                                    Prefix='output/180328_M02348_0164_000000000-BM6GH_version8/BLANK-CRCQV34Run002-1_S1/',
                                    Delimiter='/')
    # for obj in result.get('CommonPrefixes'):
        # print(obj.get('Prefix').split('/')[2])

    # RE-MATCH "*.hardclipped.bam.bai"
    pattern = re.compile(r"[a-zA-Z0-9\t ./,<>?;:\"'`!@#$%^&*()\[\]{}_+=|\\-]+\.hardclipped\.bam\.bai$")
    # RE-MATCH "*.hardclipped.bam"
    pattern2 = re.compile(r"[a-zA-Z0-9\t ./,<>?;:\"'`!@#$%^&*()\[\]{}_+=|\\-]+\.hardclipped\.bam$")
    # RE-MATCH "*.hardclipped.bam.prediction_result.vcf"
    pattern3 = re.compile(r"[a-zA-Z0-9\t ./,<>?;:\"'`!@#$%^&*()\[\]{}_+=|\\-]+\.hardclipped\.bam\.prediction_result\.vcf$")

    for k in result["Contents"]:
        file_name = k["Key"].split('/')[-1]
        match   = bool(re.match(pattern, file_name))
        match2  = bool(re.match(pattern2, file_name))
        match3  = bool(re.match(pattern3, file_name))

        if match or match2 or match3:
            copy_source = {
                'Bucket'    : src_bucket_name,
                'Key'       : 'output/180328_M02348_0164_000000000-BM6GH_version8/BLANK-CRCQV34Run002-1_S1/' + k["Key"].split('/')[-1]
            }
            client_bucket.copy(copy_source, 'output/180328_M02348_0164_000000000-BM6GH_version8/BLANK-CRCQV34Run002-1_S1/' + k["Key"].split('/')[-1])



if __name__ == '__main__':
     canexia_worker()


