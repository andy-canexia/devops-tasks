# Ticket: https://contextualgenomics.atlassian.net/browse/SUP-518
#
# Author: Andy Bo Wu
# Date: Dec 7, 2020

import boto3
import re
from collections import defaultdict

# low-level aws service access
s3_client = boto3.client('s3')
# high-level, object-oriented API
s3_resource = boto3.resource('s3')

src_bucket_name     = 'cg-rd'
level_one_folder    = 'output/'
# target_file_name    = 'list.txt'
target_file_name    = 'list_test.txt'
array_target_run = []
dict_subfolder_target_run = defaultdict(list)

client_bucket = s3_resource.Bucket('cg-rd-for-sam')



def read_list_files(fn):
    local_array = []
    with open(fn, 'r') as f:
        for line in f:
            local_array.append(line.strip("\n"))
    return local_array


def retrieve_subfolders(bucket, local_folder, array_fn):
    local_dict = defaultdict(list)
    for fn in array_fn:
        response = s3_client.list_objects(Bucket    = bucket,
                                          Prefix    = local_folder + fn + '/', # THE ENDING `/` IS A MUST!!
                                          Delimiter ='/')
        for obj in response.get('CommonPrefixes'):
            # print(obj.get('Prefix').split('/')[2])
            local_dict[fn].append(obj.get('Prefix').split('/')[2])

    return local_dict


def retrive_filter_copy_files(dict_subfolder_target_run):
    pass


def canexia_worker():
    # read target list, and put file names into an array
    array_target_run = read_list_files(target_file_name)

    # retrieve name of subfolders in each target run
    dict_subfolder_target_run = retrieve_subfolders(src_bucket_name, level_one_folder, array_target_run)

    print(dict_subfolder_target_run)

    # retrieve all files, and do filtering using RE, anc copy to a new bucket
    retrive_filter_copy_files(dict_subfolder_target_run)

    result = s3_client.list_objects(Bucket=src_bucket_name,
                                    Prefix='output/180328_M02348_0164_000000000-BM6GH_version8/BLANK-CRCQV34Run002-1_S1/',
                                    Delimiter='/')


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
            # client_bucket.copy(copy_source, 'output/180328_M02348_0164_000000000-BM6GH_version8/BLANK-CRCQV34Run002-1_S1/' + k["Key"].split('/')[-1])



if __name__ == '__main__':
     canexia_worker()


