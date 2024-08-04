#!/bin/bash
set -eu

cd $(dirname $0)

profile=""
bucket_name=""
profile_option=""

while getopts p:b:d: OPT; do
	case $OPT in
	p)
		profile="$OPTARG"
		;;
	b)
		bucket_name="$OPTARG"
		;;
	*) ;;
	esac
done

if [ -z "${bucket_name}" ]; then
	echo "bucket_name option (-b) is required"
	exit 1
fi

region="us-east-1"
az_id="use1-az4"
# naming for S3 Express One Zone (using az 4 in es-east-1)
lower_bucket_name=$(echo "${bucket_name}--${az_id}--x-s3" | tr '[:upper:]' '[:lower:]')

if [ -n "${profile}" ]; then
	profile_option="--profile ${profile} --region ${region}"
fi

if [ -z "$(aws s3 ls ${profile_option} | grep ${lower_bucket_name})" ]; then
	aws s3api create-bucket \
		--bucket ${lower_bucket_name} \
		--create-bucket-configuration "Location={Type=AvailabilityZone,Name=${az_id}},Bucket={DataRedundancy=SingleAvailabilityZone,Type=Directory}" \
		${profile_option}
fi

dir="./testfiles/${bucket_name}"
mkdir -p ${dir}

# about 1000,000 objects on the directory buckets for S3 Express One Zone
## NOTE: It'll cost you $2.5 (S3 Express One Zone: 0.0025USD USD / 1000 PUT)
for i in $(seq 1 1000); do
	touch ${dir}/${i}_{1..1000}_${RANDOM}.txt

	aws s3 cp ${dir} s3://${lower_bucket_name}/ --recursive ${profile_option} >/dev/null

	rm ${dir}/*.txt
done

rm -rf ${dir}
