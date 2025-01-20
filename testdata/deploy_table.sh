#!/bin/bash
set -eu

cd $(dirname $0)

profile=""
bucket_name=""
option=""
profile_option=""
region="us-east-1"
region_option="--region ${region}"

while getopts p:b:r: OPT; do
	case $OPT in
	p)
		profile="$OPTARG"
		;;
	b)
		bucket_name="$OPTARG"
		;;
	r)
		region="$OPTARG"
		;;
	*) ;;
	esac
done

if [ -z "${bucket_name}" ]; then
	echo "bucket_name option (-b) is required"
	exit 1
fi

lower_bucket_name=$(echo "${bucket_name}" | tr '[:upper:]' '[:lower:]')

if [ -n "${profile}" ]; then
	profile_option="--profile ${profile}"
fi
if [ -n "${region}" ]; then
	region_option="--region ${region}"
fi
if [ -n "${profile_option}" ] || [ -n "${region_option}" ]; then
	option="${region_option} ${profile_option}"
fi

exist=$(aws s3tables list-table-buckets ${option} | jq -r '.tableBuckets[] | select(.name == "'${lower_bucket_name}'")' || true)
if [ -z "${exist}" ]; then
	aws s3tables create-table-bucket --name ${lower_bucket_name} ${option}
fi

account_id=$(aws sts get-caller-identity --query Account --output text ${option})

table_bucket_arn="arn:aws:s3tables:${region}:${account_id}:bucket/${lower_bucket_name}"

aws s3tables create-namespace --table-bucket-arn ${table_bucket_arn} --namespace "my_namespace_1" ${option}
aws s3tables create-namespace --table-bucket-arn ${table_bucket_arn} --namespace "my_namespace_2" ${option}

aws s3tables create-table --table-bucket-arn ${table_bucket_arn} --namespace "my_namespace_1" --name "my_table_1" --format "ICEBERG" ${option}
aws s3tables create-table --table-bucket-arn ${table_bucket_arn} --namespace "my_namespace_1" --name "my_table_2" --format "ICEBERG" ${option}
aws s3tables create-table --table-bucket-arn ${table_bucket_arn} --namespace "my_namespace_2" --name "my_table_1" --format "ICEBERG" ${option}
aws s3tables create-table --table-bucket-arn ${table_bucket_arn} --namespace "my_namespace_2" --name "my_table_2" --format "ICEBERG" ${option}
