#!/bin/bash
set -eu

cd $(dirname $0)

profile=""
bucket_prefix="cls3-test"
profile_option=""
num_buckets=10
region="us-east-1"
region_option="--region ${region}"
option=""

while getopts p:b:n:r: OPT; do
	case $OPT in
	p)
		profile="$OPTARG"
		;;
	b)
		bucket_prefix="$OPTARG"
		;;
	n)
		num_buckets="$OPTARG"
		;;
	r)
		region="$OPTARG"
		;;
	*) ;;
	esac
done

if [ -z "${bucket_prefix}" ]; then
	echo "bucket_prefix option (-b) is required"
	exit 1
fi

if ! [[ "${num_buckets}" =~ ^[0-9]+$ ]]; then
	echo "number of buckets (-n) must be a positive integer"
	exit 1
fi

if [ "${num_buckets}" -gt 10 ]; then
	echo "number of buckets (-n) must be less than or equal to 10 for table buckets"
	exit 1
fi

if [ -n "${profile}" ]; then
	profile_option="--profile ${profile}"
fi
if [ -n "${region}" ]; then
	region_option="--region ${region}"
fi
if [ -n "${profile_option}" ] || [ -n "${region_option}" ]; then
	option="${region_option} ${profile_option}"
fi

random_suffix=$RANDOM
padded_start=$(printf "%02d" 1)
padded_end=$(printf "%02d" ${num_buckets})
echo "=== buckets: ${bucket_prefix}-${random_suffix}-[${padded_start}-${padded_end}] ==="

for bucket_num in $(seq 1 ${num_buckets}); do
	padded_num=$(printf "%02d" ${bucket_num})
	bucket_name="${bucket_prefix}-${random_suffix}-${padded_num}"
	lower_bucket_name=$(echo "${bucket_name}" | tr '[:upper:]' '[:lower:]')

	exist=$(aws s3tables list-table-buckets ${option} | jq -r '.tableBuckets[] | select(.name == "'${lower_bucket_name}'")' || true)
	if [ -z "${exist}" ]; then
		aws s3tables create-table-bucket --name ${lower_bucket_name} ${option}
	fi

	account_id=$(aws sts get-caller-identity --query Account --output text ${option})

	table_bucket_arn="arn:aws:s3tables:${region}:${account_id}:bucket/${lower_bucket_name}"

	# 10 * 10 = 100 (max size of tables per table bucket)
	for i in {1..10}; do
		aws s3tables create-namespace \
			--table-bucket-arn ${table_bucket_arn} \
			--namespace "my_namespace_${i}" ${option} >/dev/null

		for table in {1..10}; do
			aws s3tables create-table \
				--table-bucket-arn ${table_bucket_arn} \
				--namespace "my_namespace_${i}" \
				--name "my_table_${table}" \
				--format "ICEBERG" ${option} >/dev/null &
		done
		wait
	done
done
