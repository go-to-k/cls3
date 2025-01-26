#!/bin/bash
set -eu

cd $(dirname $0)

profile=""
bucket_prefix="cls3-test"
profile_option=""
num_buckets=10
objects_per_bucket=10000
# objects_per_bucket=1000000

while getopts p:b:n:o: OPT; do
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
	o)
		objects_per_bucket="$OPTARG"
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
	echo "number of buckets (-n) must be less than or equal to 10 for directory buckets"
	exit 1
fi

if ! [[ "${objects_per_bucket}" =~ ^[0-9]+$ ]]; then
	echo "number of objects (-o) must be a positive integer"
	exit 1
fi

region="us-east-1"
az_id="use1-az4"

if [ -n "${profile}" ]; then
	profile_option="--profile ${profile} --region ${region}"
fi

# Calculate iterations considering 1000 files per iteration
iterations=$((objects_per_bucket / 1000))
if [ ${iterations} -lt 1 ]; then
	iterations=1
fi

random_suffix=$RANDOM
padded_start=$(printf "%02d" 1)
padded_end=$(printf "%02d" ${num_buckets})
echo "=== buckets: ${bucket_prefix}-${random_suffix}-[${padded_start}-${padded_end}]--${az_id}--x-s3 ==="
echo "=== objects: $(printf "%'d" ${objects_per_bucket}) per bucket ==="

pids=()
for bucket_num in $(seq 1 ${num_buckets}); do
	(
		padded_num=$(printf "%02d" ${bucket_num})
		bucket_name="${bucket_prefix}-${random_suffix}-${padded_num}"
		# naming for S3 Express One Zone (using az 4 in es-east-1)
		lower_bucket_name=$(echo "${bucket_name}--${az_id}--x-s3" | tr '[:upper:]' '[:lower:]')

		if [ -z "$(aws s3api list-directory-buckets ${profile_option} | grep ${lower_bucket_name})" ]; then
			aws s3api create-bucket \
				--bucket ${lower_bucket_name} \
				--create-bucket-configuration "Location={Type=AvailabilityZone,Name=${az_id}},Bucket={DataRedundancy=SingleAvailabilityZone,Type=Directory}" \
				${profile_option} >/dev/null
		fi

		dir="./testfiles/${lower_bucket_name}"
		mkdir -p ${dir}

		# about ${objects_per_bucket} objects on the directory buckets for S3 Express One Zone
		## NOTE: It'll cost you $2.5 (S3 Express One Zone: 0.0025USD USD / 1000 PUT)
		## NOTE: You can create up to 10 directory buckets in each of your AWS accounts.
		## FIXME: Errors often occur when uploading files to S3 Express One Zone. (Retrying fixes it, but it happens again with another file.)
		### upload failed: testfiles/test-goto-01/1_864_21587.txt to s3://test-goto-01--use1-az4--x-s3/1_864_21587.txt An error occurred (400) when calling the PutObject operation: Bad Request
		for i in $(seq 1 ${iterations}); do
			touch ${dir}/${i}_{1..1000}_${RANDOM}.txt

			## Do not finish even in the event of an error because the above error will occur.
			set +e
			aws s3 cp ${dir} s3://${lower_bucket_name}/ --recursive ${profile_option} >/dev/null
			set -e

			rm ${dir}/*.txt
		done

		rm -rf ${dir}
	) &
	pids[$!]=$!
done

wait "${pids[@]}"
