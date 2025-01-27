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

if ! [[ "${objects_per_bucket}" =~ ^[0-9]+$ ]]; then
	echo "number of objects (-o) must be a positive integer"
	exit 1
fi

if [ -n "${profile}" ]; then
	profile_option="--profile ${profile}"
fi

# Calculate iterations considering 4 versions (3 copies and 1 deletion) and 1000 files
iterations=$((objects_per_bucket / (4 * 1000)))
if [ ${iterations} -lt 1 ]; then
	iterations=1
fi

random_suffix=$RANDOM
padded_start=$(printf "%04d" 1)
padded_end=$(printf "%04d" ${num_buckets})
echo "=== buckets: ${bucket_prefix}-${random_suffix}-[${padded_start}-${padded_end}] ==="
echo "=== versions: $(printf "%'d" ${objects_per_bucket}) per bucket ==="

pids=()
for bucket_num in $(seq 1 ${num_buckets}); do
	(
		padded_num=$(printf "%04d" ${bucket_num})
		bucket_name="${bucket_prefix}-${random_suffix}-${padded_num}"
		lower_bucket_name=$(echo "${bucket_name}" | tr '[:upper:]' '[:lower:]')

		exist=$(aws s3 ls ${profile_option} | grep -E " ${lower_bucket_name}$" || true)
		if [ -z "${exist}" ]; then
			aws s3 mb s3://${lower_bucket_name} ${profile_option} >/dev/null
		fi

		aws s3api put-bucket-versioning --bucket ${lower_bucket_name} --versioning-configuration Status=Enabled ${profile_option}

		dir="./testfiles/${bucket_name}"
		mkdir -p ${dir}

		# about ${objects_per_bucket} versions
		## NOTE: e.g.) For 1,000,000 versions per bucket, it'll cost you about $3.75 (0.005 USD / 1000 PUT)(DELETE operation is free)
		## NOTE: 1,000,000 versions = 250,000 objects × 4 versions (3 PUT operations and 1 DELETE operation per object)
		for i in $(seq 1 ${iterations}); do
			touch ${dir}/${i}_{1..1000}_${RANDOM}.txt

			# version
			version_pids=()
			for _ in $(seq 1 3); do
				aws s3 cp ${dir} s3://${lower_bucket_name}/ --recursive ${profile_option} >/dev/null &
				version_pids[$!]=$!
			done
			wait "${version_pids[@]}"

			# delete marker
			aws s3 rm s3://${lower_bucket_name}/ --recursive ${profile_option} >/dev/null

			rm ${dir}/*.txt
		done

		rm -rf ${dir}
	) &
	pids[$!]=$!

	# Wait for every 10 processes to complete before starting new ones
	if [ ${#pids[@]} -eq 10 ]; then
		wait "${pids[@]}"
		pids=()
	fi
done

# Wait for remaining processes
if [ ${#pids[@]} -gt 0 ]; then
	wait "${pids[@]}"
fi
