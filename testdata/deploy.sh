#!/bin/bash
set -eu

cd $(dirname $0)

profile=""
bucket_name=""
profile_option=""

while getopts p:b: OPT; do
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

lower_bucket_name=$(echo "${bucket_name}" | tr '[:upper:]' '[:lower:]')

if [ -n "${profile}" ]; then
	profile_option="--profile ${profile}"
fi

if [ -z "$(aws s3 ls ${profile_option} | grep ${lower_bucket_name})" ]; then
	aws s3 mb s3://${lower_bucket_name} ${profile_option}
fi

aws s3api put-bucket-versioning --bucket ${lower_bucket_name} --versioning-configuration Status=Enabled

dir="./testfiles"
mkdir -p ${dir}

# about 300,000 versions
for i in $(seq 1 100); do
	touch ${dir}/${i}_{1..1000}_${RANDOM}.txt

	aws s3 cp ${dir} s3://${lower_bucket_name}/ --recursive ${profile_option} >/dev/null
	aws s3 cp ${dir} s3://${lower_bucket_name}/ --recursive ${profile_option} >/dev/null # version
	aws s3 rm s3://${lower_bucket_name}/ --recursive ${profile_option} >/dev/null        # delete marker

	rm ${dir}/*.txt
done

rm -rf ${dir}
