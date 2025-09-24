package endpoint

import "strings"

func IsCloudflareR2Endpoint(endpointUrl string) bool {
	if endpointUrl == "" {
		return false
	}

	// Cloudflare R2 S3-compatible endpoints only
	// Format: https://<account_id>.r2.cloudflarestorage.com
	return strings.Contains(strings.ToLower(endpointUrl), "r2.cloudflarestorage.com")
}

func IsAWSS3Endpoint(endpointUrl string) bool {
	if endpointUrl == "" {
		return true
	}

	lowerUrl := strings.ToLower(endpointUrl)

	// AWS S3 endpoint patterns:
	// - s3.amazonaws.com
	// - s3.<region>.amazonaws.com
	// - s3-<region>.amazonaws.com (legacy format)
	// - <bucket>.s3.amazonaws.com (virtual hosted-style, but we check the domain part)
	// - <bucket>.s3.<region>.amazonaws.com (virtual hosted-style, but we check the domain part)
	return strings.Contains(lowerUrl, ".amazonaws.com") &&
		(strings.Contains(lowerUrl, "s3.") || strings.Contains(lowerUrl, "s3-"))
}
