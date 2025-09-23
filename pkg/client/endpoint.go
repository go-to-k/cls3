package client

import "strings"

func IsCloudflareR2Endpoint(endpointUrl string) bool {
	// Cloudflare R2 S3-compatible endpoints only
	// Format: https://<account_id>.r2.cloudflarestorage.com
	return strings.Contains(strings.ToLower(endpointUrl), "r2.cloudflarestorage.com")
}
