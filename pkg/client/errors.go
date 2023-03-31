package client

import "fmt"

var _ error = (*ClientError)(nil)

// ClientError provides the error with a resource name
type ClientError struct {
	ResourceName *string
	Err          error
}

func (e *ClientError) Error() string {
	var msg string
	if e.ResourceName == nil {
		msg = fmt.Sprintf("[resource -] %v", e.Err)
	} else {
		msg = fmt.Sprintf("[resource %s] %v", *e.ResourceName, e.Err)
	}
	return msg
}

func (e *ClientError) Unwrap() error {
	return e.Err
}
