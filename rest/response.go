/*
 * Copyright 2016 ClusterHQ
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package rest provides the building blocks for implementing the V1 HTTP REST
// API handlers
package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pborman/uuid"
)

// HeaderRequestID is the header containing the request ID
const HeaderRequestID = "VH-RequestID"

// Response is the container for JSON data returned by the V1 API
type Response struct {
	// The unique identifier, assigned by the server for the request
	RequestID uuid.UUID `json:"request_id"`
	// Method is the HTTP verb used in the request
	Method string `json:"method"`
	// RequestPath is the URI of the requested resource(s)
	RequestPath string `json:"request_path"`
	// Params should contains the request parameters as well as any parameters
	// that the particular API call fulfilled with default values
	Params map[string]interface{} `json:"params,omitempty"`
	// Success indicates the overall success of the request; if false, there are
	// one or more issues that occurred during the request.
	Success bool `json:"success"`
	// ProcessingTime indicates the number of seconds required to process and
	// fulfill the request
	ProcessingTime float64 `json:"processing_time"`
	// Result the result of a successful request
	Result json.RawMessage `json:"result,omitempty"`
	// ErrorMessage is a message returned for unsuccessful requests
	ErrorMessage string `json:"error_message,omitempty"`

	// unexported fields
	startTime time.Time
}

// NewResponse initializes a Response object, setting a UUID for the request,
// tracking the start time for the request and filling the request path and
// method. The Params field is not filled out automatically as the caller
// may choose to ignore some parameters provided.
func NewResponse(r *http.Request) *Response {
	return &Response{
		startTime:   time.Now(),
		RequestID:   uuid.NewRandom(),
		Success:     true,
		Method:      r.Method,
		RequestPath: r.URL.Path,
		Params:      make(map[string]interface{}),
	}
}

// SetResult adds a value to the Result field.
func (r *Response) SetResult(value interface{}) error {
	// encode the value as JSON
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}

	r.Result = json.RawMessage(encoded)
	return nil
}

// GetResult retrieves the raw JSON result from the Result field and decodes the
// value into the given value pointer
func (r *Response) GetResult(value interface{}) error {
	return json.Unmarshal(r.Result, value)
}

// SetGenericServerError sets a generic error message indicating that
// the server encountered an error that prevented a successful response. The
// Success and ErrorMessage fields are set.
func (r *Response) SetGenericServerError() {
	r.Success = false
	r.ErrorMessage = "An error occurred, please try again later"
}

// SetUnauthorizedError sets an error message indicating that the current
// authenticated user is not authorized for the current request. The Success
// and ErrorMessage fields are set.
func (r *Response) SetUnauthenticatedError() {
	r.Success = false
	r.ErrorMessage = "Unauthenticated"
}

// SetUnauthorizedError sets an error message indicating that the current
// authenticated user is not authorized for the current request. The Success
// and ErrorMessage fields are set.
func (r *Response) SetUnauthorizedError() {
	r.Success = false
	r.ErrorMessage = "Unauthorized"
}

// SetNotFoundError sets an error message indicating that one or more
// of the records refered to were not found in the system. The Success and
// ErrorMessage fields are set.
func (r *Response) SetNotFoundError() {
	r.Success = false
	r.ErrorMessage = "Not found"
}

// SetError sets an error message an sets success to false
func (r *Response) SetError(message string) {
	r.Success = false
	r.ErrorMessage = message
}

// Write writes the JSON form of the Response including the sending of the
// HTTP status code.
func (r *Response) Write(status int, w http.ResponseWriter) error {
	endTime := time.Now()
	r.ProcessingTime = endTime.Sub(r.startTime).Seconds()

	body, err := json.Marshal(r)
	if err != nil {
		// failed to marshal, probably a bug, just respond with a 500
		w.WriteHeader(http.StatusInternalServerError)
		// write a minimal response
		w.Write([]byte(`{"success":false,"error_message":"internal error"}`))
		return fmt.Errorf("Failed to marshal response JSON: %v\n", err)
	}

	w.Header().Set(HeaderRequestID, r.RequestID.String())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, err = w.Write(body)
	if err != nil {
		// probably the client disconnected
		return fmt.Errorf("Failed to write response JSON: %v\n", err)
	}

	return nil
}

// ResultMap is a type alias for setting a result simply
type ResultMap map[string]interface{}

// SetParams translates the Query structure into a params for http response.
// Note: Currently, we exploit JSON package to achieve this. Yes, we may incur
// some cost in a Marshal and Unmarshal step, but this avoids ugly type assertion
// jumble. In another word, the code is cleaner in this manner.
func (r *Response) SetParams(in interface{}) error {
	binary, err := json.Marshal(in)
	if err != nil {
		return err
	}

	var params interface{}
	err = json.Unmarshal(binary, &params)
	if err != nil {
		return err
	}

	r.Params = params.(map[string]interface{})
	return nil
}
