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

package protocols

import (
	"net/http"

	"github.com/pborman/uuid"
)

const (
	// correlationIDKey is the field key to CorrelationID.
	correlationIDKey = "correlation_id"
)

// SetCorrelationID sets the correslation ID for the header.
func SetCorrelationID(r *http.Request, correlationID string) {
	r.Header.Set(correlationIDKey, correlationID)
}

// GetCorrelationID obtains the correlationID from the header.
func GetCorrelationID(r *http.Request) string {
	corrID := r.Header.Get(correlationIDKey)
	return corrID
}

// GenerateCorrelationID generates a GUID.
func GenerateCorrelationID() string {
	return uuid.New()
}
