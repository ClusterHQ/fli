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

package rest

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pborman/uuid"
)

type testJSONObj struct {
	Text string `json:"text"`
}

func TestResponseMarshal(t *testing.T) {
	r, err := http.NewRequest("OPTION", "/this/is/a/test", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp := NewResponse(r)
	resp.RequestID = uuid.Parse("60ce7510-c89f-4fae-bc27-da8c1b8e04fd")
	resp.ErrorMessage = "This is an error"
	resp.SetResult(&testJSONObj{Text: "Here we are"})

	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}

	actual := string(body)
	const expected = `{"request_id":"60ce7510-c89f-4fae-bc27-da8c1b8e04fd",` +
		`"method":"OPTION","request_path":"/this/is/a/test","success":true,` +
		`"processing_time":0,"result":{"text":"Here we are"},` +
		`"error_message":"This is an error"}`
	if string(body) != expected {
		t.Errorf("expected '%s' but was '%s'", expected, actual)
	}
}

func TestResponseUnmarshal(t *testing.T) {
	const test = `{"request_id":"60ce7510-c89f-4fae-bc27-da8c1b8e04fd",` +
		`"method":"OPTION","request_path":"/this/is/a/test","success":true,` +
		`"processing_time":0,"result":{"text":"Here we are"},` +
		`"error_message":"This is an error"}`
	var resp Response
	err := json.Unmarshal([]byte(test), &resp)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorMessage != "This is an error" {
		t.Errorf("Expected 'This is an error' but was '%s'", resp.ErrorMessage)
	}

	var obj testJSONObj
	err = resp.GetResult(&obj)
	if err != nil {
		t.Fatal(err)
	}
	if obj.Text != "Here we are" {
		t.Errorf("Expected 'Here we are' but was '%s'", obj.Text)
	}
}
