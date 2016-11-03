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

package transferhdr

import (
	"github.com/ClusterHQ/go/dl/encdec"
	dlhash "github.com/ClusterHQ/go/dl/hash"
)

type (
	// Hdr contains the transfer information, it is the first part transferred.
	Hdr struct {
		// Ver is wire format version. If the protocol changes, for example, record format changes, or
		// end of transfer is added at the end of the transfer, etc, this number will go up by 1.
		// Backward compatibility may or may not be maintained.
		Ver int

		// Hash is the type of hash
		Hash dlhash.Type

		// EncDec is the type of hash
		EncDec encdec.Type
	}
)

const (
	// CurVer is the currently supported software version
	CurVer int = 1
)
