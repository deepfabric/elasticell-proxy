// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// +build freebsd openbsd netbsd dragonfly linux

package storage

import (
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testNemoKVSuite{})
var _ = Suite(&testNemoHashSuite{})
var _ = Suite(&testNemoListSuite{})
var _ = Suite(&testNemoSetSuite{})
var _ = Suite(&testNemoZSetSuite{})

var _ = Suite(&testNemoDataSuite{})
var _ = Suite(&testNemoMetaSuite{})

var _ = Suite(&testNemoWBSuite{})

func TestNemo(t *testing.T) {
	TestingT(t)
}
