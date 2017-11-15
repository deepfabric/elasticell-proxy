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

package util

import (
	"encoding/binary"
	"math"
	"strconv"

	"github.com/pkg/errors"
)

// BytesToUint64 bytes -> uint64
func BytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, errors.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

// Uint64ToBytes uint64 -> bytes
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// StrInt64 str -> int64
func StrInt64(v []byte) (int64, error) {
	return strconv.ParseInt(SliceToString(v), 10, 64)
}

// StrFloat64 str -> float64
func StrFloat64(v []byte) (float64, error) {
	return strconv.ParseFloat(SliceToString(v), 64)
}

// FormatInt64ToBytes int64 -> string
func FormatInt64ToBytes(v int64) []byte {
	return strconv.AppendInt(nil, v, 10)
}

// FormatFloat64ToBytes float64 -> string
func FormatFloat64ToBytes(v float64) []byte {
	return strconv.AppendFloat(nil, v, 'f', -1, 64)
}

/*
Float32ToSortableUint64 converts a float32 string to sortable uint64.

Refers to:
github.com/apache/lucene-solr/lucene/core/src/java/org/apache/lucene/util/NumericUtils.java,
  public static int floatToSortableInt(float value);
  public static long doubleToSortableLong(double value);

https://en.wikipedia.org/wiki/Single-precision_floating-point_format
https://en.wikipedia.org/wiki/Double-precision_floating-point_format
*/
func Float32ToSortableUint64(valS string) (val uint64, err error) {
	var valF float64
	if valF, err = strconv.ParseFloat(valS, 32); err != nil {
		return
	}
	bits := math.Float32bits(float32(valF))
	int0 := int32(bits)
	val = uint64(uint32(int0^((int0>>31)&0x7fffffff)) ^ 0x80000000)
	return
}

//Float64ToSortableUint64 converts a float64 string to sortable uint64.
func Float64ToSortableUint64(valS string) (val uint64, err error) {
	var valF float64
	if valF, err = strconv.ParseFloat(valS, 64); err != nil {
		return
	}
	bits := math.Float64bits(valF)
	int0 := int64(bits)
	val = uint64(int0^((int0>>63)&0x7fffffffffffffff)) ^ 0x8000000000000000
	return
}
