// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package glog

import (
	"k8s.io/klog/v2"
)

// Level is a shim
type Level klog.Level

// Verbose is a shim
type Verbose klog.Verbose

// Flush is a shim
func Flush() {
	klog.Flush()
}

// V is a shim
func V(level Level) Verbose {
	return Verbose(klog.V(klog.Level(level)))
}

// Info is a shim
func (v Verbose) Info(args ...interface{}) {
	klog.Info(args...)
}

// Infoln is a shim
func (v Verbose) Infoln(args ...interface{}) {
	klog.Infoln(args...)
}

// Infof is a shim
func (v Verbose) Infof(format string, args ...interface{}) {
	klog.Infof(format, args...)
}

// Info is a shim
func Info(args ...interface{}) {
	klog.Info(args...)
}

// InfoDepth is a shim
func InfoDepth(depth int, args ...interface{}) {
	klog.InfoDepth(depth, args...)
}

// Infoln is a shim
func Infoln(args ...interface{}) {
	klog.Infoln(args...)
}

// Infof is a shim
func Infof(format string, args ...interface{}) {
	klog.Infof(format, args...)
}

// Warning is a shim
func Warning(args ...interface{}) {
	klog.Warning(args...)
}

// WarningDepth is a shim
func WarningDepth(depth int, args ...interface{}) {
	klog.WarningDepth(depth, args...)
}

// Warningln is a shim
func Warningln(args ...interface{}) {
	klog.Warningln(args...)
}

// Warningf is a shim
func Warningf(format string, args ...interface{}) {
	klog.Warningf(format, args...)
}

// Error is a shim
func Error(args ...interface{}) {
	klog.Error(args...)
}

// ErrorDepth is a shim
func ErrorDepth(depth int, args ...interface{}) {
	klog.ErrorDepth(depth, args...)
}

// Errorln is a shim
func Errorln(args ...interface{}) {
	klog.Errorln(args...)
}

// Errorf is a shim
func Errorf(format string, args ...interface{}) {
	klog.Errorf(format, args...)
}

// Fatal is a shim
func Fatal(args ...interface{}) {
	klog.Fatal(args...)
}

// FatalDepth is a shim
func FatalDepth(depth int, args ...interface{}) {
	klog.FatalDepth(depth, args...)
}

// Fatalln is a shim
func Fatalln(args ...interface{}) {
	klog.Fatalln(args...)
}

// Fatalf is a shim
func Fatalf(format string, args ...interface{}) {
	klog.Fatalf(format, args...)
}

// Exit is a shim
func Exit(args ...interface{}) {
	klog.Exit(args...)
}

// ExitDepth is a shim
func ExitDepth(depth int, args ...interface{}) {
	klog.ExitDepth(depth, args...)
}

// Exitln is a shim
func Exitln(args ...interface{}) {
	klog.Exitln(args...)
}

// Exitf is a shim
func Exitf(format string, args ...interface{}) {
	klog.Exitf(format, args...)
}
