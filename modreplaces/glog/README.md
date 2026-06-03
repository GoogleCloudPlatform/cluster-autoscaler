### What's that and why?

This is a glog shim which delegates all the calls to its successor: k8s.io/klog/v2. glog is right now a transitive dependency of cluster autoscaler and sadly it's not entirely* compatible with our usage of klog. They have conflicting flags and there's no way to gracefully befriend them as glog is doing implicit initialization which is not controlled by the user, klog on the contrary expects user to call `InitFlags` function to register all of its flags into the command line flagset. When doing such a registration - glog flag turns out to be already registered there which causes panic. 

And while there's a way to mitigate that - it's even more hacky than what's going on here and allows only the glog subset of flags to be defined while klog exposes more options: https://github.com/kubernetes/klog/blob/main/examples/coexist_glog/coexist_glog.go

### What does it exactly do?

It just translates all the glog calls to klog, as their APIs are extremely consistent. The only problem it causes is klog looking at the wrong stack frame and displaying the wrong source location the log is emitted, it probably can be fixed, but this implementation is sufficient for the current state of things as glog is not used in the third-party portion of the code we are **using**, but it's definitely there when initializing the package