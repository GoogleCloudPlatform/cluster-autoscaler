# Simulator of CA expander

In main CA folder:
go test -v ./... -run ExpanderPriceOSS
go test -v ./... -run ExpanderPriceGKE
go test -v ./... -run ExpanderComparison

Tests print results to stdout in CSV format.
