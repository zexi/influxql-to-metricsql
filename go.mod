module github.com/zexi/influxql-to-metricsql

go 1.18

require (
	github.com/influxdata/influxql v1.1.0
	github.com/influxdata/promql/v2 v2.12.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.6.0
)

require (
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
)

replace github.com/influxdata/promql/v2 => github.com/zexi/promql/v2 v2.12.1
