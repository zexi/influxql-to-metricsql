package main

import (
	"fmt"

	"github.com/zexi/influxql-to-metricsql/converter"
)

func main() {
	sqls := []string{
		`SELECT free FROM "disk" WHERE host = 'ceph-04-192-168-222-114' AND path = '/opt/cloud'`,
		`SELECT mean("in") FROM "swap" WHERE host =~ /$hostname$/ GROUP BY time(2d), host`,
	}

	for _, sql := range sqls {
		promQL, err := converter.Translate(sql)
		if err != nil {
			panic(fmt.Errorf("Translate: %q , error: %v", sql, err))
		}
		fmt.Printf("===========\n")
		fmt.Printf("%s\n%s\n", sql, promQL)
	}

	sqlTimes := []string{
		`SELECT free FROM "disk" WHERE host = 'ceph-04-192-168-222-114' AND path = '/opt/cloud' AND time >= 1698163200000ms and time <= 1698335999000ms`,
		`SELECT free FROM "disk" WHERE host = 'ceph-04-192-168-222-114' AND path = '/opt/cloud' AND time > now() - 1h`,
	}
	for _, sql := range sqlTimes {
		fmt.Printf("\n===========\n")
		promQL, tr, err := converter.TranslateWithTimeRange(sql)
		if err != nil {
			panic(fmt.Errorf("Translate: %q , error: %v", sql, err))
		}
		fmt.Printf("%s\n%s, time range: %#v\n", sql, promQL, tr)
	}
}
