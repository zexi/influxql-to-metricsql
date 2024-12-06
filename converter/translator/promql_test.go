package translator

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/promql/v2/pkg/labels"
)

func Test_metricsQL_getMetricName(t *testing.T) {
	tests := []struct {
		sql     string
		want    string
		wantErr bool
	}{
		{
			sql:     `SELECT mean(usage) FROM "cpu" WHERE hostname = 'office-hq'`,
			want:    "cpu_usage",
			wantErr: false,
		},
		{
			sql:     `SELECT free from disk`,
			want:    "disk_free",
			wantErr: false,
		},
		{
			sql:     `SELECT * from disk`,
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			q, err := influxql.ParseStatement(tt.sql)
			if err != nil {
				t.Errorf("influxql.ParserQuery(%q): %v", tt.sql, err)
				return
			}
			sq, ok := q.(*influxql.SelectStatement)
			if !ok {
				t.Errorf("can't cast %#v to *influxql.SelectStatement", q)
				return
			}
			got, err := getMetricName(sq.Sources, sq.Fields[0])
			if (err != nil) != tt.wantErr {
				t.Errorf("getMetricName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getMetricName() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLabelsVisitor_Visit(t *testing.T) {
	nMatcher := func(t labels.MatchType, n string, k string) *labels.Matcher {
		l, _ := labels.NewMatcher(t, n, k)
		return l
	}
	tests := []struct {
		expr    string
		want    []*labels.Matcher
		wantErr bool
	}{
		{
			expr: `host = 'server01'`,
			want: []*labels.Matcher{
				nMatcher(labels.MatchEqual, "host", "server01"),
			},
		},
		{
			expr: `host != 'server01'`,
			want: []*labels.Matcher{
				nMatcher(labels.MatchNotEqual, "host", "server01"),
			},
		},
		{
			expr: `hostname =~ /regexp/`,
			want: []*labels.Matcher{
				nMatcher(labels.MatchRegexp, "hostname", "regexp"),
			},
		},
		{
			expr: `hostname !~ /regexp/`,
			want: []*labels.Matcher{
				nMatcher(labels.MatchNotRegexp, "hostname", "regexp"),
			},
		},
		{
			expr: `hostname = 'office01' AND region =~ /uswest.*/`,
			want: []*labels.Matcher{
				nMatcher(labels.MatchEqual, "hostname", "office01"),
				nMatcher(labels.MatchRegexp, "region", "uswest.*"),
			},
		},
		{
			expr: `hostname = 'office01' or region =~ /uswest.*/`,
			want: []*labels.Matcher{
				nMatcher(labels.MatchEqual, "hostname", "office01"),
				nMatcher(labels.MatchRegexp, "region", "uswest.*"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			l := newLabelsVisitor()
			expr, err := influxql.ParseExpr(tt.expr)
			if err != nil {
				t.Errorf("ParseExpr %q, error: %v", tt.expr, err)
				return
			}
			influxql.Walk(l, expr)
			if tt.wantErr {
				if l.Error() == nil {
					t.Errorf("no error happened, got: %v", l.Labels())
				}
			} else {
				if got := l.labels; !reflect.DeepEqual(got, tt.want) {
					t.Errorf("After Visited labels = %v, want %v, error: %v", got, tt.want, l.Error())
				}
			}
		})
	}
}

func Test_metricsQL_Translate(t *testing.T) {
	tests := []struct {
		sql     string
		want    string
		wantErr bool
	}{
		{
			sql:     "select free from disk",
			want:    `disk_free`,
			wantErr: false,
		},
		{
			sql:     `SELECT free FROM "disk" WHERE host = 'ceph-04-192-168-222-114' AND path = '/opt/cloud'`,
			want:    `disk_free{host="ceph-04-192-168-222-114",path="/opt/cloud"}`,
			wantErr: false,
		},
		{
			sql:     `SELECT free FROM "disk" WHERE host = 'ceph-04-192-168-222-114' OR path = '/opt/cloud' OR tname = 'test' OR tname2 = 'test2'`,
			want:    `disk_free{host="ceph-04-192-168-222-114" or path="/opt/cloud" or tname="test" or tname2="test2"}`,
			wantErr: false,
		},
		{
			sql:     `SELECT free FROM "disk" WHERE host = 'ceph-04-192-168-222-114' OR path = '/opt/cloud' OR tname = 'test' and tname2 = 'test2'`,
			want:    `disk_free{host="ceph-04-192-168-222-114" or path="/opt/cloud" or tname="test",tname2="test2"}`,
			wantErr: false,
		},
		{
			sql:     `SELECT free FROM "disk" WHERE host = 'ceph-04-192-168-222-114' AND path = '/opt/cloud' OR tname = 'test'`,
			want:    `disk_free{host="ceph-04-192-168-222-114",path="/opt/cloud" or tname="test"}`,
			wantErr: false,
		},
		{
			sql:     `SELECT mean("in") FROM "swap" WHERE host =~ /$hostname$/ GROUP BY time(2d), host`,
			want:    `avg by(host) (avg_over_time(swap_in{host=~"$hostname$"}[2d]))`,
			wantErr: false,
		},
		{
			sql:     `SELECT last("uptime") FROM "system" WHERE time > now() - 3m GROUP BY *, time(1m) fill(none)`,
			want:    `last_over_time(system_uptime[1m])`,
			wantErr: false,
		},
		//{
		//	sql:     `SELECT last("exit_status") FROM "smart_device" WHERE "health_ok" = false AND time > now() - 1h GROUP BY *`,
		//	want:    `last_over_time(exit_status{health_ok=false}[1m])`,
		//	wantErr: false,
		//},
		{
			sql:  `SELECT mean("usage_active") FROM "cpu" WHERE "res_type" = 'host' AND time > now() - 1h GROUP BY "host_id"`,
			want: `avg by(host_id) (avg_over_time(cpu_usage_active{res_type="host"}[1m]))`,
		},
		{
			sql:  `SELECT abs(mean("bps_recv")) FROM "vm_netio" WHERE "project_domain" != '' AND time > now() - 10080m GROUP BY "vm_name", "vm_id", time(7d) fill(none)`,
			want: `avg by(vm_name, vm_id) (abs(avg_over_time(vm_netio_bps_recv{project_domain!=""}[1w])))`,
		},
		{
			sql:  `SELECT last(*) FROM mem WHERE time > now() - 1h`,
			want: `last_over_time({__name__=~"^mem_.*"}[1m])`,
		},
		{
			sql:  `SELECT count("usage_active") FROM "vm_cpu" WHERE ("db" = 'telegraf' AND "host" = 'test-69-onecloud01-10-127-100-2') AND time > now() - 1h GROUP BY *, time(2m) fill(none)`,
			want: `count(vm_cpu_usage_active{db="telegraf",host="test-69-onecloud01-10-127-100-2"}[2m])`,
		},
		{
			sql:  `SELECT sum("bps_recv") FROM "vm_netio" WHERE "db" = 'telegraf' AND time > now() - 1h GROUP BY "host", time(2m) fill(none)`,
			want: `sum by(host) (vm_netio_bps_recv{db="telegraf"}[2m])`,
		},
		{
			sql:  `SELECT min("bps_recv") FROM "vm_netio" WHERE "db" = 'telegraf' AND time > now() - 1h GROUP BY "host", time(2m) fill(none)`,
			want: `min by(host) (vm_netio_bps_recv{db="telegraf"}[2m])`,
		},
		{
			sql:  `SELECT sum("free"), sum("used"), sum("total") FROM "disk" WHERE time > now() - 720h GROUP BY fill(none)`,
			want: `union(label_set(sum(disk_free[1m]), "__union_result__", "sum_disk_free"), label_set(sum(disk_used[1m]), "__union_result__", "sum_disk_used"), label_set(sum(disk_total[1m]), "__union_result__", "sum_disk_total"))`,
		},
		{
			sql:  `SELECT top("usage_active", "vm_name", "vm_id", 5) FROM "vm_cpu" WHERE ("project_domain" != '' AND "project_tags.0.0.key" = 'user:L2.1')`,
			want: `topk_avg(5, vm_cpu_usage_active{project_domain!="",project_tags.0.0.key="user:L2.1"}[1m])`,
		},
		{
			sql:  `SELECT percentile("bps_recv", 95) FROM "vm_netio" WHERE "vm_id" = 'cdc9df53-7175-42b4-8ea9-04139d18825a' AND time > now() - 10080m GROUP BY time(7d)`,
			want: `quantile_over_time(0.95, vm_netio_bps_recv{vm_id="cdc9df53-7175-42b4-8ea9-04139d18825a"}[1w])`,
		},
		//{
		//	sql:  `SELECT top("usage_active", "vm_name", "vm_id", 5) FROM "vm_cpu" WHERE ("project_domain" != '' OR "project_tags.0.0.key" = 'user:L2.1')`,
		//	want: `topk_avg(5, vm_cpu_usage_active{project_domain!="",project_tags.0.0.key="user:L2.1"}[1m])`,
		//},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			m := NewPromQL()
			s, err := influxql.ParseStatement(tt.sql)
			if err != nil {
				t.Errorf("ParseStatement(%q) error = %v", tt.sql, err)
				return
			}
			got, err := m.Translate(s)
			if (err != nil) != tt.wantErr {
				t.Errorf("Translate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Translate() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getTimeRange(t *testing.T) {
	start, _ := time.Parse(time.RFC3339, "2023-10-24T16:00:00Z")
	end, _ := time.Parse(time.RFC3339, "2023-10-26T15:59:59Z")
	tests := []struct {
		cond    string
		want    influxql.TimeRange
		wantErr bool
	}{
		{
			cond: `time >= 1698163200000ms and time <= 1698335999000ms`,
			want: influxql.TimeRange{
				Min: start,
				Max: end,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.cond, func(t *testing.T) {
			cond, err := influxql.ParseExpr(tt.cond)
			if err != nil {
				t.Fatalf("parseExpr %q: %v", tt.cond, err)
				return
			}
			_, got, err := getTimeRange(cond)
			if (err != nil) != tt.wantErr {
				t.Errorf("getTimeRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(*got, tt.want) {
				t.Errorf("getTimeRange() got = %v, want %v", got, tt.want)
			}
		})
	}
}
