package translator

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxql"
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
			m := metricsQL{}
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
			got, err := m.getMetricName(sq.Sources, sq.Fields)
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
	tests := []struct {
		expr string
		want []string
	}{
		{
			expr: `host = 'server01'`,
			want: []string{"host=server01"},
		},
		{
			expr: `host != 'server01'`,
			want: []string{"host!=server01"},
		},
		{
			expr: `hostname =~ /regexp/`,
			want: []string{`hostname~="regexp"`},
		},
		{
			expr: `hostname = 'office01' AND region =~ /uswest.*/`,
			want: []string{`hostname="office01"`, `region~="uswest.*"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			l := NewLabelsVisitor()
			expr, err := influxql.ParseExpr(tt.expr)
			if err != nil {
				t.Errorf("ParseExpr %q, error: %v", tt.expr, err)
				return
			}
			//cond, _, err := influxql.ConditionExpr(expr, nil)
			influxql.Walk(l, expr)
			if got := l.labels; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("After Visited labels = %v, want %v", got, tt.want)
			}
		})
	}
}
