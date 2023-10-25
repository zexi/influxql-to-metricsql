package translator

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/promql/v2"
	"github.com/influxdata/promql/v2/pkg/labels"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

type promQL struct {
	startTime       string
	endTime         string
	groupByWildcard bool
}

func NewPromQL() Translator {
	return &promQL{}
}

func (m promQL) Translate(s influxql.Statement) (string, error) {
	selectS, ok := s.(*influxql.SelectStatement)
	if !ok {
		return "", errors.Errorf("Only SelectStatement is supported, input %t", s)
	}
	return m.translate(selectS)
}

func (m promQL) translate(s *influxql.SelectStatement) (string, error) {
	metricName, err := getMetricName(s.Sources, s.Fields)
	if err != nil {
		return "", errors.Wrap(err, "getMetricName")
	}
	aggrOp, err := getAggrOperator(s.Fields)
	if err != nil {
		return "", errors.Wrap(err, "get aggregate operator")
	}
	//interval, err := s.GroupByInterval()
	//if err != nil {
	//	return "", errors.Wrap(err, "GroupByInterval")
	//}
	matchers, err := m.getLabels(s.Condition)
	if err != nil {
		return "", errors.Wrap(err, "get matchers")
	}
	nameMatcher, _ := labels.NewMatcher(labels.MatchEqual, labels.MetricName, metricName)
	matchers = append(matchers, nameMatcher)

	lookbehindWin, groups, err := m.getGroups(s.Dimensions)
	if err != nil {
		return "", errors.Wrap(err, "get groups")
	}

	return m.formatExpression(metricName, matchers, lookbehindWin, aggrOp, groups)
}

func (m promQL) formatExpression(
	metricName string,
	ls []*labels.Matcher,
	lookbehindWindow string,
	aggrOp string,
	groups []string) (string, error) {
	fmt.Printf("=====name: %s, labels: %#v, lookbehindWindow: %q, aggrOp: %q, groups: %#v\n", metricName, ls, lookbehindWindow, aggrOp, groups)

	var result promql.Expr
	if aggrOp != "" {
		if lookbehindWindow == "" {
			lookbehindWindow = "1m"
		}
		dur, err := model.ParseDuration(lookbehindWindow)
		if err != nil {
			return "", errors.Wrapf(err, "ParseDuration: %q", lookbehindWindow)
		}
		ms := &promql.MatrixSelector{
			Name:          metricName,
			LabelMatchers: ls,
			Range:         time.Duration(dur),
		}
		result = ms
	} else {
		vs := &promql.VectorSelector{
			Name:          metricName,
			LabelMatchers: ls,
		}
		result = vs
	}

	if len(groups) != 0 && len(aggrOp) == 0 {
		return "", errors.Errorf("Can't use group by when aggregate operator is empty")
	}

	switch aggrOp {
	case "mean":
		// https://docs.victoriametrics.com/MetricsQL.html#avg_over_time
		expr := &promql.Call{
			Func: &promql.Function{
				Name:       "avg_over_time",
				ArgTypes:   []promql.ValueType{promql.ValueTypeMatrix},
				Variadic:   0,
				ReturnType: promql.ValueTypeVector,
			},
			Args: promql.Expressions{result},
		}
		result = expr
	case "last":
		// https://docs.victoriametrics.com/MetricsQL.html#last_over_time
		expr := &promql.Call{
			Func: &promql.Function{
				Name:       "last_over_time",
				ArgTypes:   []promql.ValueType{promql.ValueTypeMatrix},
				Variadic:   0,
				ReturnType: promql.ValueTypeVector,
			},
			Args: promql.Expressions{result},
		}
		result = expr
	}

	fmt.Printf("=====m.GroupByWildcard: %v, %#v\n", m.groupByWildcard, result)

	if len(groups) != 0 && !m.groupByWildcard {
		expr := &promql.AggregateExpr{
			Op:   promql.ItemAvg,
			Expr: result,
		}
		if len(groups) != 0 {
			expr.Grouping = groups
		}
		result = expr
	}

	return result.String(), nil
}

func getAggrOperator(fields influxql.Fields) (string, error) {
	if len(fields) != 1 {
		return "", errors.Errorf("fields %#v length doesn't equal 1", fields)
	}
	field := fields[0]
	aggrOp, ok := field.Expr.(*influxql.Call)
	if !ok {
		return "", nil
	}
	return aggrOp.Name, nil
}

func getMetricName(sources influxql.Sources, fields influxql.Fields) (string, error) {
	if len(sources) != 1 {
		return "", errors.Errorf("sources %#v length doesn't equal 1", sources)
	}
	if len(fields) != 1 {
		return "", errors.Errorf("fields %#v length doesn't equal 1", fields)
	}
	src := sources[0]
	field := fields[0]
	measurement, ok := src.(*influxql.Measurement)
	if !ok {
		return "", errors.Errorf("source %#v is not measurement type", src)
	}

	var (
		fieldName string
		err       error
	)

	switch expr := field.Expr.(type) {
	case *influxql.VarRef:
		fieldName = expr.Val
	case *influxql.Call:
		fieldName, err = getCallVariable(expr)
	default:
		return "", errors.Errorf("field.Expr %#v is not supported", expr)
	}

	return fmt.Sprintf("%s_%s", measurement.Name, fieldName), err
}

func getCallVariable(c *influxql.Call) (string, error) {
	if len(c.Args) != 1 {
		return "", errors.Errorf("length of args %#v != 1", c.Args)
	}
	switch args := c.Args[0].(type) {
	case *influxql.VarRef:
		return args.Val, nil
	default:
		return "", errors.Errorf("unsupported args %#v", args)
	}
	return c.Args[0].String(), nil
}

type labelsVisitor struct {
	err    error
	labels []*labels.Matcher
	curKey string
	curOp  influxql.Token
	curVal string
}

func NewLabelsVisitor() *labelsVisitor {
	return &labelsVisitor{
		err:    nil,
		labels: make([]*labels.Matcher, 0),
	}
}

func (l *labelsVisitor) Error() error {
	return l.err
}

func (l *labelsVisitor) Labels() []*labels.Matcher {
	return l.labels
}

func (l *labelsVisitor) commitLabel() error {
	if l.err != nil {
		return l.err
	}
	var (
		label *labels.Matcher
		err   error
	)
	var promOP labels.MatchType
	switch l.curOp {
	case influxql.EQ:
		promOP = labels.MatchEqual
	case influxql.NEQ:
		promOP = labels.MatchNotEqual
	case influxql.EQREGEX:
		promOP = labels.MatchRegexp
	case influxql.NEQREGEX:
		promOP = labels.MatchNotRegexp
	default:
		return errors.Errorf("Not suport influxdb operator: %s", l.curOp)
	}
	label, err = labels.NewMatcher(promOP, l.curKey, l.curVal)
	if err != nil {
		return errors.Wrapf(err, "not supported operator: %q", l.curOp)
	}

	l.labels = append(l.labels, label)
	return nil
}

func (l *labelsVisitor) Visit(node influxql.Node) influxql.Visitor {
	// fmt.Printf("-- visit: %#v\n", node)
	if l.err != nil {
		log.Printf("error happend: %v, visting skipped", l.err)
		return l
	}
	switch expr := node.(type) {
	case *influxql.BinaryExpr:
		if expr.Op == influxql.OR {
			l.err = errors.Errorf("%#v: OR is not suported yet.", expr)
			return l
		}
		l.curOp = expr.Op
	case *influxql.VarRef:
		l.curKey = expr.Val
	case *influxql.StringLiteral:
		l.curVal = expr.Val
		if err := l.commitLabel(); err != nil {
			l.err = err
		}
	case *influxql.RegexLiteral:
		l.curVal = expr.Val.String()
		if err := l.commitLabel(); err != nil {
			l.err = err
		}
	}
	return l
}

func (m promQL) getLabels(cond influxql.Expr) ([]*labels.Matcher, error) {
	if cond == nil {
		return nil, nil
	}
	v := NewLabelsVisitor()
	influxql.Walk(v, cond)
	return v.Labels(), v.Error()
}

func (m *promQL) getGroups(groups influxql.Dimensions) (string, []string, error) {
	result := []string{}
	var (
		lookbehindWindow string
	)
	for _, group := range groups {
		tmpWin, grp, err := m.getGroup(group)
		if err != nil {
			return "", result, errors.Wrapf(err, "getGroup %q", group)
		}
		if tmpWin != "" {
			lookbehindWindow = tmpWin
		}
		if grp != "" {
			result = append(result, grp)
		}
	}
	return lookbehindWindow, result, nil
}

func (m *promQL) getGroup(group *influxql.Dimension) (string, string, error) {
	fmt.Printf("---try group: %#v\n", group)
	grp := group.Expr
	lookbehindWindow := ""
	switch expr := grp.(type) {
	case *influxql.Call:
		if expr.Name == "time" {
			lookbehindWindow = expr.Args[0].String()
		}
		return lookbehindWindow, "", nil
	case *influxql.VarRef:
		return "", expr.Val, nil
	case *influxql.Wildcard:
		m.groupByWildcard = true
		return "", "", nil
	}
	return "", "", errors.Errorf("not support %q", group.String())
}
