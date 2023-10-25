package translator

import (
	"fmt"
	"log"
	"strings"

	"github.com/influxdata/influxql"
	"github.com/pkg/errors"
)

type metricsQL struct {
}

func NewMetricsQL() Translator {
	return &metricsQL{}
}

func (m metricsQL) Translate(s influxql.Statement) (string, error) {
	selectS, ok := s.(*influxql.SelectStatement)
	if !ok {
		return "", errors.Errorf("Only SelectStatement is supported, input %t", s)
	}
	return m.translate(selectS)
}

func (m metricsQL) translate(s *influxql.SelectStatement) (string, error) {
	var buf strings.Builder
	metricName, err := m.getMetricName(s.Sources, s.Fields)
	if err != nil {
		return "", errors.Wrap(err, "getMetricName")
	}

	m.getLabels(s.Condition)

	buf.WriteString(metricName)

	return buf.String(), nil
}

func (m metricsQL) getMetricName(sources influxql.Sources, fields influxql.Fields) (string, error) {
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
		fieldName, err = m.getCallVariable(expr)
	default:
		return "", errors.Errorf("field.Expr %#v is not supported", expr)
	}

	return fmt.Sprintf("%s_%s", measurement.Name, fieldName), err
}

func (m metricsQL) getCallVariable(c *influxql.Call) (string, error) {
	if len(c.Args) != 1 {
		return "", errors.Errorf("length of args %#v != 1", c.Args)
	}
	return c.Args[0].String(), nil
}

type LabelsVisitor struct {
	err    error
	labels []string
	curKey string
	curOp  influxql.Token
	curVal string
}

func NewLabelsVisitor() *LabelsVisitor {
	return &LabelsVisitor{
		err:    nil,
		labels: make([]string, 0),
	}
}

func (l *LabelsVisitor) commitLabel() error {
	op := l.curOp.String()
	if l.curOp == influxql.EQREGEX {
		op = "~="
	}
	label := fmt.Sprintf(`%s%s"%s"`, l.curKey, op, l.curVal)
	l.labels = append(l.labels, label)
	return nil
}

func (l *LabelsVisitor) Visit(node influxql.Node) influxql.Visitor {
	fmt.Printf("visit: %#v\n", node)
	if l.err != nil {
		log.Printf("error happend: %v, visting skipped", l.err)
		return l
	}
	switch expr := node.(type) {
	case *influxql.BinaryExpr:
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

func (m metricsQL) getLabels(cond influxql.Expr) ([]string, error) {
	if cond == nil {
		return nil, nil
	}
	v := NewLabelsVisitor()
	influxql.Walk(v, cond)
	binExpr, ok := cond.(*influxql.BinaryExpr)
	if !ok {
		return nil, errors.Errorf("%#v isn't supported", cond)
	}
	if binExpr.Op != influxql.AND {
		return nil, errors.Errorf("Op %v isn't supported", binExpr.Op)
	}
	return nil, nil
}
