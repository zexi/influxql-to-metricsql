package converter

import (
	"io"

	"github.com/influxdata/influxql"
	"github.com/pkg/errors"

	"github.com/zexi/influxql-to-metricsql/converter/translator"
)

type Converter interface {
	Translate() (string, error)
}

type converter struct {
	influxParser *influxql.Parser
	translator   translator.Translator
}

func New(r io.Reader) Converter {
	c := &converter{
		influxParser: influxql.NewParser(r),
		translator:   translator.NewMetricsQL(),
	}
	return c
}

func (c converter) Translate() (string, error) {
	q, err := c.influxParser.ParseQuery()
	if err != nil {
		return "", errors.Wrap(err, "influxParser.ParserQuery")
	}
	if len(q.Statements) >= 1 {
		return "", errors.Errorf("Only support 1 statement translating")
	}
	return c.translator.Translate(q.Statements[0])
}
