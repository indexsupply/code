package abi

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"go/format"
	"text/template"
	"unicode"

	"github.com/indexsupply/x/isxerrors"
)

//go:embed template.txt
var abitemp string

func camel(str string) string {
	var (
		in  = []rune(str)
		res []rune
	)
	for i, r := range in {
		switch {
		case r == '_':
			//skip
		case i == 0 || in[i-1] == '_':
			res = append(res, unicode.ToUpper(r))
		default:
			res = append(res, r)
		}
	}
	return string(res)
}

func Gen(pkg string, js []byte) ([]byte, error) {
	events := []Event{}
	err := json.Unmarshal(js, &events)
	if err != nil {
		return nil, isxerrors.Errorf("parsing abi json: %w", err)
	}
	var (
		b bytes.Buffer
		t *template.Template
	)

	t = template.New("abi")
	t.Funcs(template.FuncMap{"camel": camel})
	t, err = t.Parse(abitemp)
	if err != nil {
		return nil, isxerrors.Errorf("parsing template: %w", err)
	}

	err = t.ExecuteTemplate(&b, "header", pkg)
	if err != nil {
		return nil, isxerrors.Errorf("executing header template: %w", err)
	}
	for _, event := range events {
		if event.Type != "event" {
			continue
		}
		err = t.ExecuteTemplate(&b, "event", event)
		if err != nil {
			return nil, isxerrors.Errorf("executing event template: %w", err)
		}
	}
	code, err := format.Source(b.Bytes())
	if err != nil {
		return nil, isxerrors.Errorf("formatting source: %w", err)
	}
	return code, nil
}