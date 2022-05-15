/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package filter

import (
	"fmt"
	"regexp"
	"strings"
)

type tableRulesParser struct {
	rules []tableRule
}

func (p *tableRulesParser) parse(pat string) error {
	var tm matcher
	tm, err := p.parsePattern(pat)
	if err != nil {
		return err
	}

	p.rules = append(p.rules, tableRule{
		table: tm,
	})
	return nil
}

var (
	wildcardRangeRegexp = regexp.MustCompile(`^\[!?(?:\\[^0-9a-zA-Z]|[^\\\]])+\]`)
)

func (p *tableRulesParser) parsePattern(line string) (matcher, error) {
	var (
		literalStringBuilder   strings.Builder
		wildcardPatternBuilder strings.Builder
		isLiteralString        = true
		i                      = 0
	)
	literalStringBuilder.Grow(len(line))
	wildcardPatternBuilder.Grow(len(line) + 6)
	wildcardPatternBuilder.WriteString("(?i)(^|([\\s\\t\\n]+))")

	for i < len(line) {
		c := line[i]
		switch c {
		case '\\':
			isLiteralString = false
			wildcardPatternBuilder.WriteString("\\")
			i++
		case '.':
			isLiteralString = false
			wildcardPatternBuilder.WriteString(".")
			i++
		case '*':
			// wildcard
			isLiteralString = false
			wildcardPatternBuilder.WriteString(".*")
			i++

		case '?':
			isLiteralString = false
			wildcardPatternBuilder.WriteByte('.')
			i++

		case '[':
			// range of characters
			isLiteralString = false
			rangeLoc := wildcardRangeRegexp.FindStringIndex(line[i:])
			if len(rangeLoc) < 2 {
				return nil, fmt.Errorf("syntax error: failed to parse character class")
			}
			end := i + rangeLoc[1]
			switch line[i+1] {
			case '!':
				wildcardPatternBuilder.WriteString("[^")
				wildcardPatternBuilder.WriteString(line[i+2 : end])
			case '^': // `[^` is not special in a glob pattern. escape it.
				wildcardPatternBuilder.WriteString(`[\^`)
				wildcardPatternBuilder.WriteString(line[i+2 : end])
			default:
				wildcardPatternBuilder.WriteString(line[i:end])
			}
			i = end

		default:
			if c == '$' || c == '_' || isASCIIAlphanumeric(c) || c >= 0x80 {
				literalStringBuilder.WriteByte(c)
				wildcardPatternBuilder.WriteByte(c)
				i++
			} else {
				return nil, fmt.Errorf("unexpected special character '%c'", c)
			}
		}
	}

	line = line[i:]
	if isLiteralString {
		return stringMatcher(literalStringBuilder.String()), nil
	}
	wildcardPatternBuilder.WriteByte('$')

	m, err := newRegexpMatcher(wildcardPatternBuilder.String())
	if err != nil {
		return nil, err
	}
	return m, nil
}

func isASCIIAlphanumeric(b byte) bool {
	return '0' <= b && b <= '9' || 'a' <= b && b <= 'z' || 'A' <= b && b <= 'Z'
}
