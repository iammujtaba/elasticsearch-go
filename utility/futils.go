package utility

import (
	"encoding/json"
	"strconv"
	"strings"
)

func ConstructQuery(q string, size int) *strings.Reader {
	var query = `{"query": {`
	query = query + q
	query = query + `}, "size": ` + strconv.Itoa(size) + `}`

	isValid := json.Valid([]byte(query))

	if !isValid {
		query = "{}" // match_all query *
	}

	var b strings.Builder
	b.WriteString(query)
	read := strings.NewReader(b.String())
	return read
}
