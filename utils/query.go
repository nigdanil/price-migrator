package utils

import (
	"fmt"
	"strings"
)

func BuildUpsertQuery(table string, columns []string, primaryKey string) string {
	placeholders := make([]string, len(columns))
	setParts := []string{}

	for i, col := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		if col != primaryKey {
			setParts = append(setParts, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}

	return fmt.Sprintf(`
INSERT INTO %s (%s)
VALUES (%s)
ON CONFLICT (%s) DO UPDATE SET %s`,
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		primaryKey,
		strings.Join(setParts, ", "),
	)
}
