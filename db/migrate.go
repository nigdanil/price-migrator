package db

import (
	"database/sql"
	"fmt"
	"log"
	"price-migrator/utils"
	"strings"
)

// MigrateTable синхронизирует таблицу PostgreSQL с SQLite:
// - удаляет отсутствующие записи (кроме gallery_categories)
// - обновляет изменённые
// - добавляет новые
func MigrateTable(src, dst *sql.DB, table string) error {
	log.Printf("➡ Начало миграции таблицы: %s", table)

	rows, err := src.Query("SELECT * FROM " + table)
	if err != nil {
		return fmt.Errorf("sqlite select error: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("sqlite column error: %v", err)
	}

	primaryKey := cols[0]
	sqliteKeys := make(map[interface{}]bool)
	var sqliteData [][]interface{}

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("sqlite scan error: %v", err)
		}
		sqliteKeys[vals[0]] = true
		sqliteData = append(sqliteData, vals)
	}
	log.Printf("📥 Извлечено из SQLite: %d строк", len(sqliteData))

	tx, err := dst.Begin()
	if err != nil {
		return fmt.Errorf("postgres tx begin error: %v", err)
	}

	if _, err := tx.Exec("SET CONSTRAINTS ALL DEFERRED"); err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to defer constraints: %v", err)
	}

	// 🔐 Пропускаем удаление вручную добавленных категорий
	if table != "gallery_categories" && len(sqliteKeys) > 0 {
		keys := make([]string, 0, len(sqliteKeys))
		for k := range sqliteKeys {
			keys = append(keys, fmt.Sprintf("'%v'", k))
		}
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s NOT IN (%s)", table, primaryKey, strings.Join(keys, ","))
		result, err := tx.Exec(deleteQuery)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("postgres delete error: %v", err)
		}
		affected, _ := result.RowsAffected()
		log.Printf("🗑 Удалено из PostgreSQL: %d строк", affected)
	}

	insertQuery := utils.BuildUpsertQuery(table, cols, primaryKey)
	stmt, err := tx.Prepare(insertQuery)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	successCount := 0
	for i, row := range sqliteData {
		if _, err := stmt.Exec(row...); err != nil {
			tx.Rollback()
			log.Printf("❌ Ошибка на строке %d: %v", i, err)
			log.Printf("🔍 Проблемная строка: %v", row)
			return fmt.Errorf("insert/upsert error: %v", err)
		}
		successCount++
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit error: %v", err)
	}

	log.Printf("✅ Миграция таблицы %s завершена: %d строк вставлено/обновлено\n", table, successCount)
	return nil
}
