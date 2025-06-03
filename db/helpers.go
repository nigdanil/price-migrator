package db

import (
	"database/sql"
	"fmt"
	"log"
)

// LogMissingCategoryRefs выводит предупреждения о несвязанных category_id
func LogMissingCategoryRefs(src *sql.DB) {
	rows, err := src.Query(`SELECT DISTINCT category_id FROM category_urls WHERE category_id IS NOT NULL`)
	if err != nil {
		log.Printf("❌ Ошибка выборки category_ids: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var catID string
		if err := rows.Scan(&catID); err != nil {
			log.Printf("❌ Ошибка чтения category_id: %v", err)
			continue
		}

		var exists bool
		err = src.QueryRow(`SELECT EXISTS (SELECT 1 FROM gallery_categories WHERE id = ?)`, catID).Scan(&exists)
		if err != nil {
			log.Printf("❌ Ошибка проверки существования категории %s: %v", catID, err)
			continue
		}
		if !exists {
			log.Printf("🚨 ВНИМАНИЕ: category_id '%s' есть в category_urls, но отсутствует в gallery_categories", catID)
		}
	}
}

// EnsureCategoriesExist проверяет, что все category_id, встречающиеся в других таблицах, существуют в gallery_categories.
func EnsureCategoriesExist(sqliteDB, pgDB *sql.DB) error {
	log.Println("🛠 Проверка недостающих категорий...")

	query := `
		SELECT DISTINCT category_id FROM (
			SELECT category_id FROM category_urls
			UNION
			SELECT category_id FROM gallery_products
			UNION
			SELECT category_id FROM gallery_product_prices
		) WHERE category_id IS NOT NULL
	`
	rows, err := sqliteDB.Query(query)
	if err != nil {
		return fmt.Errorf("failed to fetch category_ids: %v", err)
	}
	defer rows.Close()

	needed := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		needed[id] = true
	}

	// Получаем уже существующие
	existingRows, err := pgDB.Query(`SELECT id FROM gallery_categories`)
	if err != nil {
		return fmt.Errorf("failed to fetch existing categories: %v", err)
	}
	defer existingRows.Close()

	for existingRows.Next() {
		var id string
		if err := existingRows.Scan(&id); err != nil {
			return err
		}
		delete(needed, id)
	}

	if len(needed) == 0 {
		log.Println("✅ Все категории присутствуют в gallery_categories.")
		return nil
	}

	log.Printf("➕ Добавляем %d недостающих категорий...", len(needed))
	tx, err := pgDB.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT INTO gallery_categories (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for id := range needed {
		log.Printf("➕ Добавлена категория: %s", id)
		if _, err := stmt.Exec(id, id); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert category error: %v", err)
		}
	}

	return tx.Commit()
}
