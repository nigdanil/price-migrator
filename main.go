package main

import (
	"fmt"
	"log"
	"os"
	"price-migrator/db"

	"github.com/joho/godotenv"
)

func main() {
	// Загрузка .env
	if err := godotenv.Load(); err != nil {
		log.Println("⚠️  .env файл не найден, используются переменные среды")
	}

	sqliteDB, err := db.ConnectSQLite("./scraper.db")
	if err != nil {
		log.Fatal("SQLite open error:", err)
	}
	defer sqliteDB.Close()

	pgConnStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s search_path=%s sslmode=%s",
		os.Getenv("PG_HOST"),
		os.Getenv("PG_PORT"),
		os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"),
		os.Getenv("PG_DB"),
		os.Getenv("PG_SCHEMA"),
		os.Getenv("PG_SSLMODE"),
	)

	postgresDB, err := db.ConnectPostgres(pgConnStr)
	if err != nil {
		log.Fatal("PostgreSQL open error:", err)
	}
	defer postgresDB.Close()

	log.Println("🔍 Проверка ссылок в category_urls...")
	db.LogMissingCategoryRefs(sqliteDB)

	if err := db.EnsureCategoriesExist(sqliteDB, postgresDB); err != nil {
		log.Fatalf("Error ensuring categories: %v", err)
	}

	if err := db.MigrateTable(sqliteDB, postgresDB, "gallery_categories"); err != nil {
		log.Fatalf("Error migrating gallery_categories: %v", err)
	}

	tables := []string{
		"category_urls",
		"gallery_products",
		"gallery_product_prices",
	}

	for _, table := range tables {
		if err := db.MigrateTable(sqliteDB, postgresDB, table); err != nil {
			log.Fatalf("Error migrating %s: %v", table, err)
		}
	}

	fmt.Println("✅ Migration completed successfully.")
}
