package gorm

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type opengauss struct {
	commonDialect
}

func init() {
	RegisterDialect("opengauss", &opengauss{})
}

func (opengauss) GetName() string {
	return "opengauss"
}

func (opengauss) BindVar(i int) string {
	return fmt.Sprintf("$%v", i)
}

func (s *opengauss) DataTypeOf(field *StructField) string {
	var dataValue, sqlType, size, additionalType = ParseFieldStructForDialect(field, s)

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "boolean"

		case reflect.Int8, reflect.Int16, reflect.Uint8, reflect.Uint16:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "smallserial"
			} else {
				sqlType = "smallint"
			}

		case reflect.Int, reflect.Int32, reflect.Uint, reflect.Uintptr:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "serial"
			} else {
				sqlType = "integer"
			}
		case reflect.Int64, reflect.Uint32, reflect.Uint64:
			if s.fieldCanAutoIncrement(field) {
				field.TagSettingsSet("AUTO_INCREMENT", "AUTO_INCREMENT")
				sqlType = "bigserial"
			} else {
				sqlType = "bigint"
			}
		case reflect.Float32, reflect.Float64:
			sqlType = "numeric"

		case reflect.String:
			if _, ok := field.TagSettingsGet("SIZE"); !ok {
				size = 0 // if SIZE haven't been set, use `text` as the default type, as there are no performance different
			}

			if size > 0 && size < 65532 {
				sqlType = fmt.Sprintf("varchar(%d)", size)
			} else {
				sqlType = "text"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "timestamp with time zone"
			}
		case reflect.Map:
			if dataValue.Type().Name() == "Hstore" {
				sqlType = "hstore"
			}
		default:
			if IsByteArrayOrSlice(dataValue) {
				sqlType = "bytea"

				if isUUID(dataValue) {
					sqlType = "uuid"
				}

				if isJSON(dataValue) {
					sqlType = "jsonb"
				}
			}
		}
	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) for opengauss", dataValue.Type().Name(), dataValue.Kind().String()))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}

	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (s opengauss) HasIndex(tableName string, indexName string) bool {
	var count int
	s.db.QueryRow("SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $2 AND schemaname = CURRENT_SCHEMA()", tableName, indexName).Scan(&count)
	return count > 0
}

func (s opengauss) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	s.db.QueryRow("SELECT count(pc.conname) FROM PG_CATALOG.PG_CONSTRAINT pc JOIN PG_CATALOG.PG_CLASS tc ON pc.CONRELID = tc.OID JOIN PG_CATALOG.PG_NAMESPACE ns ON tc.RELNAMESPACE = ns.OID WHERE ns.NSPNAME = CURRENT_SCHEMA() AND tc.RELNAME = $1 AND pc.CONNAME = $2 AND pc.CONTYPE = 'f'", tableName, foreignKeyName).Scan(&count)

	return count > 0
}

func (s opengauss) HasTable(tableName string) bool {
	var count int
	s.db.QueryRow("SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1 AND n.nspname = CURRENT_SCHEMA()", tableName).Scan(&count);

	return count > 0
}

func (s opengauss) HasColumn(tableName string, columnName string) bool {
	var count int
	s.db.QueryRow("SELECT count(*) FROM PG_CATALOG.PG_CLASS c JOIN PG_CATALOG.PG_ATTRIBUTE a ON c.OID = a.attrelid WHERE c.relname = $1 AND a.attname = $2", tableName, columnName).Scan(&count)

	return count > 0
}

func (s opengauss) CurrentDatabase() (name string) {
	s.db.QueryRow("SELECT CURRENT_DATABASE()").Scan(&name)
	return
}

func (s opengauss) LastInsertIDOutputInterstitial(tableName, key string, columns []string) string {
	return ""
}

func (s opengauss) LastInsertIDReturningSuffix(tableName, key string) string {
	return fmt.Sprintf("RETURNING %v.%v", tableName, key)
}

func (opengauss) SupportLastInsertID() bool {
	return false
}
