package gormbatchputs

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jinzhu/gorm"
	"github.com/theplant/batchputs"
)

type RowPreProcessor func(row interface{}) (skip bool, err error)
type Batcher struct {
	db                *gorm.DB
	rows              interface{}
	onlyColumns       []string
	excludeColumns    []string
	rowPreProcessors  []RowPreProcessor
	maxSqlParamsCount int
}

func clone(b *Batcher) (r *Batcher) {
	r = &Batcher{
		db:                b.db,
		rows:              b.rows,
		onlyColumns:       b.onlyColumns,
		excludeColumns:    b.excludeColumns,
		rowPreProcessors:  b.rowPreProcessors,
		maxSqlParamsCount: b.maxSqlParamsCount,
	}
	return
}

func New() (b *Batcher) {
	b = &Batcher{}
	return
}

func (b *Batcher) WithDB(db *gorm.DB) (r *Batcher) {
	r = clone(b)
	r.db = db
	return
}

func (b *Batcher) Verbose() (r *Batcher) {
	batchputs.Verbose = true
	r = b
	return
}

func (b *Batcher) OnlyColumns(columns ...string) (r *Batcher) {
	r = clone(b)
	r.onlyColumns = columns
	return
}

func (b *Batcher) MaxSqlParamsCount(count int) (r *Batcher) {
	r = clone(b)
	r.maxSqlParamsCount = count
	return
}

func (b *Batcher) ExcludeColumns(columns ...string) (r *Batcher) {
	r = clone(b)
	r.excludeColumns = columns
	return
}

func (b *Batcher) PreProcessors(procs ...RowPreProcessor) (r *Batcher) {
	r = clone(b)
	r.rowPreProcessors = procs
	return
}

func (b *Batcher) Rows(rows interface{}) (r *Batcher) {
	r = clone(b)
	r.rows = rows
	return
}

func (b *Batcher) Put() (err error) {
	val := reflect.ValueOf(b.rows)
	if val.Kind() != reflect.Array && val.Kind() != reflect.Slice {
		panic("parameter must be array or slice")
	}

	if val.Len() == 0 {
		return
	}

	v := val.Index(0).Interface()
	scp := b.db.NewScope(v)
	tableName := scp.TableName()
	if len(scp.PrimaryFields()) != 1 {
		return fmt.Errorf("table `%s` must have exactly one primary column, but has %v", tableName, fieldColumns(scp.PrimaryFields()))
	}

	fields := removeRelationships(b.calcColumns(scp.Fields()))

	columns := fieldColumns(fields)
	var primaryKeyColumn = scp.PrimaryKey()
	var rows [][]interface{}

	for i := 0; i < val.Len(); i++ {
		var row []interface{}
		e := val.Index(i)
		rowObj := e.Interface()
		var skip bool
		skip, err = b.processRow(rowObj)
		if err != nil {
			return
		}

		if skip {
			continue
		}

		rowScp := b.db.NewScope(rowObj)
		if rowScp.PrimaryKeyZero() {
			return fmt.Errorf("at rows %d, primary field `%s` value is zero, but is required", i+1, rowScp.PrimaryField().DBName)
		}
		for _, f := range fields {
			field, _ := rowScp.FieldByName(f.Name)
			row = append(row, field.Field.Interface())
		}

		rows = append(rows, row)
	}

	maxsc := b.maxSqlParamsCount
	if maxsc == 0 {
		maxsc = 65536
	}

	err = batchputs.CollectChangePutWithMaxSQLParamsCount(
		b.db.DB(),
		b.db.Dialect().GetName(),
		tableName,
		primaryKeyColumn,
		columns,
		rows,
		nil,
		maxsc,
	)
	if err != nil {
		return
	}
	return
}

func (b *Batcher) processRow(row interface{}) (skip bool, err error) {
	for _, proc := range b.rowPreProcessors {
		skip, err = proc(row)
		if err != nil {
			return
		}
	}
	return
}

func fieldColumns(fields []*gorm.Field) (columns []string) {
	for _, f := range fields {
		columns = append(columns, f.DBName)
	}
	return
}

func removeRelationships(fields []*gorm.Field) (results []*gorm.Field) {
	for _, f := range fields {
		if f.Relationship != nil {
			continue
		}
		results = append(results, f)
	}
	return
}

func (b *Batcher) calcColumns(fields []*gorm.Field) (results []*gorm.Field) {
	if len(b.onlyColumns) > 0 {
		onlyColumnsStr := strings.Join(b.onlyColumns, "/")
		for _, f := range fields {
			if strings.Index(onlyColumnsStr, f.DBName) >= 0 {
				results = append(results, f)
			}
		}
		return
	}

	if len(b.excludeColumns) > 0 {
		excludeColumnsStr := strings.Join(b.excludeColumns, "/")
		for _, f := range fields {
			if strings.Index(excludeColumnsStr, f.DBName) < 0 {
				results = append(results, f)
			}
		}
		return
	}

	results = fields
	return
}
