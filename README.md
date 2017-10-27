




* [Type Batcher](#type-batcher)
  * [New](#batcher-new)
  * [Exclude Columns](#batcher-exclude-columns)
  * [Only Columns](#batcher-only-columns)
  * [Pre Processors](#batcher-pre-processors)
  * [Put](#batcher-put)
  * [Rows](#batcher-rows)
  * [Verbose](#batcher-verbose)
  * [With DB](#batcher-with-db)
* [Type Row Pre Processor](#type-row-pre-processor)






## Type: Batcher
``` go
type Batcher struct {
    // contains filtered or unexported fields
}
```






### Batcher: New
``` go
func New() (b *Batcher)
```

#### A list of rows, Put to multiple databases
```go
	db := openAndMigrate()
	bputs := gormbatchputs.New().Rows([]*Country{
	    {
	        Code:      "CHN",
	        ShortName: "China",
	    },
	    {
	        Code:      "US",
	        ShortName: "America",
	    },
	})
	
	dbs := []*gorm.DB{db, db, db}
	for _, d := range dbs {
	    bputs.WithDB(d).Put()
	}
```

#### A database, Put many rows many times to different tables
```go
	db := openAndMigrate()
	bputs := gormbatchputs.New().WithDB(db)
	
	err := bputs.Rows([]*Country{
	    {
	        Code:       "CHN",
	        ShortName:  "China",
	        Population: 13e8,
	    },
	    {
	        Code:       "US",
	        ShortName:  "America",
	        Population: 5e8,
	    },
	}).Put()
	if err != nil {
	    panic(err)
	}
	
	err = bputs.Rows([]*City{
	    {
	        Code:       "HZ",
	        Name:       "Hangzhou",
	        Population: 8e6,
	    },
	    {
	        Code:       "SH",
	        Name:       "Shanghai",
	        Population: 1e8,
	    },
	}).Put()
	if err != nil {
	    panic(err)
	}
```

#### A database, Put only the same columns to different tables
```go
	countries := []*Country{
	    {
	        Code:       "CHN",
	        ShortName:  "China",
	        Population: 13e8,
	    },
	    {
	        Code:       "US",
	        ShortName:  "America",
	        Population: 5e8,
	    },
	}
	cities := []*City{
	    {
	        Code:       "HZ",
	        Name:       "Hangzhou",
	        Population: 8e6,
	    },
	    {
	        Code:       "SH",
	        Name:       "Shanghai",
	        Population: 1e8,
	    },
	}
	db := openAndMigrate()
	bputs := gormbatchputs.New().WithDB(db).OnlyColumns("code", "population")
	
	err := bputs.Rows(countries).Put()
	if err != nil {
	    panic(err)
	}
	
	err = bputs.Rows(cities).Put()
	if err != nil {
	    panic(err)
	}
```



### Batcher: Exclude Columns
``` go
func (b *Batcher) ExcludeColumns(columns ...string) (r *Batcher)
```



### Batcher: Only Columns
``` go
func (b *Batcher) OnlyColumns(columns ...string) (r *Batcher)
```



### Batcher: Pre Processors
``` go
func (b *Batcher) PreProcessors(procs ...RowPreProcessor) (r *Batcher)
```



### Batcher: Put
``` go
func (b *Batcher) Put() (err error)
```



### Batcher: Rows
``` go
func (b *Batcher) Rows(rows interface{}) (r *Batcher)
```



### Batcher: Verbose
``` go
func (b *Batcher) Verbose() (r *Batcher)
```



### Batcher: With DB
``` go
func (b *Batcher) WithDB(db *gorm.DB) (r *Batcher)
```



## Type: Row Pre Processor
``` go
type RowPreProcessor func(row interface{}) (skip bool, err error)
```










