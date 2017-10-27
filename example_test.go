package gormbatchputs_test

import "github.com/theplant/gormbatchputs"
import "github.com/jinzhu/gorm"

/*
#### A list of rows, Put to multiple databases
*/
func ExampleNew_1ToManyDBs() {

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
}

/*
#### A database, Put many rows many times to different tables
*/
func ExampleNew_2OneDbPutManyTimes() {

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
}

/*
#### A database, Put only the same columns to different tables
*/
func ExampleNew_2OnlyCertainColumnsToDifferentTables() {
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
}
