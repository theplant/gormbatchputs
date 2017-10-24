package gormbatchputs_test

import (
	"os"
	"testing"

	"github.com/theplant/testingutils"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
	"github.com/theplant/gofixtures"
	"github.com/theplant/gormbatchputs"
)

type Country struct {
	Code         string `gorm:"primary_key" sql:"size:50"`
	ShortName    string `sql:"size:500"`
	SpecialNotes string `sql:"size:2000"`
	Region       string `sql:"size:500"`
	IncomeGroup  string `sql:"size:500"`
	Count        int
	AvgAge       float64
	Tall         bool
}

var data = []*Country{
	{
		Code:         "BEN",
		ShortName:    "Benin",
		SpecialNotes: `Benin.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1985.  National accounts reference year: .  Latest population census: 2013.  Latest household survey: Multiple Indicator Cluster Survey (MICS), 2014.`,
		Region:       "Sub-Saharan Africa",
		IncomeGroup:  "",
		Count:        0,
		AvgAge:       0,
		Tall:         true,
	},
	{
		Code:         "BFA",
		ShortName:    "Burkina Faso",
		SpecialNotes: `Burkina Faso.  Region: Sub-Saharan Africa.  Income group: Low income.  Lending category: IDA.  Currency unit: CFA franc.  National accounts base year: 1999.  National accounts reference year: .  Latest population census: 2006.  Latest household survey: Malaria Indicator Survey (MIS), 2014.`,
		Region:       "Sub-Saharan Africa",
		IncomeGroup:  "",
		Count:        2,
		AvgAge:       2,
	},
	{
		Code:         "BGD",
		ShortName:    "Bangladesh",
		SpecialNotes: `Bangladesh.  Region: South Asia.  Income group: Lower middle income.  Lending category: IDA.  Currency unit: Bangladeshi taka.  National accounts base year: 2005/06.  National accounts reference year: .  Latest population census: 2011.  Latest household survey: Demographic and Health Survey (DHS), 2014; HIV/Maternal and Child Health (HIV/MCH) Service Provision Assessments (SPA), 2014.  Special notes: Fiscal year end: June 30; reporting period for national accounts data: FY. The new base year is 2005/06.`,
		Region:       "South Asia",
		IncomeGroup:  "",
		Count:        1,
		AvgAge:       1,
	},
}

var putCases = []struct {
	name            string
	onlyColumns     []string
	excludeColumns  []string
	preProcessors   []gormbatchputs.RowPreProcessor
	expectedResults []*Country
}{
	{
		name:        "only code and short_name",
		onlyColumns: []string{"code", "short_name"},
		expectedResults: []*Country{
			{
				Code:      "BEN",
				ShortName: "Benin",
			},
			{
				Code:      "BFA",
				ShortName: "Burkina Faso",
			},
			{
				Code:      "BGD",
				ShortName: "Bangladesh",
			},
		},
	},
	{
		name:           "exclude special_notes",
		excludeColumns: []string{"special_notes"},
		expectedResults: []*Country{
			{
				Code:        "BEN",
				ShortName:   "Benin",
				Region:      "Sub-Saharan Africa",
				IncomeGroup: "",
				Count:       0,
				AvgAge:      0,
				Tall:        true,
			},
			{
				Code:        "BFA",
				ShortName:   "Burkina Faso",
				Region:      "Sub-Saharan Africa",
				IncomeGroup: "",
				Count:       2,
				AvgAge:      2,
			},
			{
				Code:        "BGD",
				ShortName:   "Bangladesh",
				Region:      "South Asia",
				IncomeGroup: "",
				Count:       1,
				AvgAge:      1,
			},
		},
	},
	{
		name:           "set count to 100",
		excludeColumns: []string{"special_notes"},
		preProcessors: []gormbatchputs.RowPreProcessor{
			func(row interface{}) (skip bool, err error) {
				c := row.(*Country)
				c.Count = 100
				return
			},
		},
		expectedResults: []*Country{
			{
				Code:        "BEN",
				ShortName:   "Benin",
				Region:      "Sub-Saharan Africa",
				IncomeGroup: "",
				Count:       100,
				AvgAge:      0,
				Tall:        true,
			},
			{
				Code:        "BFA",
				ShortName:   "Burkina Faso",
				Region:      "Sub-Saharan Africa",
				IncomeGroup: "",
				Count:       100,
				AvgAge:      2,
			},
			{
				Code:        "BGD",
				ShortName:   "Bangladesh",
				Region:      "South Asia",
				IncomeGroup: "",
				Count:       100,
				AvgAge:      1,
			},
		},
	},
	{
		name:           "skip BEN",
		excludeColumns: []string{"special_notes"},
		preProcessors: []gormbatchputs.RowPreProcessor{
			func(row interface{}) (skip bool, err error) {
				c := row.(*Country)
				if c.Code == "BEN" {
					skip = true
				}
				return
			},
		},
		expectedResults: []*Country{
			{
				Code:        "BFA",
				ShortName:   "Burkina Faso",
				Region:      "Sub-Saharan Africa",
				IncomeGroup: "",
				Count:       100,
				AvgAge:      2,
			},
			{
				Code:        "BGD",
				ShortName:   "Bangladesh",
				Region:      "South Asia",
				IncomeGroup: "",
				Count:       100,
				AvgAge:      1,
			},
		},
	},
}

var EmptyData = gofixtures.Data(
	gofixtures.Sql(
		``,
		[]string{
			"countries",
		},
	),
)

func TestPut(t *testing.T) {
	db := openAndMigrate()
	for _, c := range putCases {
		EmptyData.TruncatePut(db)
		bputs := gormbatchputs.New(db).
			Verbose().
			ExcludeColumns(c.excludeColumns...).
			OnlyColumns(c.onlyColumns...).
			PreProcessors(c.preProcessors...)
		err := bputs.Put(data)
		if err != nil {
			t.Fatal(err)
		}
		var countries []*Country
		err = db.Order("code ASC").Find(&countries).Error
		if err != nil {
			t.Fatal(err)
		}

		diff := testingutils.PrettyJsonDiff(c.expectedResults, countries)
		if len(diff) > 0 {
			t.Error(c.name, diff)
		}
	}
}

func openAndMigrate() *gorm.DB {
	d, err := gorm.Open(os.Getenv("DB_DIALECT"), os.Getenv("DB_PARAMS"))
	if err != nil {
		panic(err)
	}
	d.DropTable(&Country{})
	d.AutoMigrate(&Country{})
	d.LogMode(true)

	if err != nil {
		panic(err)
	}
	return d
}
