package gormbatchputs_test

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/theplant/testingutils"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/jinzhu/now"
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

type DeliveryHub struct {
	gorm.Model

	Name string

	NumberOfDay uint

	DefaultSiteInterval uint

	TimeZone string

	CyclicDisposeWeekdays  SerializeUintArray `sql:"type:varchar(1024)"`
	CyclicDeliveryWeekdays SerializeUintArray `sql:"type:varchar(1024)"`

	DeliveryHours []DeliveryHour `gorm:"ForeignKey:DeliveryHubID"`

	SkipDisposeDays  []DeliveryDay `gorm:"ForeignKey:HubDisposeID"`
	SkipDeliveryDays []DeliveryDay `gorm:"ForeignKey:HubDeliveryID"`

	SiteIntervals []SiteInterval
}

type DeliveryDay struct {
	gorm.Model

	HubDisposeID  uint
	HubDeliveryID uint

	Date time.Time
}

type DeliveryHour struct {
	gorm.Model

	DeliveryHubID uint

	Name  string `sql:"type:varchar(100)" gorm:"unique_index:uidx_delivery_hour_name"`
	Value string `sql:"type:varchar(10)" gorm:"unique_index:uidx_delivery_hour_value"`
}

type SiteInterval struct {
	gorm.Model

	DeliveryHubID uint `gorm:"unique_index:uidx_site_interval_delivery_hub_id_name_alias"`

	Name  string `sql:"type:varchar(100)" gorm:"unique_index:uidx_site_interval_delivery_hub_id_name_alias"`
	Alias string `sql:"type:varchar(100)" gorm:"unique_index:uidx_site_interval_delivery_hub_id_name_alias"`

	Interval uint
}

type SerializeUintArray []uint

func (uints *SerializeUintArray) Scan(data interface{}) (err error) {
	var byteData []byte
	switch values := data.(type) {
	case []byte:
		byteData = values
	case string:
		byteData = []byte(values)
	default:
		err = errors.New("unsupported driver")
		return
	}

	err = json.Unmarshal(byteData, uints)

	return
}

func (uints SerializeUintArray) Value() (driver.Value, error) {
	return json.Marshal(uints)
}

var curDay = now.BeginningOfDay()

var hData = []*DeliveryHub{
	{
		Name:                   "Trinet",
		NumberOfDay:            20,
		DefaultSiteInterval:    5,
		CyclicDisposeWeekdays:  []uint{1, 2},
		CyclicDeliveryWeekdays: []uint{3},
		SkipDisposeDays: []DeliveryDay{
			{Date: curDay.AddDate(0, 0, -50)},
			{Date: curDay.AddDate(0, 0, -5)},
			{Date: curDay.AddDate(0, 0, 1)},
			{Date: curDay.AddDate(0, 0, 2)},
			{Date: curDay.AddDate(0, 0, 3)},
		},
		SkipDeliveryDays: []DeliveryDay{
			{Date: curDay.AddDate(0, 0, -30)},
			{Date: curDay.AddDate(0, 0, -3)},
			{Date: curDay.AddDate(0, 0, 4)},
			{Date: curDay.AddDate(0, 0, 10)},
			{Date: curDay.AddDate(0, 0, 19)},
		},
		DeliveryHours: []DeliveryHour{
			{Name: "無し", Value: "0"},
			{Name: "8〜12時", Value: "1"},
			{Name: "12〜14時", Value: "2"},
			{Name: "14〜16時", Value: "3"},
			{Name: "16〜18時", Value: "4"},
			{Name: "18〜20時", Value: "5"},
			{Name: "19〜21時", Value: "6"},
		},
		SiteIntervals: []SiteInterval{
			{Name: "北海道", Alias: "北海道", Interval: 4},
			{Name: "青森", Alias: "青森県", Interval: 2},
			{Name: "岩手", Alias: "岩手県", Interval: 2},
			{Name: "宮城", Alias: "宮城県", Interval: 1},
			{Name: "秋田", Alias: "秋田県", Interval: 2},
			{Name: "山形", Alias: "山形県", Interval: 1},
			{Name: "福島", Alias: "福島県", Interval: 1},
			{Name: "茨城", Alias: "茨城県", Interval: 1},
			{Name: "栃木", Alias: "栃木県", Interval: 1},
			{Name: "群馬", Alias: "群馬県", Interval: 1},
			{Name: "埼玉", Alias: "埼玉県", Interval: 1},
			{Name: "千葉", Alias: "千葉県", Interval: 1},
			{Name: "東京", Alias: "東京都", Interval: 1},
			{Name: "神奈川", Alias: "神奈川県", Interval: 1},
			{Name: "山梨", Alias: "山梨県", Interval: 1},
			{Name: "長野", Alias: "長野県", Interval: 1},
			{Name: "新潟", Alias: "新潟県", Interval: 1},
			{Name: "富山", Alias: "富山県", Interval: 1},
			{Name: "石川", Alias: "石川県", Interval: 1},
			{Name: "福井", Alias: "福井県", Interval: 1},
			{Name: "静岡", Alias: "静岡県", Interval: 1},
			{Name: "愛知", Alias: "愛知県", Interval: 1},
			{Name: "岐阜", Alias: "岐阜県", Interval: 1},
			{Name: "三重", Alias: "三重県", Interval: 1},
			{Name: "滋賀", Alias: "滋賀県", Interval: 1},
			{Name: "京都", Alias: "京都府", Interval: 1},
			{Name: "大阪", Alias: "大阪府", Interval: 1},
			{Name: "兵庫", Alias: "兵庫県", Interval: 2},
			{Name: "奈良", Alias: "奈良県", Interval: 2},
			{Name: "和歌山", Alias: "和歌山県", Interval: 2},
			{Name: "岡山", Alias: "岡山県", Interval: 2},
			{Name: "広島", Alias: "広島県", Interval: 2},
			{Name: "鳥取", Alias: "鳥取県", Interval: 2},
			{Name: "島根", Alias: "島根県", Interval: 2},
			{Name: "山口", Alias: "山口県", Interval: 2},
			{Name: "徳島", Alias: "徳島県", Interval: 2},
			{Name: "香川", Alias: "香川県", Interval: 2},
			{Name: "愛媛", Alias: "愛媛県", Interval: 2},
			{Name: "高知", Alias: "高知県", Interval: 2},
			{Name: "福岡", Alias: "福岡県", Interval: 2},
			{Name: "佐賀", Alias: "佐賀県", Interval: 2},
			{Name: "長崎", Alias: "長崎県", Interval: 2},
			{Name: "熊本", Alias: "熊本県", Interval: 3},
			{Name: "大分", Alias: "大分県", Interval: 2},
			{Name: "宮崎", Alias: "宮崎県", Interval: 3},
			{Name: "鹿児島", Alias: "鹿児島県", Interval: 4},
			{Name: "沖縄", Alias: "沖縄県", Interval: 2},
		},
	},
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
	data            interface{}
	onlyColumns     []string
	excludeColumns  []string
	preProcessors   []gormbatchputs.RowPreProcessor
	expectedResults []*Country
}{
	{
		name:        "only code and short_name",
		data:        data,
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
		data:           data,
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
		data:           data,
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
		data:           data,
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
		err := bputs.Put(c.data)
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
	d.AutoMigrate(
		&Country{},
		&DeliveryHub{},
		&DeliveryDay{},
		&DeliveryHour{},
		&SiteInterval{},
	)
	d.LogMode(true)

	if err != nil {
		panic(err)
	}
	return d
}
