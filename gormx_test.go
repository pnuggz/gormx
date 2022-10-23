package gormx_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pnuggz/gormx"
	"github.com/pnuggz/gormx/models"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	port = 3366
)

func createConnection(t *testing.T) *gorm.DB {
	dataSource := fmt.Sprintf("gormx:gormx@tcp(localhost:%s)/gormx?charset=utf8mb4&parseTime=true", strconv.FormatInt(port, 10))

	db, err := gorm.Open(mysql.Open(dataSource), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		t.Errorf("%s", err)
		return nil
	}

	// creation of user table.
	db.Set("gorm:table_options", "ENGINE=InnoDB").AutoMigrate(
		&models.T1{},
		&models.T2{},
		&models.T3{},
	)

	db.Exec("truncate t1")
	db.Exec("truncate t2")
	db.Exec("truncate t3")

	return db
}

func TestGormxConnectMySQL(t *testing.T) {
	db := createConnection(t)
	sql, _ := db.DB()
	defer sql.Close()

	assert.IsType(t, &gorm.DB{}, db)
}

func TestNew(t *testing.T) {
	assert := assert.New(t)
	db := createConnection(t)
	sql, _ := db.DB()
	defer sql.Close()

	type testCase struct {
		name       string
		arg        *gorm.DB
		assertions func(gormx.Gormx, error)
	}

	testCases := []testCase{
		{
			name: "nil db",
			arg:  nil,
			assertions: func(gormx gormx.Gormx, err error) {
				assert.Nil(gormx)
				assert.Error(err)
			},
		},
		{
			name: "valid db",
			arg:  db,
			assertions: func(gormx gormx.Gormx, err error) {
				assert.NotNil(gormx)
				assert.NotNil(gormx.Gorm())
				assert.NoError(err)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gormx, err := gormx.New(tc.arg)
			tc.assertions(gormx, err)
		})
	}
}

func TestConnect(t *testing.T) {
	assert := assert.New(t)
	dataSource := fmt.Sprintf("gormx:gormx@tcp(localhost:%s)/gormx?charset=utf8mb4&parseTime=true", strconv.FormatInt(port, 10))
	gormConfig := new(gorm.Config)

	type testCase struct {
		name       string
		arg        string
		config     *gorm.Config
		assertions func(gormx.Gormx, error)
	}

	testCases := []testCase{
		{
			name:   "invalid config",
			arg:    "",
			config: nil,
			assertions: func(gormx gormx.Gormx, err error) {
				assert.Nil(gormx)
				assert.Error(err)
			},
		},
		{
			name:   "invalid datasource",
			arg:    "",
			config: gormConfig,
			assertions: func(gormx gormx.Gormx, err error) {
				assert.Nil(gormx)
				assert.Error(err)
			},
		},
		{
			name:   "valid datasource",
			arg:    dataSource,
			config: gormConfig,
			assertions: func(gormx gormx.Gormx, err error) {
				assert.NotNil(gormx)
				assert.NotNil(gormx.Gorm())
				assert.NoError(err)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gormx, err := gormx.Connect(tc.arg, tc.config)
			tc.assertions(gormx, err)

			if gormx != nil && gormx.Gorm() != nil {
				gormx.Close()
			}
		})
	}
}

func TestGormx_Ping(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		name       string
		setup      func(gormx.Gormx)
		assertions func(error)
	}

	testCases := []testCase{
		{
			name: "open db",
			setup: func(g gormx.Gormx) {

			},
			assertions: func(err error) {
				assert.NoError(err)
			},
		},
		{
			name: "closed db",
			setup: func(g gormx.Gormx) {
				g.Close()
			},
			assertions: func(err error) {
				assert.Error(err)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := createConnection(t)

			gx, _ := gormx.New(db)

			tc.setup(gx)

			err := gx.Ping()
			tc.assertions(err)

			gx.Close()
		})
	}
}

func TestGormx_Close(t *testing.T) {
	assert := assert.New(t)

	type testCase struct {
		name       string
		setup      func(gormx.Gormx)
		assertions func(error)
	}

	testCases := []testCase{
		{
			name: "open db",
			setup: func(g gormx.Gormx) {

			},
			assertions: func(err error) {
				assert.NoError(err)
			},
		},
		{
			name: "closed db",
			setup: func(g gormx.Gormx) {
				g.Close()
			},
			assertions: func(err error) {
				assert.NoError(err)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db := createConnection(t)

			gx, _ := gormx.New(db)

			tc.setup(gx)

			err := gx.Close()
			tc.assertions(err)
		})
	}
}

type T1 struct {
	ID string `json:"id" db:"id"`
}

type T2 struct {
	ID string `json:"id" db:"id"`
}

type T3 struct {
	ID string `json:"id" db:"id"`
}

func TestSingleCommit(t *testing.T) {
	db := createConnection(t)
	gx, _ := gormx.New(db)
	defer gx.Close()

	ctx := context.Background()

	txService := gx.BeginTxx(ctx)

	tx1 := gx.BeginTxx(ctx)
	tx1.Exec("INSERT INTO t1(id) VALUES('abc')")
	tx1.Commitx()

	txService.Commitx()

	var t1s []T1
	gx.Gorm().Find(&t1s)

	for _, t1 := range t1s {
		assert.Equal(t, "abc", t1.ID)
	}

	if len(t1s) == 0 {
		t.Errorf("commit didn't work")
	}
}

func TestSingleRollback(t *testing.T) {
	db := createConnection(t)
	gx, _ := gormx.New(db)
	defer gx.Close()

	ctx := context.Background()

	txService := gx.BeginTxx(ctx)

	tx1 := gx.BeginTxx(ctx)
	tx1.Exec("INSERT INTO t1(id) VALUES('abc')")
	tx1.Rollback()

	txService.Commitx()

	var t1s []T1
	gx.Gorm().Find(&t1s)

	for _, t1 := range t1s {
		assert.Equal(t, "abc", t1.ID)
	}

	if len(t1s) != 0 {
		t.Errorf("rollback didn't work")
	}
}

func TestSingleCommitAndSingleRollback(t *testing.T) {
	db := createConnection(t)
	gx, _ := gormx.New(db)
	defer gx.Close()

	fmt.Println("")

	ctx := context.Background()

	gx.BeginTxx(ctx)

	tx1 := gx.BeginTxx(ctx)
	tx1.Exec("INSERT INTO t1(id) VALUES('abc')")
	gx.Commitx()

	tx2 := gx.BeginTxx(ctx)
	fmt.Println(tx2)
	tx2.Exec("INSERT INTO t2(id) VALUES('abc')")
	gx.Rollbackx()

	gx.Commitx()

	var t1s []T1
	gx.Gorm().Find(&t1s)

	for _, t1 := range t1s {
		assert.Equal(t, "abc", t1.ID)
	}

	if len(t1s) == 0 {
		t.Errorf("commit didn't work")
	}

	var t2s []T2
	gx.Gorm().Find(&t2s)

	for _, t2 := range t2s {
		assert.Equal(t, "abc", t2.ID)
	}

	if len(t2s) != 0 {
		t.Errorf("rollback didn't work")
	}
}

func TestDoubleCommitAndSingleRollback(t *testing.T) {
	db := createConnection(t)
	gx, _ := gormx.New(db)
	defer gx.Close()

	ctx := context.Background()

	txService := gx.BeginTxx(ctx)

	tx1 := gx.BeginTxx(ctx)
	tx1.Exec("INSERT INTO t1(id) VALUES('abc')")
	tx1.Commitx()

	tx2 := gx.BeginTxx(ctx)
	tx2.Exec("INSERT INTO t2(id) VALUES('abc')")
	tx2.Commitx()

	tx3 := gx.BeginTxx(ctx)
	tx3.Exec("INSERT INTO t3(id) VALUES('abc')")
	tx3.Rollbackx()

	txService.Commitx()

	var t1s []T1
	gx.Gorm().Find(&t1s)

	if len(t1s) == 0 {
		t.Errorf("commit didn't work")
	}

	var t2s []T2
	gx.Gorm().Find(&t2s)

	if len(t2s) == 0 {
		t.Errorf("commit didn't work")
	}

	var t3s []T3
	gx.Gorm().Find(&t3s)

	if len(t3s) != 0 {
		t.Errorf("rollback didn't work")
	}
}

func TestDoubleCommitAndSingleRollbackAndAllRollback(t *testing.T) {
	db := createConnection(t)
	gx, _ := gormx.New(db)
	defer gx.Close()

	ctx := context.Background()

	txService := gx.BeginTxx(ctx)

	tx1 := gx.BeginTxx(ctx)
	tx1.Exec("INSERT INTO t1(id) VALUES('abc')")
	tx1.Commitx()

	tx2 := gx.BeginTxx(ctx)
	tx2.Exec("INSERT INTO t2(id) VALUES('abc')")
	tx2.Commitx()

	tx3 := gx.BeginTxx(ctx)
	tx3.Exec("INSERT INTO t3(id) VALUES('abc')")
	tx3.Rollbackx()

	txService.Rollbackx()

	var t1s []T1
	gx.Gorm().Find(&t1s)

	if len(t1s) != 0 {
		t.Errorf("rollback didn't work")
	}

	var t2s []T2
	gx.Gorm().Find(&t2s)

	if len(t2s) != 0 {
		t.Errorf("rollback didn't work")
	}

	var t3s []T3
	gx.Gorm().Find(&t3s)

	if len(t3s) != 0 {
		t.Errorf("rollback didn't work")
	}
}
