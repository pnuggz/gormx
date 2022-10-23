package gormx

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/rogpeppe/fastuuid"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	// ErrNotInTransaction is returned when using Commit
	// outside of a transaction.
	ErrNotInTransaction = errors.New("not in transaction")

	// ErrIncompatibleOption is returned when using an option incompatible
	// with the selected driver.
	ErrIncompatibleOption = errors.New("incompatible option")

	// ErrInvalidGormDB is returned when a nil Gorm DB is used to
	// initialise Gormx
	ErrInvalidGormDB = errors.New("invalid Gorm DB")

	// ErrInvalidGormDBConfig is returned when a nil Gorm DB config is used to
	// initialise Gormx
	ErrInvalidGormDBConfig = errors.New("invalid Gorm DB config")
)

var uuids = fastuuid.MustNewGenerator()

// Gormx is a gorm db wrapper that can manage nested transactions.
type Gormx interface {
	// Ping tests the underlying sql connection.
	Ping() error
	// Close the underlying sql connection.
	Close() error
	// Begin a new transaction.
	Beginx() *gorm.DB
	// Begin a new transaction using the provided context and options.
	// Note that the provided parameters are only used when opening a new transaction,
	// not on nested ones.
	BeginTxx(ctx context.Context) *gorm.DB
	// Rollback the associated transaction.
	Rollback() error
	// Commit the assiociated transaction.
	Commit() error
	// Gorm returns the underlying Gorm DB.
	Gorm() *gorm.DB
	// Tx returns the underlying transaction.
	Tx() *gorm.DB
}

// New creates a new Gormx with the given DB.
func New(gorm *gorm.DB) (Gormx, error) {
	if gorm == nil {
		return nil, ErrInvalidGormDB
	}

	gormx := &gormx{
		gorm,
		nil,
		[]string{},
		true,
		0,
		0,
	}

	return gormx, nil
}

// Connect to a database.
func Connect(dataSourceName string, config *gorm.Config) (Gormx, error) {
	if config == nil {
		return nil, ErrInvalidGormDBConfig
	}

	db, err := gorm.Open(mysql.Open(dataSourceName), config)
	if err != nil {
		return nil, err
	}

	gormx, err := New(db)
	if err != nil {
		// the connection has been opened within this function, we must close it
		// on error.
		db, err := db.DB()
		if err != nil {
			return nil, err
		}
		db.Close()
		return nil, err
	}

	return gormx, nil
}

type gormx struct {
	*gorm.DB
	tx               *gorm.DB
	savePointIDs     []string
	savePointEnabled bool
	transactionCount int
	commitCount      int
}

func (g *gormx) Ping() error {
	if g.DB == nil {
		return ErrInvalidGormDB
	}

	db, err := g.DB.DB()
	if err != nil {
		return err
	}

	return db.Ping()
}

// Closes the underlying SQL database connection
func (g *gormx) Close() error {
	var db *sql.DB
	var err error

	if g.DB == nil {
		return ErrInvalidGormDB
	}

	db, err = g.DB.DB()
	if err != nil {
		return err
	}

	err = db.Close()
	if err == nil {
		g.DB = nil
	}

	return err
}

// Creates a new transaction with a background context
func (g *gormx) Beginx() *gorm.DB {
	return g.BeginTxx(context.Background())
}

// Creates a new transaction with a context
func (g *gormx) BeginTxx(ctx context.Context) *gorm.DB {
	if g.tx == nil {
		// new actual transaction
		db := g.DB.WithContext(ctx)
		g.tx = db.Begin()
	}

	g.transactionCount += 1

	// savepoints name must start with a char and cannot contain dashes (-)
	savePointID := "sp_" + strings.Replace(uuids.Hex128(), "-", "_", -1)
	g.savePointIDs = append(g.savePointIDs, savePointID)
	g.tx = g.tx.SavePoint(savePointID)

	return g.tx
}

// Rollback the transaction to a prior save point, or rollback the whole transaction
// all together if it is at the top level
func (g *gormx) Rollback() error {
	if g.tx == nil {
		return ErrNotInTransaction
	}

	g.transactionCount -= 1

	// if we are not at the top level then
	// just rollback to the previous level
	if g.transactionCount != g.commitCount {
		savePointID := g.savePointIDs[len(g.savePointIDs)-1]
		g.tx = g.tx.RollbackTo(savePointID)
		g.savePointIDs = g.savePointIDs[:len(g.savePointIDs)-1]
		return nil
	}

	g.tx.Rollback()
	g.tx = nil
	return nil
}

// Commits the transaction, or commit the whole transaction all together
// if it is at the number of nested transaction and commit count is equal
func (g *gormx) Commit() error {
	if g.tx == nil {
		return ErrNotInTransaction
	}

	g.commitCount += 1

	// If this is not the final commit, then
	// we just continue
	if g.transactionCount != g.commitCount {
		return nil
	}

	g.tx.Commit()
	g.tx = nil
	return nil
}

// Gorm returns the underlying gorm db.
func (g *gormx) Gorm() *gorm.DB {
	return g.DB
}

// Tx returns the underlying transaction.
func (g *gormx) Tx() *gorm.DB {
	return g.tx
}
