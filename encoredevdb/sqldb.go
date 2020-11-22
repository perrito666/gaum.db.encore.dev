package encoredevdb

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	gaumErrors "github.com/ShiftLeftSecurity/gaum/db/errors"
	"github.com/ShiftLeftSecurity/gaum/db/logging"
	"github.com/ShiftLeftSecurity/gaum/db/srm"

	"encore.dev/storage/sqldb"
	"github.com/ShiftLeftSecurity/gaum/db/connection"
)

var _ connection.DatabaseHandler = &Connector{}
var _ connection.DB = &DB{}

// Connector implements connection.Handler
type Connector struct {
	ConnectionString string
}

// DefaultPGPoolMaxConn is an arbitrary number of connections that I decided was ok for the pool
const DefaultPGPoolMaxConn = 10

// Open opens a connection to postgres and returns it wrapped into a connection.DB
func (c *Connector) Open(ci *connection.Information) (connection.DB, error) {
	return &DB{}, nil
}

// DB wraps pgx.Conn into a struct that implements connection.DB
type DB struct {
	tx          *sqldb.Tx
	logger      logging.Logger
	execTimeout *time.Duration
}

// Clone returns a copy of DB with the same underlying Connection
func (d *DB) Clone() connection.DB {
	return &DB{
		logger: d.logger,
	}
}

func snakesToCamels(s string) string {
	var c string
	var snake bool
	for i, v := range s {
		if i == 0 {
			c += strings.ToUpper(string(v))
			continue
		}
		if v == '_' {
			snake = true
			continue
		}
		if snake {
			c += strings.ToUpper(string(v))
			continue
		}
		c += string(v)
	}
	return c
}

// EQueryIter Calls EscapeArgs before invoking QueryIter
func (d *DB) EQueryIter(statement string, fields []string, args ...interface{}) (connection.ResultFetchIter, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, fmt.Errorf("escaping arguments: %w", err)
	}
	return d.QueryIter(s, fields, a)
}

func (d *DB) wrapQueryInTx(ctx context.Context, statement string, args ...interface{}) (*sqldb.Rows, error) {
	return sqldb.QueryTx(d.tx, ctx, statement, args)
}

// QueryIter returns an iterator that can be used to fetch results one by one, beware this holds
// the connection until fetching is done.
// the passed fields are supposed to correspond to the fields being brought from the db, no
// check is performed on this.
func (d *DB) QueryIter(statement string, fields []string, args ...interface{}) (connection.ResultFetchIter, error) {
	var rows *sqldb.Rows
	var err error
	var connQ func(context.Context, string, ...interface{}) (*sqldb.Rows, error)
	if d.tx != nil {
		connQ = d.wrapQueryInTx
	} else {
		connQ = sqldb.Query
	}

	if len(args) != 0 {
		rows, err = connQ(context.TODO(), statement, args...)
	} else {
		rows, err = connQ(context.TODO(), statement)
	}
	if err != nil {
		return func(interface{}) (bool, func(), error) { return false, func() {}, nil },
			fmt.Errorf("querying database: %w", err)
	}

	var fieldMap map[string]reflect.StructField
	var typeName string
	if !rows.Next() {
		return func(interface{}) (bool, func(), error) { return false, func() {}, nil },
			sql.ErrNoRows
	}
	if len(fields) == 0 || (len(fields) == 1 && fields[0] == "*") {
		return nil, fmt.Errorf("could not fetch field information from query :%w", err)
	}
	return func(destination interface{}) (bool, func(), error) {
		var err error
		if reflect.TypeOf(destination).Elem().Name() != typeName {
			typeName, fieldMap, err = srm.MapFromPtrType(destination, []reflect.Kind{}, []reflect.Kind{
				reflect.Map, reflect.Slice,
			})
			if err != nil {
				defer rows.Close()
				return false, func() {}, fmt.Errorf("cant fetch data into %T: %w", destination, err)
			}
		}
		fieldRecipients := srm.FieldRecipientsFromType(d.logger, fields, fieldMap, destination)

		err = rows.Scan(fieldRecipients...)
		if err != nil {
			defer rows.Close()
			return false, func() {}, fmt.Errorf(
				"scanning values into recipient, connection was closed: %w", err)
		}

		return rows.Next(), func() { rows.Close() }, rows.Err()
	}, nil
}

// EQueryPrimitive calls EscapeArgs before invoking QueryPrimitive.
func (d *DB) EQueryPrimitive(statement string, field string, args ...interface{}) (connection.ResultFetch, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, fmt.Errorf("escaping arguments: %w", err)
	}
	return d.QueryPrimitive(s, field, a)
}

// QueryPrimitive returns a function that allows recovering the results of the query but to a slice
// of a primitive type, only allowed if the query fetches one field.
func (d *DB) QueryPrimitive(statement string, field string, args ...interface{}) (connection.ResultFetch, error) {
	var rows *sqldb.Rows
	var err error
	var connQ func(context.Context, string, ...interface{}) (*sqldb.Rows, error)
	if d.tx != nil {
		connQ = d.wrapQueryInTx
	} else {
		connQ = sqldb.Query
	}

	if len(args) != 0 {
		rows, err = connQ(context.TODO(), statement, args...)
	} else {
		rows, err = connQ(context.TODO(), statement)
	}
	if err != nil {
		return func(interface{}) error { return nil },
			fmt.Errorf("querying database: %w", err)
	}
	return func(destination interface{}) error {
		if reflect.TypeOf(destination).Kind() != reflect.Ptr {
			return fmt.Errorf("the passed receiver is not a pointer, connection is still open")
		}
		// TODO add a timer that closes rows if nothing is done.
		defer rows.Close()
		var err error
		reflect.ValueOf(destination).Elem().Set(reflect.MakeSlice(reflect.TypeOf(destination).Elem(), 0, 0))

		// Obtain the actual slice
		destinationSlice := reflect.ValueOf(destination).Elem()

		// If this is not Ptr->Slice->Type it would have failed already.
		tod := reflect.TypeOf(destination).Elem().Elem()

		for rows.Next() {
			// Get a New ptr to the object of the type of the slice.
			newElemPtr := reflect.New(tod)

			// Try to fetch the data
			err = rows.Scan(newElemPtr.Interface())
			if err != nil {
				defer rows.Close()
				return fmt.Errorf("scanning values into recipient, connection was closed: %w", err)
			}
			// Add to the passed slice, this will actually add to an already populated slice if one
			// passed, how cool is that?
			destinationSlice.Set(reflect.Append(destinationSlice, newElemPtr.Elem()))
		}
		return rows.Err()
	}, nil
}

// EQuery calls EscapeArgs before invoking Query
func (d *DB) EQuery(statement string, fields []string, args ...interface{}) (connection.ResultFetch, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, fmt.Errorf("escaping arguments: %w", err)
	}
	return d.Query(s, fields, a)
}

// Query returns a function that allows recovering the results of the query, beware the connection
// is held until the returned closusure is invoked.
func (d *DB) Query(statement string, fields []string, args ...interface{}) (connection.ResultFetch, error) {
	var rows *sqldb.Rows
	var err error
	var connQ func(context.Context, string, ...interface{}) (*sqldb.Rows, error)
	if d.tx != nil {
		connQ = d.wrapQueryInTx
	} else {
		connQ = sqldb.Query
	}

	if len(args) != 0 {
		rows, err = connQ(context.TODO(), statement, args...)
	} else {
		rows, err = connQ(context.TODO(), statement)
	}
	if err != nil {
		return func(interface{}) error { return nil },
			fmt.Errorf("querying database: %w", err)
	}
	var fieldMap map[string]reflect.StructField

	return func(destination interface{}) error {
		if reflect.TypeOf(destination).Kind() != reflect.Ptr {
			return fmt.Errorf("the passed receiver is not a pointer, connection is still open")
		}
		// TODO add a timer that closes rows if nothing is done.
		defer rows.Close()
		var err error
		reflect.ValueOf(destination).Elem().Set(reflect.MakeSlice(reflect.TypeOf(destination).Elem(), 0, 0))

		// Obtain the actual slice
		destinationSlice := reflect.ValueOf(destination).Elem()

		// If this is not Ptr->Slice->Type it would have failed already.
		tod := reflect.TypeOf(destination).Elem().Elem()

		if len(fields) == 0 || (len(fields) == 1 && fields[0] == "*") {
			// This seems to make a query each time so perhaps it goes outside.

			return fmt.Errorf("could not fetch field information from query :%w", err)
		}

		for rows.Next() {
			// Get a New ptr to the object of the type of the slice.
			newElemPtr := reflect.New(tod)
			// Get the concrete object
			var newElem reflect.Value
			var newElemType reflect.Type
			if tod.Kind() == reflect.Ptr {
				// Handle slice of pointer
				intermediatePtr := newElemPtr.Elem()
				concrete := tod.Elem()
				newElemType = concrete
				// this will most likely always be the case, but let's be defensive
				if intermediatePtr.IsNil() {
					concreteInstancePtr := reflect.New(concrete)
					intermediatePtr.Set(concreteInstancePtr)
				}
				newElem = intermediatePtr.Elem()
			} else {
				newElemType = newElemPtr.Elem().Type()
				newElem = newElemPtr.Elem()
			}
			// Get it's type.
			ttod := newElem.Type()

			// map the fields of the type to their potential sql names, this is the only "magic"
			fieldMap = make(map[string]reflect.StructField, ttod.NumField())
			_, fieldMap, err = srm.MapFromTypeOf(newElemType,
				[]reflect.Kind{}, []reflect.Kind{
					reflect.Map, reflect.Slice,
				})
			if err != nil {
				defer rows.Close()
				return fmt.Errorf("cant fetch data into %T: %w", destination, err)
			}

			// Construct the recipient fields.
			fieldRecipients := srm.FieldRecipientsFromValueOf(d.logger, fields, fieldMap, newElem)

			// Try to fetch the data
			err = rows.Scan(fieldRecipients...)
			if err != nil {
				defer rows.Close()
				return fmt.Errorf("scanning values into recipient, connection was closed: %w", err)
			}
			// Add to the passed slice, this will actually add to an already populated slice if one
			// passed, how cool is that?
			destinationSlice.Set(reflect.Append(destinationSlice, newElemPtr.Elem()))
		}
		return rows.Err()
	}, nil
}

// ERaw calls EscapeArgs before invoking Raw
func (d *DB) ERaw(statement string, args []interface{}, fields ...interface{}) error {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return fmt.Errorf("escaping arguments: %w", err)
	}
	return d.Raw(s, a, fields)
}

// Raw will run the passed statement with the passed args and scan the first resul, if any,
// to the passed fields.
func (d *DB) Raw(statement string, args []interface{}, fields ...interface{}) error {
	var rows *sqldb.Row

	if d.execTimeout != nil {
		ctx, cancel := context.WithTimeout(context.TODO(), *d.execTimeout)
		defer cancel()
		if d.tx != nil {
			rows = sqldb.QueryRowTx(d.tx, ctx, statement, args...)
		} else {
			rows = sqldb.QueryRow(ctx, statement, args...)
		}

	} else {
		if d.tx != nil {
			rows = sqldb.QueryRowTx(d.tx, context.TODO(), statement, args...)
		} else {
			rows = sqldb.QueryRow(context.TODO(), statement, args...)
		}
	}

	// Try to fetch the data
	err := rows.Scan(fields...)
	if err == sql.ErrNoRows {
		return gaumErrors.ErrNoRows
	}
	if err != nil {
		return fmt.Errorf("scanning values into recipient: %w", err)
	}
	return nil
}

// EExec calls EscapeArgs before invoking Exec
func (d *DB) EExec(statement string, args ...interface{}) error {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return fmt.Errorf("escaping arguments: %w", err)
	}
	return d.Exec(s, a...)
}

// Exec will run the statement and expect nothing in return.
func (d *DB) Exec(statement string, args ...interface{}) error {
	_, err := d.exec(statement, args...)
	return err
}

// ExecResult will run the statement and return the number of rows affected.
func (d *DB) ExecResult(statement string, args ...interface{}) (int64, error) {
	sqlResult, err := d.exec(statement, args...)
	if err != nil {
		return 0, err
	}
	return sqlResult.RowsAffected()
}

func (d *DB) exec(statement string, args ...interface{}) (sql.Result, error) {
	var connTag sql.Result
	var err error

	if d.execTimeout != nil {
		ctx, cancel := context.WithTimeout(context.TODO(), *d.execTimeout)
		defer cancel()
		if d.tx != nil {
			connTag, err = sqldb.ExecTx(d.tx, ctx, statement, args...)
		} else {
			connTag, err = sqldb.Exec(ctx, statement, args...)
		}
	} else {
		if d.tx != nil {
			connTag, err = sqldb.ExecTx(d.tx, context.TODO(), statement, args...)
		} else {
			connTag, err = sqldb.Exec(context.TODO(), statement, args...)
		}
	}
	if err != nil {
		return connTag, fmt.Errorf("querying database, obtained %v: %w", connTag, err)
	}
	return connTag, nil
}

// BeginTransaction returns a new DB that will use the transaction instead of the basic conn.
// if the transaction is already started the same will be returned.
func (d *DB) BeginTransaction() (connection.DB, error) {
	if d.tx != nil {
		return nil, gaumErrors.AlreadyInTX
	}
	tx, err := sqldb.Begin(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("trying to begin a transaction: %w", err)
	}
	return &DB{
		tx:     tx,
		logger: d.logger,
	}, nil
}

// IsTransaction indicates if the DB is in the middle of a transaction.
func (d *DB) IsTransaction() bool {
	return d.tx != nil
}

// CommitTransaction commits the transaction if any is in course, beavior comes straight from
// pgx.
func (d *DB) CommitTransaction() error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}

	return sqldb.Commit(d.tx)
}

// RollbackTransaction rolls back the transaction if any is in course, beavior comes straight from
// pgx.
func (d *DB) RollbackTransaction() error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}
	return sqldb.Rollback(d.tx)
}

// Set tries to run `SET LOCAL` with the passed parameters if there is an ongoing transaction.
// https://www.postgresql.org/docs/9.2/static/sql-set.html
func (d *DB) Set(set string) error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}
	// TODO check if this will work in the `SET LOCAL $1` arg format
	cTag, err := sqldb.ExecTx(d.tx, context.TODO(), "SET LOCAL "+set)
	if err != nil {
		return fmt.Errorf("trying to set local, returned: %s: %w", cTag, err)
	}
	return nil
}

// BulkInsert will use postgres copy function to try to insert a lot of data.
// You might need to use pgx types for the values to reduce probability of failure.
// https://godoc.org/github.com/jackc/pgx#Conn.CopyFrom
func (d *DB) BulkInsert(tableName string, columns []string, values [][]interface{}) (execError error) {
	return fmt.Errorf("not implemented")
}
