// Package squirrel provides a fluent SQL generator.
//
// See https://github.com/Masterminds/squirrel for examples.
package squirrel

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lann/builder"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// Sqlizer is the interface that wraps the ToSql method.
//
// ToSql returns a SQL representation of the Sqlizer, along with a slice of args
// as passed to e.g. database/sql.Exec. It can also return an error.
type Sqlizer interface {
	ToSql() (string, []interface{}, error)
}

// Yqliser is the interface that wraps the ToYQL method.
type Yqliser interface {
	ToYQL() (string, []table.ParameterOption, error)
}

// rawSqlizer is expected to do what Sqlizer does, but without finalizing placeholders.
// This is useful for nested queries.
type rawSqlizer interface {
	toSqlRaw() (string, []interface{}, error)
}

// Execer is the interface that wraps the Exec method.
//
// Exec executes the given query as implemented by database/sql.Exec.
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Queryer is the interface that wraps the Query method.
//
// Query executes the given query as implemented by database/sql.Query.
type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// QueryRower is the interface that wraps the QueryRow method.
//
// QueryRow executes the given query as implemented by database/sql.QueryRow.
type QueryRower interface {
	QueryRow(query string, args ...interface{}) RowScanner
}

// BaseRunner groups the Execer and Queryer interfaces.
type BaseRunner interface {
	Execer
	Queryer
}

// Runner groups the Execer, Queryer, and QueryRower interfaces.
type Runner interface {
	Execer
	Queryer
	QueryRower
}

// WrapStdSql wraps a type implementing the standard SQL interface with methods that
// squirrel expects.
func WrapStdSql(stdSql StdSql) Runner {
	return &stdsqlRunner{stdSql}
}

// StdSql encompasses the standard methods of the *sql.DB type, and other types that
// wrap these methods.
type StdSql interface {
	Query(string, ...interface{}) (*sql.Rows, error)
	QueryRow(string, ...interface{}) *sql.Row
	Exec(string, ...interface{}) (sql.Result, error)
}

type stdsqlRunner struct {
	StdSql
}

func (r *stdsqlRunner) QueryRow(query string, args ...interface{}) RowScanner {
	return r.StdSql.QueryRow(query, args...)
}

func setRunWith(b interface{}, runner BaseRunner) interface{} {
	switch r := runner.(type) {
	case StdSqlCtx:
		runner = WrapStdSqlCtx(r)
	case StdSql:
		runner = WrapStdSql(r)
	}
	return builder.Set(b, "RunWith", runner)
}

// RunnerNotSet is returned by methods that need a Runner if it isn't set.
var RunnerNotSet = fmt.Errorf("cannot run; no Runner set (RunWith)")

// RunnerNotQueryRunner is returned by QueryRow if the RunWith value doesn't implement QueryRower.
var RunnerNotQueryRunner = fmt.Errorf("cannot QueryRow; Runner is not a QueryRower")

// ExecWith Execs the SQL returned by s with db.
func ExecWith(db Execer, s Sqlizer) (res sql.Result, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.Exec(query, args...)
}

// QueryWith Querys the SQL returned by s with db.
func QueryWith(db Queryer, s Sqlizer) (rows *sql.Rows, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.Query(query, args...)
}

// QueryRowWith QueryRows the SQL returned by s with db.
func QueryRowWith(db QueryRower, s Sqlizer) RowScanner {
	query, args, err := s.ToSql()
	return &Row{RowScanner: db.QueryRow(query, args...), err: err}
}

// DebugSqlizer calls ToSql on s and shows the approximate SQL to be executed
//
// If ToSql returns an error, the result of this method will look like:
// "[ToSql error: %s]" or "[DebugSqlizer error: %s]"
//
// IMPORTANT: As its name suggests, this function should only be used for
// debugging. While the string result *might* be valid SQL, this function does
// not try very hard to ensure it. Additionally, executing the output of this
// function with any untrusted user input is certainly insecure.
func DebugSqlizer(s Sqlizer) string {
	sql, args, err := s.ToSql()
	if err != nil {
		return fmt.Sprintf("[ToSql error: %s]", err)
	}

	var placeholder string
	downCast, ok := s.(placeholderDebugger)
	if !ok {
		placeholder = "?"
	} else {
		placeholder = downCast.debugPlaceholder()
	}
	// TODO: dedupe this with placeholder.go
	buf := &bytes.Buffer{}
	i := 0
	for {
		p := strings.Index(sql, placeholder)
		if p == -1 {
			break
		}
		if len(sql[p:]) > 1 && sql[p:p+2] == "??" { // escape ?? => ?
			buf.WriteString(sql[:p])
			buf.WriteString("?")
			if len(sql[p:]) == 1 {
				break
			}
			sql = sql[p+2:]
		} else {
			if i+1 > len(args) {
				return fmt.Sprintf(
					"[DebugSqlizer error: too many placeholders in %#v for %d args]",
					sql, len(args))
			}
			buf.WriteString(sql[:p])
			fmt.Fprintf(buf, "'%v'", args[i])
			// advance our sql string "cursor" beyond the arg we placed
			sql = sql[p+1:]
			i++
		}
	}
	if i < len(args) {
		return fmt.Sprintf(
			"[DebugSqlizer error: not enough placeholders in %#v for %d args]",
			sql, len(args))
	}
	// "append" any remaning sql that won't need interpolating
	buf.WriteString(sql)
	return buf.String()
}

func prepareYQLString(sql string, args []interface{}) (string, error) {
	var sb strings.Builder
	for i, arg := range args {
		yqlArg, ok := arg.(types.Value)
		if !ok {
			return "", fmt.Errorf("arg %T is not ydb.Value", arg)
		}
		sb.WriteString(fmt.Sprintf("DECLARE $p%d AS ", i+1))
		sb.WriteString(yqlArg.Type().Yql())
		sb.WriteString(";\n")
	}
	sb.WriteString(sql)

	return sb.String(), nil
}

func prepareYQLParams(args []interface{}) ([]table.ParameterOption, error) {
	yqlArgs := make([]table.ParameterOption, 0, len(args))
	for i, arg := range args {
		yqlArgValue, ok := arg.(types.Value)
		if !ok {
			return nil, fmt.Errorf("arg %T is not ydb.Value", arg)
		}
		yqlArgs = append(yqlArgs, table.ValueParam(
			fmt.Sprintf("p%d", i+1), yqlArgValue,
		))
	}

	return yqlArgs, nil
}

func castArgsToYQL(args []interface{}) ([]interface{}, error) {
	if len(args) == 0 {
		return []interface{}(nil), nil
	}

	yqlArgs := make([]interface{}, 0, len(args))
	for _, arg := range args {
		switch arg.(type) {
		case types.Value:
			yqlArgs = append(yqlArgs, arg)
		default:
			castedYQLArgs, err := castArgToYQL(arg)
			if err != nil {
				return nil, fmt.Errorf("castArgToYQL: %w", err)
			}
			for _, yqlArg := range castedYQLArgs {
				yqlArgs = append(yqlArgs, yqlArg)
			}
		}
	}

	return yqlArgs, nil
}

func castArgToYQL(arg interface{}) ([]types.Value, error) {
	switch t := arg.(type) {
	case bool:
		return []types.Value{
			types.BoolValue(t),
		}, nil
	case *bool:
		return []types.Value{
			types.NullableBoolValue(t),
		}, nil
	case int:
		return []types.Value{
			types.Int64Value(int64(t)),
		}, nil
	case *int:
		tt := int64(*t)
		return []types.Value{
			types.NullableInt64Value(&tt),
		}, nil
	case int8:
		return []types.Value{
			types.Int8Value(t),
		}, nil
	case *int8:
		return []types.Value{
			types.NullableInt8Value(t),
		}, nil
	case int16:
		return []types.Value{
			types.Int16Value(t),
		}, nil
	case *int16:
		return []types.Value{
			types.NullableInt16Value(t),
		}, nil
	case int32:
		return []types.Value{
			types.Int32Value(t),
		}, nil
	case *int32:
		return []types.Value{
			types.NullableInt32Value(t),
		}, nil
	case int64:
		return []types.Value{
			types.Int64Value(t),
		}, nil
	case *int64:
		return []types.Value{
			types.NullableInt64Value(t),
		}, nil
	case uint:
		return []types.Value{
			types.Uint64Value(uint64(t)),
		}, nil
	case *uint:
		tt := uint64(*t)
		return []types.Value{
			types.NullableUint64Value(&tt),
		}, nil
	case uint8:
		return []types.Value{
			types.Uint8Value(t),
		}, nil
	case *uint8:
		return []types.Value{
			types.NullableUint8Value(t),
		}, nil
	case uint16:
		return []types.Value{
			types.Uint16Value(t),
		}, nil
	case *uint16:
		return []types.Value{
			types.NullableUint16Value(t),
		}, nil
	case uint32:
		return []types.Value{
			types.Uint32Value(t),
		}, nil
	case *uint32:
		return []types.Value{
			types.NullableUint32Value(t),
		}, nil
	case uint64:
		return []types.Value{
			types.Uint64Value(t),
		}, nil
	case *uint64:
		return []types.Value{
			types.NullableUint64Value(t),
		}, nil
	case float32:
		return []types.Value{
			types.FloatValue(t),
		}, nil
	case *float32:
		return []types.Value{
			types.NullableFloatValue(t),
		}, nil
	case float64:
		return []types.Value{
			types.DoubleValue(t),
		}, nil
	case *float64:
		return []types.Value{
			types.NullableDoubleValue(t),
		}, nil
	case string:
		return []types.Value{
			types.TextValue(t),
		}, nil
	case *string:
		return []types.Value{
			types.NullableTextValue(t),
		}, nil
	case []byte:
		return []types.Value{
			types.BytesValue(t),
		}, nil
	case time.Time:
		return []types.Value{
			types.TimestampValueFromTime(t),
		}, nil
	case *time.Time:
		return []types.Value{
			types.NullableTimestampValueFromTime(t),
		}, nil
	case json.RawMessage:
		if t == nil {
			return []types.Value{
				types.NullableJSONValueFromBytes(nil),
			}, nil
		}
		return []types.Value{
			types.JSONValueFromBytes(t),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported type `%T`", arg)
	}
}
