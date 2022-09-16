package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Address struct {
	ID int `db:"id"`
}

type Person struct {
	ID         int    `db:"id"`
	Fullname   string `db:"name"`
	PostalCode int    `db:"address_id"`
}

type Manager struct {
	Name string `db:"manager_name"`
}

type District struct {
}

type M map[string]any

func TestRound(t *testing.T) {
	var tests = []struct {
		input             string
		expectedParsed    string
		prepArgs          []any
		completeArgs      []any
		expectedCompleted string
	}{
		{
			"select p.* as &Person.*",
			"ParsedExpr[stringPart[select] outputPart[tableColumn[p.*] typeField[Person.*]]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select p.*",
		},
		{
			"select p.* AS&Person.*",
			"ParsedExpr[stringPart[select] outputPart[tableColumn[p.*] typeField[Person.*]]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select p.*",
		},
		{
			"select p.* as &Person.*, '&notAnOutputExpresion.*' as literal from t",
			"ParsedExpr[stringPart[select] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"stringPart[ '&notAnOutputExpresion.*'] " +
				"stringPart[ as literal from t]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select p.* ,  '&notAnOutputExpresion.*'  as literal from t",
		},
		{
			"select * as &Person.* from t",
			"ParsedExpr[stringPart[select] " +
				"outputPart[tableColumn[.*] typeField[Person.*]] " +
				"stringPart[ from t]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select *  from t",
		},
		{
			"select foo, bar from table where foo = $Person.ID",
			"ParsedExpr[stringPart[select foo, bar from table where foo =] " +
				"inputPart[Person.ID]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select foo, bar from table where foo = ?",
		},
		{
			"select &Person from table where foo = $Address.ID",
			"ParsedExpr[stringPart[select] outputPart[ typeField[Person.]] " +
				"stringPart[ from table where foo =] " +
				"inputPart[Address.ID]]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"select address_id, id, name  from table where foo = ?",
		},
		{
			"select &Person.* from table where foo = $Address.ID",
			"ParsedExpr[stringPart[select] " +
				"outputPart[ typeField[Person.*]] " +
				"stringPart[ from table where foo =] " +
				"inputPart[Address.ID]]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"select address_id, id, name  from table where foo = ?",
		},
		{
			"select foo, bar, &Person.ID from table where foo = 'xx'",
			"ParsedExpr[stringPart[select foo, bar,] " +
				"outputPart[ typeField[Person.ID]] " +
				"stringPart[ from table where foo =] " +
				"stringPart[ 'xx']]",
			[]any{&Person{}},
			[]any{&Person{}},
			"select foo, bar, id  from table where foo =  'xx'",
		},
		{
			"select foo, &Person.ID, bar, baz, &Manager.Name from table where foo = 'xx'",
			"ParsedExpr[stringPart[select foo,] " +
				"outputPart[ typeField[Person.ID]] " +
				"stringPart[, bar, baz,] " +
				"outputPart[ typeField[Manager.Name]] " +
				"stringPart[ from table where foo =] " +
				"stringPart[ 'xx']]",
			[]any{&Person{}, &Manager{}},
			[]any{&Person{}, &Manager{}},
			"select foo, id , bar, baz, manager_name  from table where foo =  'xx'",
		},
		{
			"SELECT * AS &Person.* FROM person WHERE name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[.*] " +
				"typeField[Person.*]] " +
				"stringPart[ FROM person WHERE name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT *  FROM person WHERE name =  'Fred'",
		},
		{
			"SELECT &Person.* FROM person WHERE name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[ typeField[Person.*]] " +
				"stringPart[ FROM person WHERE name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT address_id, id, name  FROM person WHERE name =  'Fred'",
		},
		{
			"SELECT * AS &Person.*, a.* as &Address.* FROM person, address a WHERE name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.*] typeField[Address.*]] " +
				"stringPart[ FROM person, address a WHERE name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"SELECT * , a.*  FROM person, address a WHERE name =  'Fred'",
		},
		{
			"SELECT (a.district, a.street) AS &Address.* FROM address AS a WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM address AS a WHERE p.name =] stringPart[ 'Fred']]",
			[]any{&Address{}},
			[]any{&Address{}},
			"SELECT a.district, a.street  FROM address AS a WHERE p.name =  'Fred'",
		},
		{
			"SELECT 1 FROM person WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT 1 FROM person WHERE p.name =] " +
				"stringPart[ 'Fred']]",
			[]any{},
			[]any{},
			"SELECT 1 FROM person WHERE p.name =  'Fred'",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.*, " +
				"(5+7), (col1 * col2) as calculated_value FROM person AS p " +
				"JOIN address AS a ON p.address_id = a.id WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[, (5+7), (col1 * col2) as calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"SELECT p.* , a.district, a.street , (5+7), (col1 * col2) as calculated_value FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =  'Fred'",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person AS p JOIN address AS a ON p .address_id = a.id " +
				"WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}},
			"SELECT p.* , a.district, a.street  FROM person AS p JOIN address AS a ON p .address_id = a.id WHERE p.name =  'Fred'",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
				"WHERE p.name in (select name from table where table.n = $Person.name)",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name in (select name from table where table.n =] " +
				"inputPart[Person.name] " +
				"stringPart[)]]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}, &Person{}},
			"SELECT p.* , a.district, a.street  FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name in (select name from table where table.n = ? )",
		},
		{
			"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person WHERE p.name in (select name from table " +
				"where table.n = $Person.name) UNION " +
				"SELECT p.* AS &Person.*, (a.district, a.street) AS &Address.* " +
				"FROM person WHERE p.name in " +
				"(select name from table where table.n = $Person.name)",
			"ParsedExpr[stringPart[SELECT] outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM person WHERE p.name in (select name from table where table.n =] " +
				"inputPart[Person.name] " +
				"stringPart[) UNION SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.district] tableColumn[a.street] typeField[Address.*]] " +
				"stringPart[ FROM person WHERE p.name in (select name from table where table.n =] " +
				"inputPart[Person.name] " +
				"stringPart[)]]",
			[]any{&Person{}, &Address{}},
			[]any{&Person{}, &Address{}, &Person{}, &Person{}, &Address{}, &Person{}},
			"SELECT p.* , a.district, a.street  FROM person WHERE p.name in (select name from table where table.n = ? ) UNION SELECT p.* , a.district, a.street  FROM person WHERE p.name in (select name from table where table.n = ? )",
		},
		{
			"SELECT p.* AS &Person.*, m.* AS &Manager.* " +
				"FROM person AS p JOIN person AS m " +
				"ON p.manager_id = m.id WHERE p.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.*]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[m.*] typeField[Manager.*]] " +
				"stringPart[ FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name =] " +
				"stringPart[ 'Fred']]",
			[]any{&Person{}, &Manager{}},
			[]any{&Person{}, &Manager{}},
			"SELECT p.* , m.*  FROM person AS p JOIN person AS m ON p.manager_id = m.id WHERE p.name =  'Fred'",
		},
		//{
		//	"SELECT (person.*, address.district) AS &M.* " +
		//		"FROM person JOIN address ON person.address_id = address.id " +
		//		"WHERE person.name = 'Fred'",
		//	"ParsedExpr[stringPart[SELECT] " +
		//		"outputPart[tableColumn[person.*] tableColumn[address.district] typeField[M.*]] " +
		//		"stringPart[ FROM person JOIN address ON person.address_id = address.id WHERE person.name =] " +
		//		"stringPart[ 'Fred']]",
		//	[]any{&M{}},
		//	[]any{&M{}},
		//},
		//{
		//	"SELECT p.*, a.district " +
		//		"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
		//		"WHERE p.name = $M.name",
		//	"ParsedExpr[stringPart[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =] " +
		//		"inputPart[M.name]]",
		//	[]any{&M{}},
		//	[]any{&M{}},
		//},
		{
			"SELECT person.*, address.district FROM person JOIN address " +
				"ON person.address_id = address.id WHERE person.name = 'Fred'",
			"ParsedExpr[stringPart[SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name =] " +
				"stringPart[ 'Fred']]",
			[]any{},
			[]any{},
			"SELECT person.*, address.district FROM person JOIN address ON person.address_id = address.id WHERE person.name =  'Fred'",
		},
		{
			"SELECT p FROM person WHERE p.name = $Person.name",
			"ParsedExpr[stringPart[SELECT p FROM person WHERE p.name =] inputPart[Person.name]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT p FROM person WHERE p.name = ?",
		},
		{
			"SELECT p.* AS &Person, a.District AS &District " +
				"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
				"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.District] typeField[District.]] " +
				"stringPart[ FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =] " +
				"inputPart[Person.name] " +
				"stringPart[ AND p.address_id =] " +
				"inputPart[Person.address_id]]",
			[]any{&Person{}, &District{}},
			[]any{&Person{}, &District{}, &Person{}, &Person{}},
			"SELECT p.* , a.District  FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ?  AND p.address_id = ?",
		},
		{
			"SELECT p.* AS &Person, a.District AS &District " +
				"FROM person AS p INNER JOIN address AS a " +
				"ON p.address_id = $Address.ID " +
				"WHERE p.name = $Person.name AND p.address_id = $Person.address_id",
			"ParsedExpr[stringPart[SELECT] " +
				"outputPart[tableColumn[p.*] typeField[Person.]] " +
				"stringPart[,] " +
				"outputPart[tableColumn[a.District] typeField[District.]] " +
				"stringPart[ FROM person AS p INNER JOIN address AS a ON p.address_id =] " +
				"inputPart[Address.ID] " +
				"stringPart[ WHERE p.name =] " +
				"inputPart[Person.name] " +
				"stringPart[ AND p.address_id =] " +
				"inputPart[Person.address_id]]",
			[]any{&Address{}, &Person{}, &District{}},
			[]any{&Person{}, &District{}, &Address{}, &Person{}, &Person{}},
			"SELECT p.* , a.District  FROM person AS p INNER JOIN address AS a ON p.address_id = ?  WHERE p.name = ?  AND p.address_id = ?",
		},
		{
			"SELECT p.*, a.district " +
				"FROM person AS p JOIN address AS a ON p.address_id = a.id " +
				"WHERE p.name = $Person.*",
			"ParsedExpr[stringPart[SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name =] " +
				"inputPart[Person.*]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"SELECT p.*, a.district FROM person AS p JOIN address AS a ON p.address_id = a.id WHERE p.name = ?",
		},
		{
			"INSERT INTO person (name) VALUES $Person.name",
			"ParsedExpr[stringPart[INSERT INTO person (name) VALUES] " +
				"inputPart[Person.name]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"INSERT INTO person (name) VALUES ?",
		},
		{
			"INSERT INTO person VALUES $Person.*",
			"ParsedExpr[stringPart[INSERT INTO person VALUES] " +
				"inputPart[Person.*]]",
			[]any{&Person{}},
			[]any{&Person{}},
			"INSERT INTO person VALUES ?",
		},
		{
			"UPDATE person SET person.address_id = $Address.ID " +
				"WHERE person.id = $Person.ID",
			"ParsedExpr[stringPart[UPDATE person SET person.address_id =] " +
				"inputPart[Address.ID] " +
				"stringPart[ WHERE person.id =] " +
				"inputPart[Person.ID]]",
			[]any{&Address{}, &Person{}},
			[]any{&Address{}, &Person{}},
			"UPDATE person SET person.address_id = ?  WHERE person.id = ?",
		},
	}

	parser := NewParser()
	for i, test := range tests {
		var parsedExpr *ParsedExpr
		var err error
		if parsedExpr, err = parser.Parse(test.input); parsedExpr.String() != test.expectedParsed {
			t.Errorf("Test %d Failed (Parse): input: %s\nexpected: %s\nactual: %s\n",
				i, test.input, test.expectedParsed, parsedExpr.String())
		}
		var preparedExpr *PreparedExpr
		if preparedExpr, err = parsedExpr.Prepare(test.prepArgs...); err != nil {
			t.Errorf("Test %d Failed (Prepare): input: %s\nparsed AST: %s\nerror: %s\n",
				i, test.input, test.expectedParsed, err)
		}
		if completedExpr, err := preparedExpr.Complete(test.completeArgs...); completedExpr.Sql() != test.expectedCompleted {
			t.Errorf("Test %d Failed (Complete):\ncompleted: '%s'\nexpected: '%s'"+
				"\nerror: %s types:%#v\n",
				i, completedExpr.Sql(), test.expectedCompleted, err, test.prepArgs)
		}
	}
}

// We fail Prepare when passing a type
// that does not have corresponding DSL piece in the statement
func TestSuperflousType(t *testing.T) {
	sql := "select foo as bar from t"
	parser := NewParser()
	parsed, err := parser.Parse(sql)
	assert.Equal(t, nil, err)
	_, err = parsed.Prepare(&Person{})
	assert.Equal(t, fmt.Errorf("superfluous type"), err)
}

// We fail if encounter more types than necessary
// during prepare phase
func TestSuperflousTypeV2(t *testing.T) {
	sql := "select foo as &Person, bar as &Manager from t"
	parser := NewParser()
	parsed, err := parser.Parse(sql)
	assert.Equal(t, nil, err)
	_, err = parsed.Prepare(&Person{}, &Manager{}, &Address{})
	assert.Equal(t, fmt.Errorf("superfluous type"), err)
}

// Statements without DSL parts should be passed unmodified
func TestPassthroughStatement(t *testing.T) {
	sql := "select foo as bar from t"
	parser := NewParser()
	parsed, err := parser.Parse(sql)
	assert.Equal(t, nil, err)
	prepared, err := parsed.Prepare()
	completed, err := prepared.Complete()
	assert.Equal(t, sql, completed.Sql())
}

// All DSL types in the statement must be satisfied
// during the prepare phase
func TestUnresolvedType(t *testing.T) {
	sql := "select foo as &Person, bar as &Manager from t where t.id = $Address.id"
	parser := NewParser()
	parsed, err := parser.Parse(sql)
	assert.Equal(t, nil, err)
	_, err = parsed.Prepare(&Person{}, &Address{})
	assert.Equal(t, fmt.Errorf("type info not present (Manager)"), err)
}

// We can not reflect on nil values
func TestNilTypeInPrepare(t *testing.T) {
	sql := "select foo as &Person from t"
	parser := NewParser()
	parsed, err := parser.Parse(sql)
	assert.Equal(t, nil, err)
	_, err = parsed.Prepare(nil)
	assert.Equal(t, fmt.Errorf("Can not reflect nil value"), err)
}

// Types in Prepare() should be unique.
func TestTypesNotUnique(t *testing.T) {
	sql := "select foo as &Person, bar as &Person from t"
	parser := NewParser()
	parsed, err := parser.Parse(sql)
	assert.Equal(t, nil, err)
	_, err = parsed.Prepare(&Person{}, &Person{})
	assert.Equal(t, fmt.Errorf("type 'Person' not unique"), err)
}

// We return a proper error when we find an unbound string literal
func TestUnfinishedStringLiteral(t *testing.T) {
	sql := "select foo from t where x = 'dddd"
	parser := NewParser()
	_, err := parser.Parse(sql)
	assert.Equal(t, fmt.Errorf("missing right quote in string literal"), err)
}

func TestUnfinishedStringLiteralV2(t *testing.T) {
	sql := "select foo from t where x = \"dddd"
	parser := NewParser()
	_, err := parser.Parse(sql)
	assert.Equal(t, fmt.Errorf("missing right quote in string literal"), err)
}

// We require to end the string literal with the proper quote depending
// on the opening one.
func TestUnfinishedStringLiteralV3(t *testing.T) {
	sql := "select foo from t where x = \"dddd'"
	parser := NewParser()
	_, err := parser.Parse(sql)
	assert.Equal(t, fmt.Errorf("missing right quote in string literal"), err)
}

// Detect bad input DSL pieces
func TestBadFormatInput(t *testing.T) {
	sql := "select foo from t where x = $.id"
	parser := NewParser()
	_, err := parser.Parse(sql)
	assert.Equal(t, fmt.Errorf("no qualifier in input expression"), err)
}

// Detect bad input DSL pieces
func TestBadFormatInputV2(t *testing.T) {
	sql := "select foo from t where x = $Address."
	parser := NewParser()
	_, err := parser.Parse(sql)
	assert.Equal(t, fmt.Errorf("expecting identifier after 'Address.'"), err)
}

// Detect bad output DSL pieces
func TestBadFormatOutput(t *testing.T) {
	sql := "select foo as && from t"
	parser := NewParser()
	_, err := parser.Parse(sql)
	assert.Equal(t, fmt.Errorf("malformed output expression"), err)
}

// Detect bad output DSL pieces
func TestBadFormatOutputV2(t *testing.T) {
	sql := "select foo as &.bar from t"
	parser := NewParser()
	_, err := parser.Parse(sql)
	assert.Equal(t, fmt.Errorf("malformed output expression"), err)
}

// We return a proper error when the number of parameters do not match
// the number of DSL pieces in the statement
func TestNumParemeterMismatch(t *testing.T) {
	sql := "select foo from t where x = $Address.id and y = $Person.postal_code"
	parser := NewParser()
	parsed, err := parser.Parse(sql)
	assert.Equal(t, nil, err)
	prepared, err := parsed.Prepare(&Address{}, &Person{})
	_, err = prepared.Complete(&Address{})
	assert.Equal(t, fmt.Errorf("parameters mismatch. expected 2, have 1"), err)
}
