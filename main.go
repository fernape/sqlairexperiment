package main

import (
	"database/sql"
	"fmt"
	"reflect"
	sqlairreflect "sqlairtest/reflect"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type Parser struct { // AF: It'd be nice to explain what each field represents here
	parts   []Part
	str     string //TODO: change to input
	parsed  int
	skipped int
}

func NewParser() *Parser {
	return &Parser{}
}

// advance moves the parser's index forward
// by one element.
func (p *Parser) advance() bool {
	skipableBytes := map[byte]bool{
		',': true,
		'.': true,
		')': true,
		'=': true,
		'*': true,
		'/': true,
		'+': true,
	}
	p.skipSpaces()
	mark := p.skipped
	for p.skipped < len(p.str) &&
		(isNameByte(p.str[p.skipped]) || skipableBytes[p.str[p.skipped]]) {
		p.skipped++
	}
	return p.skipped != mark
}

// AF: skipped is current pos - is that a good name?
// AF: Would the file ordering be better if the helper functions came first?
func (p *Parser) parseStringLiteral() error {
	cp := p.save()
	p.skipSpaces()
	if p.skipped < len(p.str) {
		c := p.str[p.skipped]
		if c == '"' || c == '\'' {
			p.skipByte(c)
			if !p.skipByteFind(c) {
				// Reached end of string
				// and didn't find the closing quote
				p.add(cp, &stringPart{p.str[p.parsed:]})
				return fmt.Errorf("Missing right quote in string literal")
			}
			p.add(cp, &stringPart{p.str[cp.skipped:p.skipped]})
			return nil
		}
	}
	cp.restore()
	return nil
}

// parseQualifiedExpression parses an expression of the form
// qualifier.colName
// It should parse things like p.* in "Select p.* as..."
// and Person.name in "Select p.name as &Person.name from..."
// It is not an error if the qualifier OR the colName are empty // AF: you mean XOR or OR?
func (p *Parser) parseQualifiedExpression() (qualifiedName, error) {
	cp := p.save()
	var qn qualifiedName
	if id, ok := p.parseIdentifier(); ok {
		qn.Left = id
		if p.skipByte('.') {
			if name, ok := p.parseIdentifier(); ok {
				qn.Right = name
			} else {
				// There is nothing to the right of the '.'.
				// This is an error
				return qualifiedName{}, fmt.Errorf("expecting identifier after '%s.'", qn.Left)
			}
		}
		return qn, nil
	} else {
		cp.restore()
		return qualifiedName{}, nil
	}
}

// I need to ask fernando about this.
// What how does parseQualifiedExpression tell you that it cant parse it, but hasnt thrown an error.
//func (p *Parser) parseMultQualifiedExpressions() ([]qualifiedName, error) {
//	cp := p.save()
//	qe1, err := p.parseQualifiedExpression()
//	if err != nil {
//		return nil, err
//	} else if qe1 != nil {
//
//	}
//	cp.restore()
//}

func (p *Parser) parseInputExpression() error {
	cp := p.save()
	defer cp.autorestore()
	p.skipSpaces()
	if p.skipByte('$') {
		if qe, err := p.parseQualifiedExpression(); err == nil {
			if qe.Left == "" {
				return fmt.Errorf("No qualifier in input expression")
			}
			p.add(cp, &inputPart{typeField{qe.Left, qe.Right}})
		} else {
			return err
		}
	}
	return nil
}

func (p *Parser) parseOutputExpression() error {
	cp := p.save()
	// Try to parse as much as possible.
	// From:
	//  &Foo
	// to:
	//  foo.bar AS &Baz.xxx

	p.skipSpaces()
	if p.skipByte('&') {
		if qe, err := p.parseQualifiedExpression(); err == nil {
			if qe.Left == "" {
				return fmt.Errorf("Malformed output expression")
			}
			p.add(cp, &outputPart{[]tableColumn{},
				[]typeField{{qe.Left, qe.Right}}})
			return nil
		} else {
			return err
		}
	}
	// We really need to do some looping here to account for e.g.: "p.c, p.d"
	if dc, err := p.parseQualifiedExpression(); err == nil {
		// parseQualifiedExpression does not know if it is parsing
		// the left or right side of an "AS": p.* AS &Person
		// It will return Left as the element at the left of the "."
		// and Right (if any) as the right of the "."
		// When parsing the left side of "AS", if dc.Right is empty,
		// it means we parsed something like: p AS... and that "p"
		// refers to a column, not a table, so swap Left and Right
		if dc.Right == "" {
			dc.Right = dc.Left
			dc.Left = ""
		}
		p.skipSpaces()
		if p.skipString("AS") {
			p.skipSpaces()
			if p.skipByte('&') {
				if qe, err := p.parseQualifiedExpression(); err == nil {
					if qe.Left == "" {
						return fmt.Errorf("Malformed output expression")
					}
					p.add(cp, &outputPart{[]tableColumn{{dc.Left, dc.Right}},
						[]typeField{{qe.Left, qe.Right}}})
					return nil
				} else {
					return err
				}
			}
		}
	} else {
		return err
	}
	cp.restore()
	return nil
}

func (p *Parser) parseColumnGroup() bool {
	cp := p.save()
	p.skipSpaces()
	if !p.skipByte('(') {
		cp.restore()
		return false
	}
	var tclist []tableColumn
	for p.skipped < len(p.str) && !p.peekByte(')') {
		p.skipSpaces()
		if tc, err := p.parseQualifiedExpression(); err == nil {
			tclist = append(tclist, tableColumn{tc.Left, tc.Right})
			p.skipSpaces()
			if !p.skipByte(',') {
				break
			}
		}
	}
	if p.skipByte(')') {
		//FIXME: review this line
		p.skipped++
		p.skipSpaces()
		if p.skipString("AS") {
			p.skipSpaces()
			if !p.skipByte('&') {
				cp.restore()
				fmt.Println("expected '&'")
				return false
			}
			if tp, err := p.parseQualifiedExpression(); err == nil {
				p.add(cp, &outputPart{Columns: tclist, Fields: []typeField{{tp.Left, tp.Right}}})
				return true
			} else {
				fmt.Println("expecting AS <TypeDefinition>")
				cp.restore()
				return false
			}
		} else {
			// If there is no AS, it is not an error.
			// This is just a parenthesized group of things
			// Note that most databases do not support something
			// like: select (a, b) from t
			// But it is not our purpose to check SQL syntax.
			cp.restore()
			p.skipByteFind(')')
			return false
		}
	}

	cp.restore()
	p.skipped++
	p.skipped++
	return false
}

// AF: So this parses a name such as address in Person.address,
// which can also be *
func (p *Parser) parseIdentifier() (string, bool) {
	if p.skipped >= len(p.str) {
		return "", false
	}
	if p.peekByte('*') {
		p.skipped++
		return "*", true
	}
	mark := p.skipped
	if !isNameByte(p.str[p.skipped]) {
		return "", false
	}
	// could you not write for i := p.skipped; i < len(p.str); i++ {...
	// and leave out the var i int?
	var i int
	for i = p.skipped; i < len(p.str); i++ {
		if !isNameByte(p.str[i]) {
			break
		}
	}
	p.skipped = i
	return p.str[mark:i], true
}

func (p *Parser) peekByte(b byte) bool {
	return p.skipped < len(p.str) && p.str[p.skipped] == b
}

func (p *Parser) skipByte(b byte) bool {
	if p.skipped < len(p.str) && p.str[p.skipped] == b {
		p.skipped++
		return true
	}
	return false
}

func (p *Parser) skipByteFind(b byte) bool {
	for i := p.skipped; i < len(p.str); i++ {
		if p.str[i] == b {
			p.skipped = i + 1
			return true
		}
	}
	return false
}

func (p *Parser) skipSpaces() bool {
	mark := p.skipped
	for p.skipped < len(p.str) {
		if p.str[p.skipped] != ' ' {
			break
		}
		p.skipped++
	}
	return p.skipped != mark
}

func (p *Parser) skipString(s string) bool {
	if p.skipped+len(s) <= len(p.str) && strings.EqualFold(p.str[p.skipped:p.skipped+len(s)], s) {
		p.skipped += len(s)
		return true
	}
	return false
}

// Could do with a better name, prehaps isAlphanumericByte or something
func isNameByte(c byte) bool {
	return 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' ||
		'0' <= c && c <= '9' || c == '_'
}

func (p *Parser) add(cp *checkpoint, part Part) {
	if cp.skipped != p.parsed {
		p.parts = append(p.parts, &stringPart{p.str[p.parsed:cp.skipped]})
	}
	if part != nil {
		p.parts = append(p.parts, part)
	}
	p.parsed = p.skipped
}

func (p *Parser) save() *checkpoint {
	return &checkpoint{
		parser:   p,
		numParts: len(p.parts),
		skipped:  p.skipped,
		parsed:   p.parsed,
	}
}

type checkpoint struct {
	parser   *Parser
	numParts int
	skipped  int
	parsed   int
}

func (cp *checkpoint) restore() {
	cp.parser.parts = cp.parser.parts[:cp.numParts]
	cp.parser.skipped = cp.skipped
	cp.parser.parsed = cp.parsed
}

// This may become useful for defers
func (cp *checkpoint) autorestore() {
	if cp.parser.parsed < cp.skipped {
		cp.restore()
	}
}

// ParsedExpr represents a parsed expression.
// It has a representation of the original SQL statement in terms of Parts
// A SQL statement like this:
//
// Select p.* as &Person.* from person where p.name = $Boss.Name
//
// would be represented as:
//
// [stringPart outputPart stringPart inputPart]
type ParsedExpr struct {
	parts []Part
}

func generateOutputInfo(op *outputPart, targetInfo sqlairreflect.Info) OutputInfo {
	targetStruct := targetInfo.(sqlairreflect.Struct)

	tagNameList := make([]string, 0)
	for tagName, _ := range targetStruct.Fields { // range over a map iterates over key/value pairs
		tagNameList = append(tagNameList, tagName)
	}

	outputCols := make([]string, 0)

	for _, column := range op.Columns {
		colName := column.Column
		if colName == "*" {
			outputCols = append(outputCols, tagNameList...)

		} else {
			for _, tagName := range tagNameList {
				if tagName == colName {
					outputCols = append(outputCols, colName)
				}
			}
		}

	}
	return OutputInfo{outputCols, targetStruct.Name()}
}

func (pe *ParsedExpr) Prepare(args ...any) (*PreparedExpr, error) {
	argTypes, err := typesForStatement(args)
	if err != nil {
		return &PreparedExpr{}, err
	}
	if err := pe.interpret(argTypes); err != nil {
		return nil, err
	}

	outputInfos := make([]OutputInfo, 0)
	// We assume that there is one arg per input/output expression and that they are in the right order
	for _, part := range pe.parts {
		switch part.(type) {
		case *outputPart:
			op := part.(*outputPart)
			outputInfo := generateOutputInfo(op, argTypes[op.Fields[0].Type])
			outputInfos = append(outputInfos, outputInfo)
		}

	}
	return &PreparedExpr{pe, outputInfos, argTypes}, nil
}

// interpret walks the input expression tree to ensure:
// - Each input/output target in expression has type information in argTypes.
// - All type information is actually required by the input/output targets.
// - TODO (manadart 2022-07-15): Add further interpreter behaviour.
func (pe *ParsedExpr) interpret(argTypes typeMap) error {
	var err error
	seen := make(map[string]bool)

	for _, p := range pe.parts {
		switch e := p.(type) {
		case *outputPart, *inputPart:
			if seen, err = pe.validateExpressionType(e.(TypeMappingExpression), argTypes, seen); err != nil {
				return err
			}
		}
	}

	// Now compare the type names that we saw against what we have information
	// for. If unused types were supplied, it is an error condition.
	for name := range argTypes {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("superfluous type")
		}
	}

	return nil
}

// validateExpressionType ensures that the type name identity from the input
// expression is present in the input type information. If it is not, an error
// is returned. The list of seen types is updated and returned.
func (pe *ParsedExpr) validateExpressionType(
	exp TypeMappingExpression, argTypes typeMap, seen map[string]bool,
) (map[string]bool, error) {
	typeName := exp.TypeName()
	if _, ok := argTypes[typeName]; !ok {
		return seen, fmt.Errorf("type info not present (%s)", typeName)
	}

	seen[typeName] = true
	return seen, nil
}

func (pe *ParsedExpr) String() string {
	out := "ParsedExpr["
	for i, p := range pe.parts {
		if i > 0 {
			out = out + " "
		}
		out = out + p.String()
	}
	out = out + "]"
	return out
}

// PreparedExpr represents a prepared expression.
// A prepared expression has reflected type information about the in/out
// arguments used when preparing the statement.
// It also keeps a pointer to the parsed expression.
type PreparedExpr struct {
	Parsed      *ParsedExpr
	OutputSpecs []OutputInfo
	ArgTypes    typeMap
}

type OutputInfo struct {
	OutputColumns  []string
	OutputTypeName string
}

// Complete completes a expression with the values passed as paremeters.
// The goal of Complete is to return a CompletexExpr that can be executed in the
// database. This implies two things:
//
// * Remove output expressions (which are not SQL compliant)
// * Replace input expressions with their value counterparts
//
// For istance:
//
//	type Boss struct {
//		Name string
//	}
//
// Select p.* as &Person.* from person where p.name = $Boss.Name
//
// Parse, Prepare...
// ParsedExpr.Complete(&Boss{"Fred"})
//
// CompletedExpr will have:
//
// Select p.* from person where p.name = ?
func (pe *PreparedExpr) Complete(arguments ...any) (*CompletedExpr, error) {
	var ce CompletedExpr
	ce.arguments = arguments
	ce.outputSpecs = pe.OutputSpecs
	ioparts := 0
	for _, p := range pe.Parsed.parts {
		switch p := p.(type) {
		case *stringPart:
			ce.Add(p.Chunk)
		case *inputPart:
			ioparts++
			str, _ := p.ToSql()
			ce.Add(str)
		case *outputPart:
			ioparts++
			str, _ := p.ToSql(pe)
			ce.Add(str)
		}
	}
	if ioparts != len(arguments) {
		return nil, fmt.Errorf("Parameters mismatch. Expected %d, have %d", ioparts, len(arguments))
	}
	return &ce, nil
}

type CompletedExpr struct {
	sb          strings.Builder
	arguments   []any
	outputSpecs []OutputInfo
	rows        *sql.Rows
}

// add pushes a new piece to the SQL statement that will be ready to be executed
// in the DB
func (ce *CompletedExpr) Add(str string) {
	ce.sb.WriteString(str)
	ce.sb.WriteString(" ")
}

func (ce *CompletedExpr) Sql() string {
	str := ce.sb.String()
	if str[len(str)-1] == ' ' {
		return str[:len(str)-1]
	}
	return str
}

func (ce *CompletedExpr) Exec(db *sql.DB, parts []Part, argTypes typeMap) error {
	// In order to execute the query, we need to pass the proper arguments
	// so they can be bound. Get the input parts and pass them one at a
	// time.
	var bindArgs []any
	var pi int // AF: A little confusing. I guess its the counter for which argument we are currently on
	for _, part := range parts {
		switch part.(type) {
		case *inputPart:
			ip := part.(*inputPart)
			infstruct := argTypes[ip.TypeExpr.Type]
			structfield := infstruct.(sqlairreflect.Struct).Fields[ip.TypeExpr.Field]
			fieldindex := structfield.Index
			val := reflect.ValueOf(ce.arguments[pi])
			val = reflect.Indirect(val)
			if val.Kind() != reflect.Struct {
				return fmt.Errorf("Can't use as parameter something that is not a struct")
			}
			arg := val.Field(fieldindex)
			bindArgs = append(bindArgs, arg.Interface())
			pi++
		case *outputPart:
			pi++
		}
	}
	var err error
	ce.rows, err = db.Query(ce.Sql(), bindArgs...)
	if err != nil {
		return err
	}

	//Just for printing the rows:
	//  var name string
	//  if err := ce.rows.Scan(&name); err != nil {
	//      return err
	//  }
	//fmt.Printf("\nThe rows %+v\n", ce.rows)
	return nil
}

// AF: outputs are the vars to put the outputs INTO
func (ce *CompletedExpr) Scan(parts []Part, argTypes typeMap, outputs ...any) error {

	outputPartIndex := 0

	columns, _ := ce.rows.Columns()
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	colToIndex := map[string]int{}
	for i, colName := range columns {
		valuePtrs[i] = &values[i]
		colToIndex[colName] = i
	}

	ce.rows.Next() // We should really have a Next method for the whole CompleteExpression interface
	ce.rows.Scan(valuePtrs...)
	ce.rows.Close()

	for _, oi := range ce.outputSpecs {
		outputStruct := argTypes[oi.OutputTypeName].(sqlairreflect.Struct)
		s := reflect.ValueOf(outputs[outputPartIndex]).Elem()

		for _, colName := range oi.OutputColumns {
			field, found := outputStruct.Fields[colName]

			if !found {
				return fmt.Errorf("can not found column '%s' of output type %s in results", colName, oi.OutputTypeName)
			}
			val := values[colToIndex[colName]]
			findex := field.Index
			outputField := s.Field(findex)
			valType := reflect.TypeOf(val)
			if !outputField.CanSet() {
				return fmt.Errorf("the field %s of %s is not exported", field.Name, oi.OutputTypeName)
			}
			if valType == outputField.Type() {
				outputField.Set(reflect.ValueOf(val))
			} else {
				return fmt.Errorf("the column %s is type %s but the struct %s has type %s", colName, valType, field.Name, outputField.Type())
			}
		}
		outputPartIndex++
	}

	return nil
}

//// AF: outputs are the vars to put the outputs INTO
//func (ce *CompletedExpr) Scan(parts []Part, argTypes typeMap, outputs ...any) error {
//	c := sqlairreflect.Cache()
//	var outputPartIndex int
//	for _, part := range parts {
//		switch part.(type) {
//		case *outputPart:
//			op := part.(*outputPart) //AF: We need this because 'part' is currently only
//			//really something implementing the interface
//			optypename := op.Fields[0].Type                       // optypename is the name of the Struct to output into (i.e. Person)
//			reflected, err := c.Reflect(outputs[outputPartIndex]) // Get the info about the output types and put in reflected
//			if err != nil {
//				return err
//			}
//			// This checks if the output type given back in Prepare is the same as
//			// the type of the var that has been passed to Scan
//			if reflected.Name() == optypename { // reflected.Name() is the name of the type we want to output into
//				// infstruct is and Info Struct
//				infstruct := argTypes[optypename] // argTypes should be the type of the input expressions
//				// Confiusingly there is a struct called Struct. And even though infsruct is only know to be of type Info
//				// here, WE know that it is in fact a Struct which has a Fields field. How confusing.
//				struc := infstruct.(sqlairreflect.Struct)
//
//				s := reflect.ValueOf(outputs[outputPartIndex]).Elem()
//				columns, _ := ce.rows.Columns()
//				values := make([]interface{}, len(columns))
//				valuePtrs := make([]interface{}, len(columns))
//				colToIndex := map[string]int{}
//				for i, colName := range columns {
//					valuePtrs[i] = &values[i]
//					colToIndex[colName] = i
//				}
//
//				ce.rows.Next() // We should really have a Next method for the whole CompleteExpression interface
//				ce.rows.Scan(valuePtrs...)
//				ce.rows.Close()
//
//				for _, oi := range ce.outputSpecs {
//					outputStruct := argTypes[oi.OutputTypeName].(sqlairreflect.Struct)
//
//					for _, colName := range oi.OutputColumns {
//						field, found := outputStruct.Fields[colName]
//
//						if !found {
//							return fmt.Errorf("can not found column '%s' of output type %s in results", colName, oi.OutputTypeName)
//						}
//						val := values[colToIndex[colName]]
//						findex := field.Index
//						outputField := s.Field(findex)
//						valType := reflect.TypeOf(val)
//						if !outputField.CanSet() {
//							return fmt.Errorf("the field %s of %s is not exported", field.Name, struc.Name())
//						}
//						if valType == outputField.Type() {
//							outputField.Set(reflect.ValueOf(val))
//						} else {
//							return fmt.Errorf("the column %s is type %s but the struct %s has type %s", colName, valType, field.Name, outputField.Type())
//						}
//					}
//				}
//
//				for i, colName := range columns {
//					val := values[i]
//					field, found := struc.Fields[colName]
//					if !found {
//						return fmt.Errorf("can not found tag for '%s' in output variable", colName)
//					}
//					findex := field.Index
//					outputField := s.Field(findex)
//					valType := reflect.TypeOf(val)
//					if !outputField.CanSet() {
//						return fmt.Errorf("the field %s of %s is not exported", field.Name, struc.Name())
//					}
//					if valType == outputField.Type() {
//						outputField.Set(reflect.ValueOf(val))
//					} else {
//						return fmt.Errorf("the column %s is type %s but the struct %s has type %s", colName, valType, field.Name, outputField.Type())
//					}
//				}
//			}
//			outputPartIndex++
//		}
//	}
//	return nil
//}

// typesForStatement returns reflection information for the input arguments.
// The reflected type name of each argument must be unique in the list,
// which means declaring new local types to avoid ambiguity.
//
// Example:
//
//	type Person struct{}
//	type Manager Person
//
//	stmt, err := sqlair.Prepare(`
//	SELECT p.* AS &Person.*,
//		   m.* AS &Manager.*
//	  FROM person AS p
//	  JOIN person AS m
//		ON p.manager_id = m.id
//	 WHERE p.name = 'Fred'`, Person{}, Manager{})
func typesForStatement(args []any) (typeMap, error) {
	c := sqlairreflect.Cache()
	argTypes := make(typeMap)
	for _, arg := range args {
		// reflected is some type of the Info interface, right now it'll only be a Struct struct
		reflected, err := c.Reflect(arg)
		if err != nil {
			return nil, err
		}

		name := reflected.Name() // Name would be Person
		if _, ok := argTypes[name]; ok {
			return nil, fmt.Errorf("type '%s' not unique", name)
		}

		argTypes[name] = reflected
	}

	return argTypes, nil
}

// typeMap is a convenience type alias for reflection
// information indexed by type name.
type typeMap = map[string]sqlairreflect.Info

// Part defines a simple interface for all the different parts that
// make up a ParsedExpr
type Part interface {
	String() string
}

// stringPart represents a portion of the SQL statement that we are not
// interested in.
type stringPart struct {
	Chunk string
}

// qualifiedName represents a name qualified by another name in the form
// qualifier.name
// For instance: p.name or &Person.name
type qualifiedName struct {
	Left  string
	Right string
}

// tableColumn represents a column qualified by a table.
// For instance: person.name
type tableColumn struct {
	Table  string
	Column string
}

// typeField represents a field qualified by a type.
// For instance: Address.postal_code
type typeField struct {
	Type  string
	Field string
}

// TypeMappingExpression describes an expression that
// is for mapping inputs or outputs to Go types.
type TypeMappingExpression interface {
	// TypeName returns the type name used in this expression,
	// such as "Person" in "&Person.*" or "$Person.id".
	TypeName() string
}

// inputPart represents an input expression as specified in the SDL.
// For instance: $Address.postal_code
type inputPart struct {
	TypeExpr typeField
}

func (ip *inputPart) String() string {
	return "inputPart[" + ip.TypeExpr.Type + "." + ip.TypeExpr.Field + "]"
}

func (ip *inputPart) TypeName() string {
	return ip.TypeExpr.Type
}

func (ip *inputPart) ToSql() (string, error) {
	return "?", nil
}

// These names are horrible
// outputPart represents an output expression as specified in the SDL. //AF: DSL
// These are examples of valid output expressions:
//
// &Person
// &Person.*
// &Person.name
// If the outputPart only has the Fields and no Column then q="Select &Person.* From"
type outputPart struct {
	Columns []tableColumn // The bit before the AS, i.e. for p.id: Type = p, Column = id. For * its just Column=*
	Fields  []typeField   // The Go DSL bit (i.e. for &Person.id: Type = Person, Field = id)
}

func (op *outputPart) String() string {
	out := "outputPart[" + op.printColumns() + " " + op.printFields() + "]"
	return out
}

func (op *outputPart) printColumns() string {
	var out string
	for i, c := range op.Columns {
		if i > 0 {
			out = out + " "
		}
		out = out + c.String()
	}
	return out
}

func (op *outputPart) printFields() string {
	var out string
	for i, f := range op.Fields {
		if i > 0 {
			out = out + " "
		}
		out = out + f.String()
	}
	return out
}

func (op *outputPart) TypeName() string {
	// FIXME: do we need multiple fields?
	return op.Fields[0].Type
}

func (op *outputPart) ToSql(pe *PreparedExpr) (string, error) {
	// The &Type.Field syntax is part of the DSL but not SQL so we can not
	// print that. We do need to print the columns though (if any)
	// There are two cases here
	var out string
	sf := pe.ArgTypes[op.TypeName()].(sqlairreflect.Struct)
	if len(op.Columns) != 0 {
		// Case 1
		// foo as &Type.Field --> print foo
		for i, c := range op.Columns {
			if i > 0 {
				out = out + ","
			} else if c.Table != "" {
				out = out + c.Table + "." + c.Column
			} else {
				out = out + c.Column
			}
		}
		return out, nil
	}

	// Case 2: No AS just the Go Struct
	// &Type.colum --> expand to the name of the column with `db` tag.
	if op.Fields[0].Field != "*" && op.Fields[0].Field != "" {
		if dbName, found := sf.Tags[op.Fields[0].Field]; found {
			return dbName, nil
		} else {
			return "", fmt.Errorf("%s not found", dbName) // Look for the tag
		}
	}
	// if the column is '*' or there is no column (as in &Person) expand to
	// all the columns with a `db` tag. Ignore the rest.
	// The iteration order of the hash is not specified. We need
	// to use the same order to be able to write tests that do not fail
	// randomly.

	return out, nil
}

func (tf *typeField) String() string {
	return "typeField[" + tf.Type + "." + tf.Field + "]"
}

func (tc *tableColumn) String() string {
	return "tableColumn[" + tc.Table + "." + tc.Column + "]"
}

func (sp *stringPart) String() string {
	return "stringPart[" + sp.Chunk + "]"
}

func (sp *stringPart) ToSql() string {
	return sp.Chunk
}

var errNoLiteral = fmt.Errorf("expected a literal string")

func (p *Parser) init(str string) {
	p.parsed = 0
	p.skipped = 0
	p.str = str
	p.parts = nil
}

// addTail adds the remaining part of the SQL statement to be processed
func (p *Parser) addTail() {
	cp := p.save()
	p.add(cp, nil)
}

func (p *Parser) Parse(str string) (*ParsedExpr, error) {
	p.init(str)
	if p.str == "" {
		return nil, fmt.Errorf("empty statement")
	}
	// FIXME:
	// This logic seems weird as it gives the impression that
	// this checks fail if they don't parse the thing they are supposed
	// to parse but that is not the case. If any of these functions return
	// an error we should report it and exit.
	for p.skipped < len(p.str) {
		if err := p.parseInputExpression(); err != nil {
			return nil, err
		}
		if err := p.parseOutputExpression(); err != nil {
			return nil, err
		}
		p.parseColumnGroup()
		if err := p.parseStringLiteral(); err != nil {
			return nil, err
		}
		p.advance()
	}
	p.addTail()
	return &ParsedExpr{parts: p.parts}, nil
}

func createDb() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("Create table citizens (citizen_name varchar, citizen_age int, citizen_income int);")
	if err != nil {
		return nil, fmt.Errorf("error creating table: %v", err)
	}
	inserts := []string{"INSERT INTO citizens VALUES ('Fred', 30, 1000);",
		"INSERT INTO citizens VALUES ('Mark', 20, 1500);",
		"INSERT INTO citizens VALUES ('Mary', 25, 3500);",
		"INSERT INTO citizens VALUES ('James', 25, 3500);"}
	for _, q := range inserts {
		_, err := db.Exec(q)
		if err != nil {
			return nil, fmt.Errorf("error inserting data: %v", err)
		}
	}

	_, err = db.Exec("commit;")
	return db, nil
}

func main() {
	p := NewParser()
	type Address struct {
		Dummy int64 `db:"foo"`
		Code  int64 `db:"postal_code"`
	}

	type Person struct {
		Name   string `db:"citizen_name"`
		Age    int64  `db:"citizen_age"`
		Income int64  `db:"citizen_income"`
	}

	var manager Person
	var otherguy Person
	//q := "select p.* as &Person.* from citizens AS p"
	//q := "select * as &Person.* from citizens"
	q := "select * as &Person.*, citizen_name as &Person.* from citizens"
	//q := "select citizen_income AS &Person from citizens where citizen_income = $Person.citizen_income"
	// q := "select citizen_income, citizen_name AS &Person from citizens where citizen_income = $Person.citizen_income"
	fmt.Printf("Input query: %s\n", q)
	if parsedexp, err := p.Parse(q); err == nil {
		if preparedexp, err := parsedexp.Prepare(&Person{}); err == nil {
			if completedexpr, err := preparedexp.Complete(&Person{}, &Person{}); err == nil {
				//if completedexpr, err := preparedexp.Complete(&Person{}, &Person{Income: 3500}); err == nil {
				fmt.Printf("Parsed AST: %+v\n", parsedexp)
				fmt.Printf("Prepared query: %s\n", completedexpr.Sql())
				db, err := createDb()
				if err != nil {
					fmt.Println(err)
					return
				}
				if err := completedexpr.Exec(db, parsedexp.parts, preparedexp.ArgTypes); err != nil {
					fmt.Println(err)
					return
				}
				if err := completedexpr.Scan(parsedexp.parts, preparedexp.ArgTypes, &manager, &otherguy); err != nil {
					fmt.Println(err)
					return
				}
				fmt.Printf("Result: manager - %+v, other guy - %+v", manager, otherguy)
			} else {
				fmt.Printf("error completing query: %s", err)
			}
		} else {
			fmt.Printf("error preparing query: %s", err)
		}
	} else {
		fmt.Printf("error parsing query: %s", err)
		return
	}
	fmt.Printf("\n\n")
}
