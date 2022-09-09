# sqlairexperiment
Exercise on how to parse, prepare, execute and Scan values with sqlair

This is just an experiment to explore some possibilities for parsing SQLair's DSL and doing the full pipeline of a query from parsing, to execution and retrieving the values from the resultset.

It is far from complete. The code doesn't adhere to any standards whatsoever as this was just a PoC.
To run the tests, execute:

```
go test
```

To see a full example, run:

```
go run main.go
```

## How does it work?
The main idea behind this experiment is to parse a DSL statement being agnostic
of the specific SQL dialect being used by the final user. Different databases
implement SQL differently. For instance, to retrieve a maximum of N rows from
the database, many options are available:

```
select .... limit N;

select top N ...

select ... fetch first N rows first;
```

Our parser should only care about query arguments (inputs) and query output
mappings (outputs). The rest should be passed to the backend database verbatim.

Three types of nodes are defined for our AST-like structure:

* stringPart: represents things we want to pass verbatim
* inputPart: represents query arguments
* outputPart: represents query output mappings


## Parsing
The parser main loop (`Parse()`) inspects the input and tries to parse the
different elements of the DSL and emit any of the three AST-nodes mentioned
before. The parser is built around the checkpoint idea. Every function will try
to detect a specific part of the DSL (inputs, outputs, column groups, etc.).
Before they start peeking characters and moving around they create a checkpoint.
If the function is able to parse a certain element (e.g: an input expression)
the proper node will be created. If not, the parser state is restored with the
information kept in the checkpoint.

## Preparing the query
Preparing the query takes one instance for each one of the types used in the
query and generates reflection information so we can refer to it later.
If more types than needed are passed to the `Prepare()` function and error is
generated. The same occurs if too few types are passed.

## Completing the query
Once the query has been checked for syntax and type correctness, the user needs
to provide the actual values that will be used as query arguments. The
`Complete` function will match the values of the `inputPart` with the values of
the corresponding fields of the structs passed as parameters.

## Executing the query
The result of the `Complete` function should be a statement that is SQL
compliant with the dialect of the backend database. Since we only modify the
original statement for the DSL specific parts, the rest of the query should just
pass through as a `stringPart`. Once we have executed the query, we obtain a
resultset (`*sql.Row`)

## Scanning the results
The SQLair convenience layer goals are two fold:
 * Using data from golang defined structs as inputs
 * Populating golang defined structs with data obtained from the database
 The scanning part deals with the second issue. Once we have the results we scan
 the `outputParts` of the query and using the reflection information we already
 have in the cache, we populate the corresponding fields in the structure.

## Metadata tags
 The database statement deals with database columns only and knows nothing about
 the structure of the golang defined types. To help creating the relationship
 between struct fields and database columns, we use tags in the struct fields:

```
type Address struct {
    ID       int    `db:"id"`
    Lot      string `db:"lot"`
    Street   string `db:"street"`
    District string `db:"district"`
    Code     string `db:"code"`
}
```

This way, the value of the resultset corresponding to the column `code` will be used to fill the field `Code` in a variable of type `Address`. Note that the fields must be exported (first letter should be capitalized).
