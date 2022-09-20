package main

import (
	"fmt"
	"internal/parts"
)

func main() {
	op, err := parts.NewOutputPart("table", "col", "Person", "person_name")
	if err != nil {
		fmt.Print(err)
		return
	}
	ip, err := parts.NewInputPart("Address", "postal_code")
	if err != nil {
		fmt.Print(err)
		return
	}
	fmt.Printf("%s.%s\n", op.Columns.TableName(), op.Columns.ColumnName())
	fmt.Printf("%s.%s\n", op.Gotype.TypeName(), op.Gotype.TagName())
	fmt.Printf("%s.%s\n", ip.TypeName(), ip.TagName())
}
