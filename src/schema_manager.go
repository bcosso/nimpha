package main

import (
	"fmt"

	"github.com/bcosso/sqlparserproject"
)

const _schemaTable = "__sys_table"
const _columnTable = "table_name"

func (sing *SingletonTable) InitializeSchema() {
	//Check if the table exists. If it does not, create it.

	//Add Hash Index to __sys_table on the column table_name
	var index TableIndex
	index.ColumnName = _columnTable
	index.TableName = _schemaTable
	index.IndexType = "HASH"

	singletonIndex.AttachNewHashIndex(index)

}

type Schema struct {
	TableName        string
	ColumnDefinition map[string]interface{}
	Identity         bool
	ColumnIdentity   string
}

func (sing *SingletonTable) CreateSchema(schema Schema) {
	//creates a new entry in the table __sys_table if it doesn't exist (search index)
	var rowSchema mem_row
	fmt.Println("CreateSchemaMethod")
	if _, hasIndex := singletonIndex.hashIndex[_schemaTable]; hasIndex {
		fmt.Println("Found SchemaTable")
		if _, hasIndex = singletonIndex.hashIndex[_schemaTable][_columnTable][schema.TableName]; hasIndex {
			rowSchema = *(singletonIndex.hashIndex[_schemaTable][_columnTable][schema.TableName])
		} else {
			var tableContent []*mem_row
			sing.mt[_schemaTable] = tableContent
			rowSchema.Table_name = _schemaTable
			rowSchema.Parsed_Document = make(map[string]interface{})
			rowSchema.Parsed_Document[_columnTable] = schema.TableName
			fmt.Println("Populating rowSchema")
			if schema.ColumnIdentity != "" {
				var identity int
				identity = 1
				rowSchema.Parsed_Document["_identity_value"] = identity
				rowSchema.Parsed_Document["_identity_column"] = schema.ColumnIdentity
			}
			if schema.ColumnDefinition != nil {

				var columnDefinition map[string]interface{}
				for k, v := range schema.ColumnDefinition {
					columnDefinition[k] = v
				}
				rowSchema.Parsed_Document["_column_definition"] = columnDefinition

			}
			fmt.Println(rowSchema)
			//rowSchema.Parsed_Document = schema
			sing.mt[_schemaTable] = append(sing.mt[_schemaTable], &rowSchema)
			singletonIndex.AttachNewHashIndexUnity(_schemaTable, _columnTable, schema.TableName, &rowSchema)
		}
	} else {
		fmt.Println("Not found in Schema Table")
		var tableContent []*mem_row
		sing.mt[_schemaTable] = tableContent
		rowSchema.Table_name = _schemaTable
		rowSchema.Parsed_Document[_columnTable] = schema.TableName
		fmt.Println("Populating rowSchema")
		if schema.ColumnIdentity != "" {
			rowSchema.Parsed_Document["_identity_value"] = 0
			rowSchema.Parsed_Document["_identity_column"] = schema.ColumnIdentity
		}
		fmt.Println(rowSchema)
		//rowSchema.Parsed_Document = schema
		sing.mt[_schemaTable] = append(sing.mt[_schemaTable], &rowSchema)
		singletonIndex.AttachNewHashIndexUnity(_schemaTable, _columnTable, schema.TableName, &rowSchema)

	}

	//columns : table_name (string), _identity_value(int), _identity_column (string),
	//	    table_columns map[string]interface
	//Plan how this would work distributed

}

func (sing *SingletonTable) CheckForSchema(table string, row *mem_row) error {
	var schemaRow mem_row
	_, hasIndex := sing.mt[_schemaTable]
	if hasIndex {
		//Check for Identities
		if _, hasIndex = singletonIndex.hashIndex[_schemaTable][_columnTable][table]; hasIndex {
			schemaRow = *(singletonIndex.hashIndex[_schemaTable][_columnTable][table])

			if identity, hasIdentity := schemaRow.Parsed_Document["_identity_value"]; hasIdentity {
				//strId := identity.(json.Number).String()
				//newId, _ := strconv.Atoi(strId)
				newId := identity.(int)
				identityColumn, _ := schemaRow.Parsed_Document["_identity_column"]
				row.Parsed_Document[identityColumn.(string)] = newId
				newId++
				schemaRow.Parsed_Document["_identity_value"] = newId
			}

			if _, hasColumnSchema := schemaRow.Parsed_Document["_column_definition"]; hasColumnSchema {
				//strId := identity.(json.Number).String()
				//newId, _ := strconv.Atoi(strId)
				rowDereferenced := *row
				if rowDereferenced.Parsed_Document["_column_definition"] != nil {
					columnDefinition := rowDereferenced.Parsed_Document["_column_definition"].(map[string]interface{})
					for k, _ := range columnDefinition {
						//Check if the insert has a column that is not in the defined schema
						if _, hasColumn := columnDefinition[k]; !hasColumn {
							//return error
						}
					}
				}
			}
		}
	}

	return nil
}

func executeSchemaCommand(payload interface{}) interface{} {
	payload_content, ok := payload.(map[string]interface{})
	if !ok {
		fmt.Println("ERROR!")
	}
	fmt.Println("Method reached")
	//Will get a new Parser method for DDL-Like commands
	//query := payload_content["query"].(string)
	//tree := sqlparserproject.ExecuteParsingProcess(query)
	//filterNew2 := new(Filter)

	query := payload_content["query"].(string)
	tree := sqlparserproject.ExecuteParsingProcess(query)
	var schema Schema
	evaluateDDLTree(tree, &schema)

	//schema.TableName = query["table_name"].(string)

	//if _, hasIdentity := query["column_identity"]; hasIdentity {
	//	schema.ColumnIdentity = query["column_identity"].(string)
	//}

	//if _, hasDefinition := query["column_definition"]; hasDefinition {
	//	schema.ColumnDefinition = query["column_definition"].(map[string]interface{})
	//}

	fmt.Println(schema)
	//singletonTable.CreateSchema(schema)

	return "Ok"
}

func evaluateDDLTree(tree sqlparserproject.CommandTree, schema *Schema) {
	fmt.Println("88888888888888888888888888888888")
	fmt.Println(tree)
	switch tree.TypeToken {
	case "create":
	case "table":
	case "column":
	default:
		fmt.Println("")

	}

}
