package main

//import "fmt"

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
	TableName      string
	Column         map[string]interface{}
	Identity       bool
	ColumnIdentity string
}

func (sing *SingletonTable) CreateSchema(schema Schema) {
	//creates a new entry in the table __sys_table if it doesn't exist (search index)
	var rowSchema mem_row
	if _, hasIndex := singletonIndex.hashIndex[_schemaTable]; hasIndex {
		if _, hasIndex = singletonIndex.hashIndex[_schemaTable][_columnTable][schema.TableName]; hasIndex {
			rowSchema = *(singletonIndex.hashIndex[_schemaTable][_columnTable][schema.TableName])
		}
	} else {
		rowSchema.Table_name = _schemaTable
		if schema.ColumnIdentity != "" {
			rowSchema.Parsed_Document["_identity_value"] = 0
			rowSchema.Parsed_Document["_identity_column"] = schema.ColumnIdentity
		}
		//rowSchema.Parsed_Document = schema
		sing.mt[_schemaTable] = append(sing.mt[_schemaTable], &rowSchema)
		singletonIndex.AttachNewHashIndexUnity(_schemaTable, _columnTable, schema.ColumnIdentity, &rowSchema)

	}

	//columns : table_name (string), _identity_value(int), _identity_column (string),
	//	    table_columns map[string]interface

	//Plan how this would work distributed

}

func (sing *SingletonTable) CheckForSchema(table string, row *mem_row) {
	var schemaRow mem_row
	_, hasIndex := sing.mt[_schemaTable]
	if hasIndex {
		//Check for Identities
		if _, hasIndex = singletonIndex.hashIndex[_schemaTable][_columnTable][table]; hasIndex {
			schemaRow = *(singletonIndex.hashIndex[_schemaTable][_columnTable][table])

			if identity, hasIdentity := schemaRow.Parsed_Document["_identity_value"]; hasIdentity {
				newId := identity.(int)
				newId++
				identityColumn, _ := schemaRow.Parsed_Document["_identity_column"]
				row.Parsed_Document[identityColumn.(string)] = newId
			}
		}
		//Check for columns needed
		//
	}
}
