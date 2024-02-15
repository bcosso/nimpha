package main

import (
	"sql_parser"
	"fmt"
	"reflect"
	"strings"
	"strconv"
)

type Filter struct {
	Gate    	 string
	CommandLeft  interface{}
	Operation	 string
	CommandRight interface{}
	TableObject	 []SqlClause
	ChildFilters []Filter
	SelectClause []SqlClause
}


type SqlClause struct{
	Alias string
	Name string
	IsSubquery bool
	SelectableObject interface{}
} 

// func Exec(query string){
// 	// str1 := `insert into table1 (field1, field2) values (1, '2') `
// 	// str1 := `select  table1.campo1, table2.campo2 from table1, table2 where t1 = 'TEST STRING' and table1.productid = table2.productid `

// 	var action sql_parser.ActionExec = ParsingActionExec{}
// 	sql_parser.SetAction(action)
// 	sql_parser.Execute_parsing_process(query)
// }

func execute_query(payload interface{}) interface{}{
	
	payload_content, ok :=  payload.(map[string] interface{})
	if !ok{
		fmt.Println("ERROR!")	
	}

	query := payload_content["query"].(string)
	var action sql_parser.ActionExec = ParsingActionExec{}
	sql_parser.SetAction(action)
	tree := sql_parser.Execute_parsing_process(query)
	filterNew := new(Filter)
	read_through(tree, "", filterNew)


	fmt.Println("-----------------------------------------------------------")
	fmt.Println("Filter!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	fmt.Println("-----------------------------------------------------------")
	fmt.Println(filterNew)

	result := select_data_where_worker_contains_rsocket_sql(*filterNew)

	return result
}

func (internalExec ParsingActionExec) ExecActionFinal(tree sql_parser.CommandTree) {
	fmt.Println("-----------------------------------------------------------")
	fmt.Println("CorrespondingFinalActionParsing!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	fmt.Println("-----------------------------------------------------------")
	fmt.Println(tree)

	// filterNew := new(Filter)
	// read_through(tree, "", filterNew)


	// fmt.Println("-----------------------------------------------------------")
	// fmt.Println("Filter!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	// fmt.Println("-----------------------------------------------------------")
	// fmt.Println(filterNew)

	// result := select_data_where_worker_contains_rsocket_sql(*filterNew)

	// fmt.Println("-----------------------------------------------------------")
	// fmt.Println("QUERY RESULT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	// fmt.Println("-----------------------------------------------------------")
	// fmt.Println(result)

}

func return_type(nameType string) reflect.Type{
	if nameType == "string"{
		var str string
		return reflect.TypeOf(str)
	}
	return nil
}

func parseCommandType (clause sql_parser.CommandTree) interface{} {
	typeToken := strings.ToLower(clause.TypeToken)
	switch typeToken{
	case "string":
		return strings.Replace(clause.Clause, "'", "", -1)
		break
	case "field_filter":
		return clause
	case "field_select_to_show":
		return clause
	case "field":
		return clause
	case "int":
		ret, _ := strconv.Atoi(clause.Clause)
		return ret
	case "int64":
		ret, _ := strconv.Atoi(clause.Clause)
		return ret
	case "number":
		ret, _ := strconv.Atoi(clause.Clause)
		return ret
	case "float64":
		ret, _ := strconv.ParseFloat(clause.Clause, 64)
		return ret
	default:
		break
	}
	
	
	return ""
}

var _filter Filter

func read_through(tree sql_parser.CommandTree, expected_context string, currentFilter * Filter){

	var read_later *sql_parser.CommandTree
	expected_context_next := ""
	indexCommand := 0
	// operatorLast := ""
	var lastFilter * Filter
	for indexCommand < len(tree.CommandParts){
		command := tree.CommandParts[indexCommand]
		typeToken := strings.ToLower(tree.CommandParts[indexCommand].TypeToken)
		if 
		// (typeToken == "field_select_to_show" ) ||
		(tree.TypeToken == "where_fields") &&
		((typeToken == "field_filter" ) || 
		(typeToken == "number" ) || 
		(typeToken == "string" ) || 
		(typeToken == "int" ) || 
		(typeToken == "int64" ) || 
		(typeToken == "float64" ) || 
		(typeToken == "field" )){
		// (typeToken == "TABLE_FROM" )  ||
		// (typeToken == "FIELD_FILTER" )  ||

		
			filterNew := new(Filter)
			if len(tree.CommandParts[indexCommand].CommandParts) > 0{
				filterNewChild := new(Filter)
				read_through(tree.CommandParts[indexCommand], "", filterNewChild)
				filterNew.CommandLeft = filterNewChild
			}else{
				filterNew.CommandLeft = parseCommandType(tree.CommandParts[indexCommand])
				
			}

			if (indexCommand < len(tree.CommandParts) -1) {
				
				if (strings.ToLower(tree.CommandParts[indexCommand + 1].TypeToken) == "operator"){
					indexCommand ++
					filterNew.Operation = strings.ToLower(tree.CommandParts[indexCommand].ClauseName)
					indexCommand ++
					if len(tree.CommandParts[indexCommand].CommandParts) > 0{
						filterNewChild := new(Filter)
						read_through(tree.CommandParts[indexCommand], "", filterNewChild)
						filterNew.CommandRight = filterNewChild
					}else{
						// filterNew.CommandRight = tree.CommandParts[indexCommand].Clause
						filterNew.CommandRight = parseCommandType(tree.CommandParts[indexCommand])
					}
				}
			}

			
			currentFilter.ChildFilters = append(currentFilter.ChildFilters, *filterNew)
			lastFilter = &currentFilter.ChildFilters[len(currentFilter.ChildFilters) -1]
			//check if has tree.CommandParts[indexCommand] children and validate types creation
			


		}else if (typeToken == "table_from" ) || (typeToken == "table_from_command" ){
			// filterNew := new(Filter)
			
			// filterNew.Gate = "OR"
			// column := new(sql_parser.CommandTree)
			// column.Clause = "table_name"
			// filterNew.CommandLeft = column
			// filterNew.Operation = "EQUALS"
			// filterNew.CommandRight = tree.CommandParts[indexCommand].Clause

			// tableObject := SqlClause{Name: tree.CommandParts[indexCommand].Clause}

			// filterNew.TableObject = append(filterNew.TableObject, tableObject)
			
			// currentFilter.ChildFilters = append(currentFilter.ChildFilters, *filterNew)
			// currentFilter.Gate = "AND"


		}else if (typeToken == "operator"){

			if strings.ToLower(command.ClauseName) == "and" || strings.ToLower(command.ClauseName) == "or" {
				lastFilter.Gate = command.ClauseName
			// }else{ 
			// 	lastFilter.Operation = tree.CommandParts[indexCommand].ClauseName
			}
			// operatorLast = command.ClauseName
	
		}else if (typeToken == "string") ||
		(typeToken == "field_select_to_show") || 
		(typeToken == "number") || 
		(typeToken == "float64") ||
		(typeToken == "int64"){
			var objSelect SqlClause
			objSelect.SelectableObject = parseCommandType(tree.CommandParts[indexCommand])
			currentFilter.SelectClause = append(currentFilter.SelectClause, objSelect)

		}else if strings.Index(typeToken, "command") > -1 || (typeToken == "where_fields") || (typeToken == "tables_from") || (typeToken == "fields_select") || (typeToken == "fields"){
			filterNew := new(Filter)
			switch strings.ToLower(command.ClauseName) {
			case "select":
				
				if (read_later == nil) && (strings.ToLower(command.ClauseName) != "select") {
					*read_later = command
				}else{
					expected_context_next = "select"
					read_through(command, expected_context_next, filterNew)

				}
				break

			case "from":
				expected_context = "from"
				//identify here if it has real table or subquery. If it does habe subquery, I also add this filter to TableObject with the alias
				CheckNodeForTables(command, currentFilter, filterNew)

				// read_through(command, expected_context, filterNew)
				// if !IsNotSubquery {
				// 	newTableObject := SqlClause{Alias:alias, IsSubquery:!IsNotSubquery, SelectableObject: filterNew}
				// 	currentFilter.TableObject = append(currentFilter.TableObject, newTableObject)
				// }else{
				// 	newTableObject := SqlClause{Name:alias,IsSubquery:!IsNotSubquery, SelectableObject: filterNew}
				// 	currentFilter.TableObject = append(currentFilter.TableObject, newTableObject)
				// }
				break
			case "where":
				expected_context = "where"
				read_through(command, expected_context, filterNew)
				break
			case "fields":
				expected_context = "where"
				read_through(command, expected_context, filterNew)
				break
			default:
				if strings.ToLower(command.ClauseName) == "select"{
					expected_context_next = "select"

				}
				break
			}
			currentFilter.ChildFilters = append(currentFilter.ChildFilters, *filterNew)

		}else{
			if len(tree.CommandParts[indexCommand].CommandParts) > 0{
				// filterNewChild := new(Filter)
				read_through(tree.CommandParts[indexCommand], "", currentFilter)
				// filterNew.CommandRight = filterNewChild
			}
		}
		indexCommand ++
	}
	if read_later != nil {
		expected_context = "select"
		read_through(*read_later, expected_context, currentFilter)
	}
	

}

// func eval_select_command()


func (internalExec ParsingActionExec) ExecAction(tree * sql_parser.CommandTree) {
	// fmt.Println("-----------------------------------------------------------")
	// fmt.Println("CorrespondingActionParsing!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	// fmt.Println("-----------------------------------------------------------")
	// fmt.Println(tree)
}

type ParsingActionExec struct {
	sql_parser.ActionExec
}

func IsInteger(val float64) bool {
    return val == float64(int(val))
}

func IsEqualsTo(value1 interface{}, value2 interface{}) bool{
	if (value1 == value2) {
		return true
	}
	return false
}


func IsBiggerThan(value1 float64, value2 float64) bool{
	if (value1 > value2) {
		return true
	}
	return false
}

func AndCompare(arg1 bool, arg2 bool) bool{
	return arg1 && arg2
}

func OrCompare(arg1 bool, arg2 bool) bool{
	return arg1 || arg2
}
func CheckNodeForTables(tree sql_parser.CommandTree, currentFilter * Filter, filterNew * Filter ) {
	alias := ""
	for  _, branch := range tree.CommandParts{
		IsNotSubquery := false
		if strings.ToLower(branch.TypeToken) == "table_from_command" {
			IsNotSubquery = true
			fmt.Println(alias)
		}
		if branch.Alias != ""{alias = branch.Alias} else { alias = branch.Clause }

		read_through(tree, "from", filterNew)
		if !IsNotSubquery {
			newTableObject := SqlClause{Alias:alias, IsSubquery:!IsNotSubquery, SelectableObject: filterNew}
			currentFilter.TableObject = append(currentFilter.TableObject, newTableObject)
		}else{
			newTableObject := SqlClause{Name:alias,IsSubquery:!IsNotSubquery, SelectableObject: filterNew}
			currentFilter.TableObject = append(currentFilter.TableObject, newTableObject)
		}
		// if result != true{
		// 	if branch.Alias != ""{alias = branch.Alias} else { alias = branch.Clause }
		// 	fmt.Println(alias)
		// }
	}
	// return result, alias
}

