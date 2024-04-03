package main

import (
	"github.com/bcosso/sqlparserproject"
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
	AlreadyConsumed bool //This flag is meant for joins, so they can me executed only once
}

type Condition struct{
	ConditionIf Filter
	ConditionThen interface{}
	ConditionElse interface{}
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

// 	var action sqlparserproject.ActionExec = ParsingActionExec{}
// 	sqlparserproject.SetAction(action)
// 	sqlparserproject.Execute_parsing_process(query)
// }

func execute_query(payload interface{}) interface{}{
	
	payload_content, ok :=  payload.(map[string] interface{})
	if !ok{
		fmt.Println("ERROR!")	
	}

	query := payload_content["query"].(string)
	var action sqlparserproject.ActionExec = ParsingActionExec{}
	sqlparserproject.SetAction(action)
	tree := sqlparserproject.Execute_parsing_process(query)
	filterNew := new(Filter)
	read_through(tree, "", filterNew)


	fmt.Println("-----------------------------------------------------------")
	fmt.Println("Filter!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	fmt.Println("-----------------------------------------------------------")
	fmt.Println(filterNew)

	result := select_data_where_worker_contains_rsocket_sql(*filterNew)

	return result
}

func (internalExec ParsingActionExec) ExecActionFinal(tree sqlparserproject.CommandTree) {
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

func parseCommandType (clause sqlparserproject.CommandTree) interface{} {
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


func parseCondition(tree sqlparserproject.CommandTree, expected_context string, indexCommand * int) SqlClause {
	var condition Condition
	var clause SqlClause
	fmt.Println(tree)
	for *indexCommand < len(tree.CommandParts){
		typeToken := strings.ToLower(tree.CommandParts[*indexCommand].TypeToken)
		(*indexCommand) ++
		// fmt.Println("parseCondition")
		switch typeToken{
		
			case "condition_when":
				var conditionIf Filter
				read_through(tree.CommandParts[(*indexCommand) -1 ], "where", &conditionIf)
				condition.ConditionIf = conditionIf
				break
			case "condition_then":
				var conditionThen Filter
				read_through(tree.CommandParts[(*indexCommand) -1 ], "where", &conditionThen)
				if conditionThen.CommandLeft == nil && len(conditionThen.SelectClause)  > 0{
					condition.ConditionThen = conditionThen.SelectClause[0]
				}
				condition.ConditionElse = parseCondition(tree, expected_context, indexCommand)
				break
			case "condition_else":
				var conditionElse Filter
				read_through(tree.CommandParts[(*indexCommand) -1 ], "where", &conditionElse)
				if conditionElse.CommandLeft == nil && len(conditionElse.SelectClause)  > 0{
					condition.ConditionElse = conditionElse.SelectClause[0]
				}
				break

			default:
				break
		}
		
	}
	
	clause.SelectableObject = condition
	fmt.Println("Clause")
	fmt.Println(clause)

	return clause
}


func read_through(tree sqlparserproject.CommandTree, expected_context string, currentFilter * Filter){

	var read_later *sqlparserproject.CommandTree
	expected_context_next := ""
	indexCommand := 0
	// operatorLast := ""
	var lastFilter * Filter
	for indexCommand < len(tree.CommandParts){
		command := tree.CommandParts[indexCommand]
		typeToken := strings.ToLower(tree.CommandParts[indexCommand].TypeToken)
		if 
		// (typeToken == "field_select_to_show" ) ||
		(tree.TypeToken == "where_fields" || tree.TypeToken == "CONDITION_WHEN") &&
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
			// column := new(sqlparserproject.CommandTree)
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
		(typeToken == "field") ||
		(typeToken == "int64"){
			var objSelect SqlClause
			objSelect.SelectableObject = parseCommandType(tree.CommandParts[indexCommand])
			if (tree.CommandParts[indexCommand].Alias != "") { objSelect.Alias = tree.CommandParts[indexCommand].Alias }
			(*currentFilter).SelectClause = append(currentFilter.SelectClause, objSelect)

		}else if (typeToken == "condition_case"){
			var objSelect SqlClause
			indexConditionValue := 0
			indexCondition := &indexConditionValue

			objSelect = parseCondition(tree.CommandParts[indexCommand], expected_context, indexCondition)
			if (tree.Alias != "") { objSelect.Alias = tree.Alias }

			currentFilter.SelectClause = append(currentFilter.SelectClause, objSelect)
		}else if strings.Index(typeToken, "command") > -1 || (typeToken == "where_fields") || (typeToken == "tables_from") || (typeToken == "fields_select") || (typeToken == "fields") || (typeToken == "condition"){
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
				break
			case "where":
				expected_context = "where"
				read_through(command, expected_context, filterNew)
				break
			case "fields":
				expected_context = "where"
				read_through(command, expected_context, filterNew)
				break
			case "condition":
				expected_context = "where"
				read_through(command, expected_context, currentFilter)
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
				read_through(tree.CommandParts[indexCommand], "", currentFilter)
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


func (internalExec ParsingActionExec) ExecAction(tree * sqlparserproject.CommandTree) {
	// fmt.Println("-----------------------------------------------------------")
	// fmt.Println("CorrespondingActionParsing!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	// fmt.Println("-----------------------------------------------------------")
	// fmt.Println(tree)
}

type ParsingActionExec struct {
	sqlparserproject.ActionExec
}

func IsInteger(val float64) bool {
    return val == float64(int(val))
}

func AndCompare(arg1 bool, arg2 bool) bool{
	return arg1 && arg2
}

func OrCompare(arg1 bool, arg2 bool) bool{
	return arg1 || arg2
}
func CheckNodeForTables(tree sqlparserproject.CommandTree, currentFilter * Filter, filterNew * Filter ) {
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
			// if branch.
			newTableObject := SqlClause{Name: branch.Clause, Alias:alias, IsSubquery:!IsNotSubquery, SelectableObject: filterNew}
			currentFilter.TableObject = append(currentFilter.TableObject, newTableObject)
		}
	}
}

func GetConditionFlow(row mem_table_queries, column SqlClause, ctx * map[string] interface {} ) (SqlClause){
	condition := column.SelectableObject.(Condition)
	filterIf := condition.ConditionIf

	if applyLogic2(row, &filterIf, ctx){
		clauseThen := condition.ConditionThen.(SqlClause)
		column = GetFlowAfterLogic(row, clauseThen, ctx)
	}else{
		clauseElse := condition.ConditionElse.(SqlClause)
		column = GetFlowAfterLogic(row, clauseElse, ctx)	
	}


	return column 
}

func GetFlowAfterLogic( row mem_table_queries, column SqlClause, ctx * map[string] interface {} ) SqlClause{
	var condition Condition
	var clauseValidation sqlparserproject.CommandTree
	if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(column){
		column = ProjectColumns(row, column, ctx)
	}else if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(condition){
		column = GetConditionFlow(row, column, ctx)
	}else if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(clauseValidation){
		column = ProjectColumns(row, column, ctx)
	}else{
		column = ProjectColumns(row, column, ctx)
		// filterThen := column.SelectableObject.(Filter)
		// selectFieldsDecoupled2(filterThen, filterThen, 0, column.Alias, ctx)
		// selectFieldsDecoupled2(logic_filters Filter, fullLogicFilters Filter, indexFilter int, aliasSubquery string, ctx * map[string] interface{}) // var tableResult []mem_table_queries
		//Need to check how I'm going to return a subquery from here
	}
	return column
}

