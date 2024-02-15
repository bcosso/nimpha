package main

import (
	"bytes"
    "fmt"
	"log"
	//"errors"	
	//"io"
	"encoding/json"
	"strconv"
	"net/http"
	// "github.com/google/uuid"
	"github.com/gorilla/mux"
	"strings"
	"sql_parser"
	"reflect"
	"rsocket_json_requests"
)

type mem_table_queries struct {
	TableName   string
	QueryId		int
	Rows 		interface{}
}
var _currentQueryId int = 0
var _query_temp_tables []mem_table_queries

func get_all(w http.ResponseWriter, r *http.Request){
    fmt.Fprintf(w, "retrieve all data from table")
    fmt.Println("Endpoint Hit: get_all")
}

func get_rows(w http.ResponseWriter, r *http.Request){
    fmt.Fprintf(w, "retrieve rows from table")
	fmt.Println("Endpoint Hit: get_rows")
	
	vars := mux.Vars(r)
	key := vars["id"]
	operation := vars["op"]
	table_from := vars["table_from"]
	i, err := strconv.Atoi(key)
	if err != nil {
		log.Fatal(err)
	}

	get_rows_op(i, operation, table_from)
	
}


func get_range(w http.ResponseWriter, r *http.Request){
	fromStr := r.URL.Query().Get("from")

	fmt.Println(" FROM: " + fromStr)

	toStr := r.URL.Query().Get("to")
	fmt.Println(" TO: " + toStr)
	table_from := r.URL.Query().Get("table_from")

	from, err := strconv.Atoi(fromStr)
	if err != nil {
		log.Fatal(err)
	}
	to, err := strconv.Atoi(toStr)
	if err != nil {
		log.Fatal(err)
	}
	
	var result []mem_row
	for _, node := range it.Index_rows {
		if node.Table_name == table_from && 
		((node.Index_to >= to && to >= node.Index_from) ||  (node.Index_to >= from && from >= node.Index_from)) {
			from_method := get_slices(from, to, node)
			result = append(result, from_method...) 
			//make it async and combine the results later 
		}
	}
	json_rows_bytes, _ := json.Marshal(result)
	fmt.Fprintf(w, string(json_rows_bytes))


}

func get_rows_op(key int, operation string, table_from string){
	switch (operation){
		case "eq":
			get_rows_equals_to(key, table_from)
			break
		case "gt":
			get_rows_greater_than(key)
			break
		case "st":
			get_rows_smaller_than(key)
			break
	}
}

func get_rows_equals_to(key int, table_from string) []mem_row {

	//get which node I am
	//check range for the current node
	//chech it if this node can satisfy	this clause		

	var result []mem_row
	for _, node := range it.Index_rows {
		if node.Table_name == table_from && node.Index_to <= key && node.Index_from >= key {
			result = append(get_slices(key, key, node)) 
			//make it async and combine the results later 
		}
	}
	return result;

}

func get_rows_greater_than(key int) []mem_row{
	var result []mem_row
	for _, node := range it.Index_rows {
		if node.Index_to >= key && node.Index_from <= key {
			result = append(get_slices(key, node.Index_to, node)) 
			//make it async and combine the results later 
		}else if node.Index_from >= key {
			result = append(get_slices(node.Index_from, node.Index_to, node))
		}
	}
	return result;
}

func get_rows_smaller_than(key int) []mem_row{
	var result []mem_row
	for _, node := range it.Index_rows {
		if node.Index_to >= key && node.Index_from <= key {
			result = append(get_slices(node.Index_from, key, node)) 
			//make it async and combine the results later 
		}else if node.Index_to <= key {
			result = append(get_slices(node.Index_from, node.Index_to, node))
		}
	}
	return result;
}


func get_slices(from int, to int, ir index_row) []mem_row{
	//if (len(mt.rows) > 
	var rows []mem_row
	response, err := http.Get("http://" +  ir.Instance_ip + ":" + ir.Instance_port + "/" + ir.Instance_name +  "/get_slices_worker?from="+ strconv.Itoa(from) + "&to=" + strconv.Itoa(to) + "&table_from=" + ir.Table_name)
	if err != nil {
		log.Fatal(err)
	}

	dec := json.NewDecoder(response.Body)
    dec.DisallowUnknownFields()

    err = dec.Decode(&rows)
    if err != nil {
		log.Fatal(err)
    }

	return rows
}

func get_slices_worker(w http.ResponseWriter, r *http.Request) {
	

	fromStr := r.URL.Query().Get("from")

	fmt.Println(" FROM: " + fromStr)

	toStr := r.URL.Query().Get("to")
	fmt.Println(" TO: " + toStr)
	table_from := r.URL.Query().Get("table_from")

	from, err := strconv.Atoi(fromStr)
	if err != nil {
		log.Fatal(err)
	}
	to, err := strconv.Atoi(toStr)
	if err != nil {
		log.Fatal(err)
	}

	var rows_result []mem_row
	for _, row := range mt.Rows {
		if row.Key_id >= from && row.Key_id <= to && row.Table_name == table_from{
			rows_result = append(rows_result, row)
		}
	}

	json_rows_bytes, _ := json.Marshal(rows_result)
	fmt.Fprintf(w, string(json_rows_bytes))
}


func select_data(w http.ResponseWriter, r *http.Request){
	//if (len(mt.rows) > 
	var rows []mem_row
	var result []mem_row
	table_name := r.URL.Query().Get("table")
	where_field := r.URL.Query().Get("where_field")
	where_content := r.URL.Query().Get("where_content")
	where_operator := r.URL.Query().Get("where_operator")

	for _, ir := range configs_file.Peers {
		url := "http://" +  ir.Ip + ":" + ir.Port + "/" + ir.Name +  "/select_data_where_worker_" + where_operator + "?table=" + table_name + "&where_field=" + where_field + "&where_content=" + where_content
		fmt.Println(url)
		response, err := http.Get(url)
		if err != nil {
			fmt.Println(err)
		}else{


			dec := json.NewDecoder(response.Body)
			dec.DisallowUnknownFields()

			err = dec.Decode(&rows)
			if err != nil {
				log.Fatal(err)
			}
			result = append(result, rows...)
		}
	}

	json_rows_bytes, _ := json.Marshal(result)
	fmt.Fprintf(w, string(json_rows_bytes))

}

func select_data_where_worker_equals(w http.ResponseWriter, r *http.Request) {
	

	table_name := r.URL.Query().Get("table")
	where_field := r.URL.Query().Get("where_field")
	where_content := r.URL.Query().Get("where_content")
	//where_operator := r.URL.Query().Get("where_operator") // Method only for = operator. Another one will be created for contains, bigger than and smaller than


	var rows_result []mem_row
	for _, row := range mt.Rows {
		if row.Table_name == table_name{
			if row.Parsed_Document[where_field] == where_content{
				rows_result = append(rows_result, row)
			}
		}
			//var result_document 
		// Unmarshal or Decode the JSON to the interface.
	}

	json_rows_bytes, _ := json.Marshal(rows_result)
	fmt.Fprintf(w, string(json_rows_bytes))
}

func select_data_where_worker_contains(w http.ResponseWriter, r *http.Request) {
	

	table_name := r.URL.Query().Get("table")
	where_field := r.URL.Query().Get("where_field")
	where_content := r.URL.Query().Get("where_content")
	//where_operator := r.URL.Query().Get("where_operator") // Method only for = operator. Another one will be created for contains, bigger than and smaller than


	var rows_result []mem_row
	for _, row := range mt.Rows {
		if row.Table_name == table_name{
			if strings.Contains(row.Parsed_Document[where_field].(string), where_content){
				rows_result = append(rows_result, row)
			}
		}
			//var result_document 
		// Unmarshal or Decode the JSON to the interface.
	}

	json_rows_bytes, _ := json.Marshal(rows_result)
	fmt.Fprintf(w, string(json_rows_bytes))
}



func select_data_where_worker_equals_rsocket(payload interface{}) interface{}{
	
	payload_content, ok :=  payload.(map[string] interface{})
	if !ok{
		fmt.Println("ERROR!")	
	}

	table_name := payload_content["table"].(string)
	where_field := payload_content["where_field"].(string)
	where_content := payload_content["where_content"].(string)

	var rows_result []mem_row
	for _, row := range mt.Rows {
		if row.Table_name == table_name{
			if row.Parsed_Document[where_field] == where_content{
				rows_result = append(rows_result, row)
			}
		}
	}

	return rows_result
}

func select_data_where_worker_contains_rsocket(payload interface{}) interface{}{
	
	payload_content, ok :=  payload.(map[string] interface{})
	if !ok{
		fmt.Println("ERROR!")	
	}
	fmt.Println("It's called")
	table_name := payload_content["table"].(string)
	where_field := payload_content["where_field"].(string)
	where_content := payload_content["where_content"].(string)

	var rows_result []mem_row
	for _, row := range mt.Rows {
		if row.Table_name == table_name{
			if strings.Contains(row.Parsed_Document[where_field].(string), where_content){
				rows_result = append(rows_result, row)
			}
		}
	}

	return rows_result
}


func select_data_rsocket(payload interface{}) interface{}{
	var rows []mem_row
	var result []mem_row
	payload_content, ok :=  payload.(map[string] interface{})
	if !ok{
		fmt.Println("ERROR!")	
	}
	table_name := payload_content["table"].(string)
	where_field := payload_content["where_field"].(string)
	where_content := payload_content["where_content"].(string)
	where_operator := payload_content["where_operator"].(string)

	var jsonStr = `
	{
	"table":"%s",
	"where_field":"%s",
	"where_content":"%s"
	}
	`

	for _, ir := range configs_file.Peers {
		jsonStr = fmt.Sprintf(jsonStr,table_name,where_field, where_content)
		jsonMap := make(map[string]interface{})
		json.Unmarshal([]byte(jsonStr), &jsonMap)
		url := "/" + ir.Name +  "/select_data_where_worker_" + where_operator
		_port, _ := strconv.Atoi(ir.Port)
		rsocket_json_requests.RequestConfigs(ir.Ip, _port)
		
		response, err := rsocket_json_requests.RequestJSON(url, jsonMap)
		if (err != nil){
			fmt.Println(err)
		}else{
			if response != nil {
				intermediate_inteface := response.([]interface{})
				json_rows_bytes, _ := json.Marshal(intermediate_inteface)
				
				//fmt.Println(intermediate_inteface)
				reader := bytes.NewReader(json_rows_bytes)

				dec := json.NewDecoder(reader)
				dec.DisallowUnknownFields()

				err = dec.Decode(&rows)
				if err != nil {
					log.Fatal(err)
				}

				result = append(result, rows...)
			}
		}
	}

	return result
}

func select_data_where_worker_contains_rsocket_sql(payload interface{}) interface{}{
	logic_filters := payload.(Filter)
	//Distribute here the call to other instances.
	filteredResult := selectFieldsDecoupled2(logic_filters, logic_filters, 0, "")
	_query_temp_tables = nil
	_currentQueryId = 0
	return filteredResult
}

func selectFieldsDecoupled2(logic_filters Filter, fullLogicFilters Filter, indexFilter int, aliasSubquery string) interface {} {
	var tableResult []mem_table_queries
	futureAliasSubquery:=""
	// if indexFilter == 0 && (len(logic_filters.TableObject) > 0) && logic_filters.TableObject[0].IsSubquery {
	if (len(logic_filters.TableObject) > 0) && logic_filters.TableObject[0].IsSubquery {
		futureAliasSubquery = logic_filters.TableObject[0].Alias
		if indexFilter == 0 {aliasSubquery = futureAliasSubquery}
	}
	if aliasSubquery != "" && futureAliasSubquery == "" { futureAliasSubquery = aliasSubquery}
	indexFilter++

	for _, filter := range  logic_filters.ChildFilters {
		tableResult = selectFieldsDecoupled2(filter, fullLogicFilters, indexFilter, futureAliasSubquery).([]mem_table_queries)
	}

	//If is a select_to_show clause I need to Check the SelectClause and if there is no table, I create one in the in memory qyery
	if len(logic_filters.ChildFilters) > 0{
		if (len(logic_filters.ChildFilters[0].SelectClause ) > 0 ) {

			tables := lookForRelatedTablesInFilters2(logic_filters, indexFilter)
			var tableWorking []string 

			if len(tables) > 0 {
				for _, table := range tables{
					//Check existance in query_objects
					indexMemTable := isInQueryObject(table)
					if indexMemTable == -1{
						//Check existance in (index_ for distributed) mem_table
						if isInMemTable(table, logic_filters.ChildFilters[0].SelectClause) == false{
							if reflect.TypeOf(table.SelectableObject) ==  reflect.TypeOf(fullLogicFilters){
								tableResult = selectFieldsDecoupled2(table.SelectableObject.(Filter), fullLogicFilters, indexFilter, futureAliasSubquery).([]mem_table_queries)
							}else{
								var columns map[string]interface{}
								for _, column := range logic_filters.SelectClause{
									columns[column.Name] = column.SelectableObject
								}
								newRow := mem_table_queries{TableName: strconv.Itoa(indexFilter), Rows:columns}
								_query_temp_tables = append(_query_temp_tables, newRow)
								tableWorking = append(tableWorking , strconv.Itoa(indexFilter))
								tableResult = append(tableResult, newRow)
							} //I need to return the name of the table I'm working, get it in the mem_query and apply filters on the way back. I just insert on the mem query what is according to Filter
						}else{
							tableWorking = append(tableWorking , table.Name)
							tableResult = GetTableSummarize(tableWorking, logic_filters, logic_filters.ChildFilters[0].SelectClause, aliasSubquery, indexFilter)
						}
					}else{
						if ( table.Alias != ""){
							tableWorking = append(tableWorking , table.Alias)
						}else{
							tableWorking = append(tableWorking , table.Name)
						}
						
						tableResult = GetTableSummarize(tableWorking, logic_filters, logic_filters.ChildFilters[0].SelectClause, aliasSubquery, indexFilter)
					}
					//if it does not exist anywhere, throw an error
				}

			}
		}
	}
	
	return  tableResult
}
// var _query_table_id int = 0
func GetTableSummarize(tables [] string, filter Filter, selectObject []SqlClause, aliasSubquery string, indexFilter int) []mem_table_queries{
	var tableResult []mem_table_queries
	var clauseValidation sql_parser.CommandTree
	// _query_table_id ++

	for _, table := range tables {
		index := 0
		for index < len(_query_temp_tables)  {

			if _query_temp_tables[index].TableName == table{

				if applyLogic2(_query_temp_tables[index] , filter){
					columns := make(map[string]interface{})

					for _, column := range selectObject{
						if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(clauseValidation){
							clause := column.SelectableObject.(sql_parser.CommandTree)
							columns[clause.Clause] = make(map[string]interface{})
							rowColumn := _query_temp_tables[index].Rows.(map[string]interface{})
							columns[clause.Clause] = rowColumn[clause.Clause]
						}else{
							if column.Alias != "" {
								columns[column.Alias] = make(map[string]interface{})
								columns[column.Alias] =  column.SelectableObject
							}else{
								columns[column.Name] = make(map[string]interface{})
								columns[column.Name] =  column.SelectableObject
							}
						}
					}
					tableWrite := ""
					if aliasSubquery != ""{
						tableWrite = aliasSubquery
					}else{
						tableWrite = strconv.Itoa(indexFilter)
					}
					newRow := mem_table_queries{TableName: tableWrite, Rows:columns}
					tableResult = append(tableResult, newRow)

				}
			}
			index ++
		}
	}

	_query_temp_tables = append(_query_temp_tables, tableResult...)

	return  tableResult
} 


func lookForRelatedTablesInFilters(fullLogicFilters Filter, level int) []SqlClause{
	var tables []SqlClause
	for _, filter := range  fullLogicFilters.ChildFilters {

		if len(filter.ChildFilters) > 0{
			
			fmt.Println(len(filter.ChildFilters))
			fmt.Println(filter.ChildFilters)
			for _, filterChild := range filter.ChildFilters{
				if len(filterChild.TableObject) > 0{
					tables = append(tables, filterChild.TableObject...)
				}
			}
		}
		//Search also in the abstract tree. If  the abstract tree has it, then we should return the rules referent to the subtree in the Filters . 
		// Maybe implement if the context is  table_from or from, also need to add the alias to it (?)

	}
	return tables
}

func lookForRelatedTablesInFilters2(fullLogicFilters Filter, level int) []SqlClause{
	var tables []SqlClause
	if len(fullLogicFilters.TableObject) > 0 {
		tables = append(tables, fullLogicFilters.TableObject...)
	}
	return tables
}

func lookForRelatedFiltersInFilters(fullLogicFilters Filter, level int) []Filter{
	var filters []Filter
	for _, filter := range  fullLogicFilters.ChildFilters {
		if filter.CommandLeft != nil{
			filters = append(filters, filter)
		}
	}
	return filters
}

func isInQueryObject(selectableObject SqlClause) int {
	for index, query := range _query_temp_tables {
		if query.TableName == selectableObject.Name || query.TableName == selectableObject.Alias{
			return index
		}
	}
	return -1
}

func isInMemTable(tableObject SqlClause, selectObject []SqlClause) bool {
	result := false
	// var clauseValidation sql_parser.CommandTree
	for _, row := range mt.Rows {
		if (row.Table_name == tableObject.Name) || (row.Table_name == tableObject.Alias){
			name := ""
			if row.Table_name == tableObject.Name { name = tableObject.Name }else{name = tableObject.Alias}
			newRow := mem_table_queries{TableName: name, Rows:row.Parsed_Document}
			_query_temp_tables = append(_query_temp_tables, newRow)
			result = true
		}
	}

	fmt.Println(_query_temp_tables)
	return result
}

func GetValueFromFilter(contentMemRow interface{}, referenceType interface{}) interface{}{
	str := ""
	intVar := 30
	floatVar := 2.321
	
	
	var result interface {}

	switch (reflect.TypeOf(referenceType)){
	case reflect.TypeOf(str):
		result =  strings.Replace(contentMemRow.(string), "'", "", -1)
		// result =  strings.Replace(result.(string), '"', '')
		break
	case reflect.TypeOf(intVar):
		result, _ =  strconv.Atoi(contentMemRow.(string))
		break
	case reflect.TypeOf(floatVar):
		result, _ =  strconv.ParseFloat(contentMemRow.(string), 64)
		break
	default:
		result = ""
		break

	}

	return result
}

func getBiggerThan(value1 interface{}, value2 interface{}) bool{

	intVar := 30
	floatVar := 2.321

	switch (reflect.TypeOf(value1)){

	case reflect.TypeOf(intVar):
		return value1.(int) > value2.(int)
		break
	case reflect.TypeOf(floatVar):
		return value1.(float64) > value2.(float64)
		break
	default:
		return value1.(int) > value2.(int)
		break

	}
	return false
}

func GetComparisonTypeAndCompare(gateName string, leftValue bool, rightValue bool) bool{
	switch strings.ToLower(gateName){
	case "and":
		return AndCompare(leftValue, rightValue)
		break
	case "or":
		return OrCompare(leftValue, rightValue)
		break
	default:
		return false
		break
	}
	return false
}


func select_data_rsocket_sql(payload interface{}) interface{}{
	//if (len(mt.rows) > 
	var rows []mem_row
	var result []mem_row
	payload_content, ok :=  payload.(map[string] interface{})
	if !ok{
		fmt.Println("ERROR!")	
	}
	table_name := payload_content["table"].(string)
	where_field := payload_content["where_field"].(string)
	where_content := payload_content["where_content"].(string)
	where_operator := payload_content["where_operator"].(string)

	var jsonStr = `
	{
	"table":"%s",
	"where_field":"%s",
	"where_content":"%s"
	}
	`

	for _, ir := range configs_file.Peers {
		jsonStr = fmt.Sprintf(jsonStr,table_name,where_field, where_content)
		jsonMap := make(map[string]interface{})
		json.Unmarshal([]byte(jsonStr), &jsonMap)
		url := "/" + ir.Name +  "/select_data_where_worker_" + where_operator
		_port, _ := strconv.Atoi(ir.Port)
		rsocket_json_requests.RequestConfigs(ir.Ip, _port)
		
		response, err := rsocket_json_requests.RequestJSON(url, jsonMap)
		if (err != nil){
			fmt.Println(err)
		}else{
			if response != nil {
				intermediate_inteface := response.([]interface{})
				json_rows_bytes, _ := json.Marshal(intermediate_inteface)
				
				//fmt.Println(intermediate_inteface)
				reader := bytes.NewReader(json_rows_bytes)

				dec := json.NewDecoder(reader)
				dec.DisallowUnknownFields()

				err = dec.Decode(&rows)
				if err != nil {
					log.Fatal(err)
				}

				result = append(result, rows...)
			}
		}
	}

	return result
}

func applyLogic2(current_row mem_table_queries, logicObject Filter) bool{
	result := false
	previousResult := false
	previousGate := ""


	for _, filter := range logicObject.ChildFilters{

		if (reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(filter) && reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(current_row)){
			// Still work in progress, not working
			commandFilterLeft := filter.CommandLeft.(Filter)
			commandFilterRight := filter.CommandRight.(Filter)
			if len(commandFilterLeft.ChildFilters) > 0{
				result = applyLogic2(current_row, commandFilterLeft)
			}else if len(commandFilterRight.ChildFilters) > 0{
				
			}else{
				result = GetFilterAndFilter2(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
			
		}else if (reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(filter)) {
			commandFilterLeft := filter.CommandLeft.(Filter)
			if len(commandFilterLeft.ChildFilters) > 0{
				result = applyLogic2(current_row, commandFilterLeft)
			}else{
				result = GetFilterAndFilter2(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		}else if (reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(filter)) {
			commandFilterRight := filter.CommandRight.(Filter)
			if len(commandFilterRight.ChildFilters) > 0{
				result = applyLogic2(current_row, commandFilterRight)
			}else{
				result = GetFilterAndFilter2(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		}else{
			// the above statements should cover cases like "where field IN ()" or some complex subquery logic in where. 

			if filter.CommandLeft == nil {
				if len(filter.ChildFilters) > 0{
					result = applyLogic2(current_row, filter)
				}
			}else{
				//if current row has prefix, we need to check in this method if the prefix corresponds to the table name/alias of the commandleft or command right
				//if they are both CommandTrees, means it's a join. Check which side of the operation it is then scan mem_query for the other side prefix
				result = GetFilterAndFilter2(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		}

		if previousGate != ""{
			result = GetComparisonTypeAndCompare(previousGate, result, previousResult)
		}
		previousGate = filter.Gate
		previousResult = result
	}
	return result
}

func GetFilterAndFilter2(operator string, leftValue interface{}, rightValue interface{}, row mem_table_queries) bool{
	//if 
	var filterLeft sql_parser.CommandTree
	var filterLeftPointer * sql_parser.CommandTree
	var newLeftValue, newRightValue interface{}
	var clause sql_parser.CommandTree
	
	mapRow := row.Rows.(map[string] interface{})


	if (reflect.TypeOf(leftValue) == reflect.TypeOf(filterLeft) && reflect.TypeOf(rightValue) == reflect.TypeOf(filterLeft)){
		//It's a join
		clause = leftValue.(sql_parser.CommandTree)
		newLeftValue = GetValueFromFilter(mapRow[clause.Clause].(string), mapRow[clause.Clause].(string))

		//TODO for with Table to join on field
	} else if (reflect.TypeOf(rightValue) == reflect.TypeOf(filterLeft)) || (reflect.TypeOf(rightValue) == reflect.TypeOf(filterLeftPointer)) {
		if reflect.TypeOf(rightValue) == reflect.TypeOf(filterLeftPointer){
			clause = *rightValue.(*sql_parser.CommandTree)
		}else{
			clause = rightValue.(sql_parser.CommandTree)
		}

		if (clause.Clause == "table_name"){
			newRightValue = GetValueFromFilter(row.TableName, leftValue)
		}else if (mapRow[clause.Clause] != nil){
			newRightValue = GetValueFromFilter(mapRow[clause.Clause].(string), leftValue)
		}
		newLeftValue = leftValue
	}else if (reflect.TypeOf(leftValue) == reflect.TypeOf(filterLeft)) || (reflect.TypeOf(leftValue) == reflect.TypeOf(filterLeftPointer)) {
		if reflect.TypeOf(leftValue) == reflect.TypeOf(filterLeftPointer){
			clause = *leftValue.(*sql_parser.CommandTree)
		}else{
			clause = leftValue.(sql_parser.CommandTree)
		}

		fmt.Println(clause.Clause)
		if (clause.Clause == "table_name"){
			newLeftValue = GetValueFromFilter(row.TableName, rightValue)
		}else if (mapRow[clause.Clause] != nil){
			newLeftValue = GetValueFromFilter(mapRow[clause.Clause].(string), rightValue)
		}
		fmt.Println("NEITHER")
		fmt.Println(mapRow)

		newRightValue = rightValue
	}else{
		newLeftValue = leftValue
		newRightValue = rightValue
	}
	switch strings.ToLower(operator){
	case "equals":
		return (newLeftValue == newRightValue)
		break
	case "bigger_than":
		return getBiggerThan(newLeftValue , newRightValue)
		break
	default:
		return false
		break
	}
	return false
}