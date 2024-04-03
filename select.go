package main

import (
	"bytes"
    "fmt"
	"log"
	//"errors"	
	"encoding/json"
	"strconv"
	"net/http"
	"github.com/gorilla/mux"
	"strings"
	"github.com/bcosso/sqlparserproject"
	"github.com/bcosso/rsocket_json_requests"
	"reflect"
)

// relationship := Relationship{TableNameRight: clauseRight.Prefix, ColumnLeft: clauseLeft.Clause, ColumnRight: clauseRight.Clause, IndexInMemQuery:indexRight }

type Relationship struct {
	TableNameRight   string
	ColumnLeft   string
	ColumnRight   string
	IndexInMemQuery   int
	RelatedRow * mem_table_queries
}

type mem_query_collection struct{
	TableName string
	TableContent []mem_table_queries
}

// var _query map[string] []mem_table_queries

type mem_table_queries struct {
	TableName   string
	QueryId		int
	Relationships [] Relationship
	Rows 		interface{}
}
// var _currentQueryId int = 0
// var _query_temp_tables []mem_table_queries

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
		err1 := json.Unmarshal([]byte(jsonStr), &jsonMap)
		if (err1 != nil){
			fmt.Println(err1)
		}
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
	ctx :=  make(map[string] interface{})
	_query := make(map[string] []mem_table_queries)
	ctx["_query"] = _query
	_analyzedFilterList := make(map[string]int)
	ctx["_analyzedFilterList"] = _analyzedFilterList

	//Distribute here the call to other instances.
	filteredResult := selectFieldsDecoupled2(logic_filters, logic_filters, 0, "", &ctx)
	// _query_temp_tables = nil

	// _currentQueryId = 0
	fmt.Println(filteredResult)

	return filteredResult
}

func selectFieldsDecoupled2(logic_filters Filter, fullLogicFilters Filter, indexFilter int, aliasSubquery string, ctx * map[string] interface{}) interface {} {
	var tableResult []mem_table_queries
	futureAliasSubquery:=""
	_query := (*ctx)["_query"].((map[string] []mem_table_queries))

	if (len(logic_filters.TableObject) > 0) && logic_filters.TableObject[0].IsSubquery {
		futureAliasSubquery = logic_filters.TableObject[0].Alias
		if indexFilter == 0 {aliasSubquery = futureAliasSubquery}
	}
	if aliasSubquery != "" && futureAliasSubquery == "" { futureAliasSubquery = aliasSubquery}
	indexFilter++

	for _, filter := range  logic_filters.ChildFilters {
		tableResult = selectFieldsDecoupled2(filter, fullLogicFilters, indexFilter, futureAliasSubquery, ctx).([]mem_table_queries)
	}



	//If is a select_to_show clause I need to Check the SelectClause and if there is no table, I create one in the in memory qyery
	if len(logic_filters.ChildFilters) > 0{
		if (len(logic_filters.ChildFilters[0].SelectClause ) > 0 ) {

			tables := lookForRelatedTablesInFilters2(logic_filters, indexFilter)
			var tableWorking []string 
			checkForTablesInNodes(tables, logic_filters, ctx)
			if len(tables) > 0 {
				for _, table := range tables{
					//Check existance in query_objects
					foundMemTable := isInQueryObject(table, ctx)
					if !foundMemTable {
						//Check existance in (index_ for distributed) mem_table
						//Good place to fetch distributed data
						if isInMemTable(table, logic_filters.ChildFilters[0].SelectClause, ctx) == false{
							if reflect.TypeOf(table.SelectableObject) ==  reflect.TypeOf(fullLogicFilters){
								tableResult = selectFieldsDecoupled2(table.SelectableObject.(Filter), fullLogicFilters, indexFilter, futureAliasSubquery, ctx).([]mem_table_queries)
							}else{
								var columns map[string]interface{}
								for _, column := range logic_filters.SelectClause{
									columns[column.Name] = column.SelectableObject
								}
								newRow := mem_table_queries{TableName: strconv.Itoa(indexFilter), Rows:columns}

								_query[strconv.Itoa(indexFilter)] = append(_query[strconv.Itoa(indexFilter)], newRow)


								tableWorking = append(tableWorking , strconv.Itoa(indexFilter))
								tableResult = append(tableResult, newRow)
							} //I need to return the name of the table I'm working, get it in the mem_query and apply filters on the way back. I just insert on the mem query what is according to Filter
						}else{
							if ( table.Alias != ""){
								tableWorking = append(tableWorking , table.Alias)
							}else{
								tableWorking = append(tableWorking , table.Name)
							}
							tableResult = GetTableSummarize(tableWorking, logic_filters, logic_filters.ChildFilters[0].SelectClause, aliasSubquery, indexFilter, ctx)
						}
					}else{
						if ( table.Alias != ""){
							tableWorking = append(tableWorking , table.Alias)
						}else{
							tableWorking = append(tableWorking , table.Name)
						}
						
						tableResult = GetTableSummarize(tableWorking, logic_filters, logic_filters.ChildFilters[0].SelectClause, aliasSubquery, indexFilter, ctx)
					}
					//if it does not exist anywhere, throw an error
				}

			}
		}
	}
	
	return  tableResult
}
// var _query_table_id int = 0
func GetTableSummarize(tables [] string, filter Filter, selectObject []SqlClause, aliasSubquery string, indexFilter int, ctx * map[string] interface {} ) []mem_table_queries{
	var tableResult []mem_table_queries
	_query := (*ctx)["_query"].(map[string] []mem_table_queries)
	tableWrite := ""

	if aliasSubquery != ""{
		tableWrite = aliasSubquery
	}else{
		tableWrite = strconv.Itoa(indexFilter)
	}

	for _, table := range tables {
		index := 0
		for index < len(_query[table])  {
			if applyLogic2(_query[table][index] , &filter, ctx){
				columns := make(map[string]interface{})
				for _, column := range selectObject{
					columnResult := ProjectColumns(_query[table][index], column, ctx)
					if (column.Alias != ""){
						columnResult.Alias = column.Alias
					}// columnResult.Name = column.Name

					if columnResult.Alias != "" {
						columns[columnResult.Alias] = make(map[string]interface{})
						columns[columnResult.Alias] =  columnResult.SelectableObject
					}else{
						columns[columnResult.Name] = make(map[string]interface{})
						columns[columnResult.Name] =  columnResult.SelectableObject
					}
				}
				newRow := mem_table_queries{TableName: tableWrite, Rows:columns}
				tableResult = append(tableResult, newRow)
			}
			
			index ++
		}
		_query[tableWrite] = append(_query[tableWrite], tableResult...)
	}

	return  tableResult
} 

func CheckColumnExistance(row mem_table_queries, clause sqlparserproject.CommandTree) (interface{}, bool){
	rowColumn := row.Rows.(map[string]interface{})
	actualValue, found := rowColumn[clause.Clause]
	if found == false {
		for _, relationship := range row.Relationships{
			rowColumn = (*relationship.RelatedRow).Rows.(map[string]interface{})
			actualValueRelationship, foundRelationship := rowColumn[clause.Clause]
			if foundRelationship == true{
				actualValue = actualValueRelationship
				found = foundRelationship
			}
		}
	}
	
	return actualValue, found
}

func ProjectColumns(row mem_table_queries, column SqlClause,  ctx * map[string] interface {} ) SqlClause{
	var clauseValidation sqlparserproject.CommandTree
	// columns := make(map[string]interface{})
	var conditionValidation Condition
	var columnReturn SqlClause
	if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(clauseValidation){
		clause := column.SelectableObject.(sqlparserproject.CommandTree)
		// columns[clause.Clause] = make(map[string]interface{})
		rowColumn, found := CheckColumnExistance(row, clause)
		if found == false{
			//Change for proper error treatment here.
			fmt.Println(column.SelectableObject)
			panic("Non existent column:" + column.Name + " - " + column.Alias)
			
		}
		columnReturn.Alias = clause.Clause
		columnReturn.SelectableObject = rowColumn
		
	}else{
		if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(conditionValidation){
			columnReturn = GetConditionFlow(row, column, ctx)
		} else if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(columnReturn){
			columnReturn = column.SelectableObject.(SqlClause)
		}else{
			columnReturn = column
		}
	}
	

	return columnReturn
}

func checkForTablesInNodes(tables []SqlClause, filter Filter, ctx * map[string] interface{}){
	
	_query := (*ctx)["_query"].((map[string] []mem_table_queries))

	for _, table := range tables {
		//Search in nodes
		var jsonStr = `
		{
		"table":"%s",
		"alias":"%s",
		"filter":%s
		}
		`
		//need to check the safety of doing this in parallel
		isInMemTable(table, filter.ChildFilters[0].SelectClause, ctx)
		if (configs_file.ShardingType == "" ){
			return
		}
		for _, ir := range configs_file.Peers {
			if (configs_file.Instance_name != ir.Name){
				var rows []mem_table_queries
				jsonFilter, _ := json.Marshal(&filter) 
				jsonStr = fmt.Sprintf(jsonStr, table.Name , table.Alias ,string(jsonFilter))
				jsonMap := make(map[string]interface{})
				err1 := json.Unmarshal([]byte(jsonStr), &jsonMap)
				if (err1 != nil){
					fmt.Println(err1)
				}
				//New receiving function
				url := "/" + ir.Name +  "/select_table"
				_port, _ := strconv.Atoi(ir.Port)
				rsocket_json_requests.RequestConfigs(ir.Ip, _port)
				
				response, err := rsocket_json_requests.RequestJSON(url, jsonMap)

				if (err != nil){
					fmt.Println(err)
				}else{
					if response != nil {
						//add to _query_temp_tables
						intermediate_inteface := response.([]interface{})
						json_rows_bytes, _ := json.Marshal(intermediate_inteface)
						
						reader := bytes.NewReader(json_rows_bytes)
						dec := json.NewDecoder(reader)
						dec.DisallowUnknownFields()

						err = dec.Decode(&rows)
						if err != nil {
							log.Fatal(err)
						}
						tableName := table.Name
						if table.Alias != ""{
							tableName = table.Alias
						}

						_query[tableName] = append(_query[tableName], rows...)
					}
				}
			}
		}
	}
}

func select_table(payload interface{}) interface{}{

	payload_content, ok :=  payload.(map[string] interface{})
	if !ok{
		fmt.Println("ERROR!")	
	}

	table_name := payload_content["table"].(string)
	alias := payload_content["alias"].(string)
	// filter := payload_content["filter"].(string)
	
	var rows_result []mem_table_queries
	for _, row := range mt.Rows {
		if row.Table_name == table_name{

			//Need to apply some logic to filter unecessary data
			if alias != ""{
				table_name = alias
			}
			rowMemQuery := mem_table_queries{TableName: table_name, Rows: row.Parsed_Document}
			rows_result = append(rows_result, rowMemQuery)
		}
	}

	return rows_result
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
	for _, filter := range fullLogicFilters.ChildFilters {
		if filter.CommandLeft != nil{
			filters = append(filters, filter)
		}
	}
	return filters
}

func isInQueryObject(selectableObject SqlClause, ctx * map[string] interface{}) bool {
	_query := (*ctx)["_query"].((map[string] []mem_table_queries))
	_, found := _query[selectableObject.Name]
	_, foundALIAS := _query[selectableObject.Alias]

	return found || foundALIAS
}

func isTableInQueryObject(tableName string, ctx * map[string] interface{}) bool {
	_query := (*ctx)["_query"].((map[string] []mem_table_queries))
	_, found := _query[tableName]
	return found
}


func isInMemTable(tableObject SqlClause, selectObject []SqlClause, ctx * map[string] interface{}) bool {
	result := false
	_query := (*ctx)["_query"].((map[string] []mem_table_queries))
	// var clauseValidation sqlparserproject.CommandTree

	for _, row := range mt.Rows {
		if (row.Table_name == tableObject.Name) || (row.Table_name == tableObject.Alias){
			name := ""
			if (row.Table_name == tableObject.Name) && (tableObject.Alias == "") { name = tableObject.Name }else{name = tableObject.Alias}
			newRow := mem_table_queries{TableName: name, Rows:row.Parsed_Document}
			_query[name] = append(_query[name], newRow)
			result = true
		}
	}

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
// var _analyzedFilterList map[string]int
func applyLogic2(current_row mem_table_queries, logicObject2 * Filter, ctx * map[string] interface{}) bool{
	result := false
	previousResult := false
	previousGate := ""
	var treeReference sqlparserproject.CommandTree
	logicObject := *logicObject2
	_analyzedFilterList := (*ctx)["_analyzedFilterList"].(map[string]int)


	for _, filter := range logicObject.ChildFilters{

		if (reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(filter) && reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(filter)){
			// Still work in progress, not working

			commandFilterLeft := filter.CommandLeft.(Filter)
			commandFilterRight := filter.CommandRight.(Filter)
			if len(commandFilterLeft.ChildFilters) > 0{
				result = applyLogic2(current_row, &commandFilterLeft, ctx)
			}else if len(commandFilterRight.ChildFilters) > 0{
				//These two scenarios above have to be detailed.
			}else{
				//This should be the most common scenario "table1.Id = table2.Id"
				//Need to execute the join comparisson for the entire table only once
				//After that, need a method ONLY TO CHECK  
				if filter.AlreadyConsumed == false{
					// GetJoinAndJoin(filter.Operation, filter.CommandLeft, filter.CommandRight)
					filter.AlreadyConsumed = true
				}
				
				// Does this relationship exist?
				// current_row.Relationships 

				result = CheckRelationshipExistance(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
			
		}else if (reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(treeReference) && reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(treeReference)){

			//After that, need a method ONLY TO CHECK
			operation := filter.Operation
			clauseLeft := filter.CommandLeft.(sqlparserproject.CommandTree)
			clauseRight := filter.CommandRight.(sqlparserproject.CommandTree)  
			_, found := _analyzedFilterList[operation+"_"+clauseLeft.Clause+"_"+clauseRight.Clause]
			if found == false{
				
				if (isTableInQueryObject(clauseLeft.Prefix, ctx) && isTableInQueryObject(clauseRight.Prefix, ctx) ){
					GetJoinAndJoin(filter.Operation, filter.CommandLeft, filter.CommandRight, &current_row, ctx)

					if _analyzedFilterList == nil {
						newMap := make(map[string]int)
						newMap[operation+"_"+clauseLeft.Clause+"_"+clauseRight.Clause] = 1
						_analyzedFilterList = newMap
					}else{
						_analyzedFilterList[operation+"_"+clauseLeft.Clause+"_"+clauseRight.Clause] = 1
					}

					_analyzedFilterList[operation+"_"+clauseLeft.Clause+"_"+clauseRight.Clause] = 1
					
				}
			}else{//Unoptimized
				// GetJoinAndJoin(filter.Operation, filter.CommandLeft, filter.CommandRight)
			}
			
			result = CheckRelationshipExistance(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			
			
		} else if (reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(filter)) {
			commandFilterLeft := filter.CommandLeft.(Filter)
			if len(commandFilterLeft.ChildFilters) > 0{
				result = applyLogic2(current_row, &commandFilterLeft, ctx)
			}else{
				result = GetFilterAndFilter2(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		}else if (reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(filter)) {
			commandFilterRight := filter.CommandRight.(Filter)
			if len(commandFilterRight.ChildFilters) > 0{
				result = applyLogic2(current_row, &commandFilterRight, ctx)
			}else{
				result = GetFilterAndFilter2(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		}else{
			// the above statements should cover cases like "where field IN ()" or some complex subquery logic in where. 

			if filter.CommandLeft == nil {
				if len(filter.ChildFilters) > 0{
					result = applyLogic2(current_row, &filter, ctx)
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

func CheckWhichSideContainsColumn(operator string, leftValue interface{}, rightValue interface{}, row mem_table_queries) (int, sqlparserproject.CommandTree){
	var clause sqlparserproject.CommandTree
	var filterReference sqlparserproject.CommandTree
	var filterReferencePointer * sqlparserproject.CommandTree
	
	if (reflect.TypeOf(leftValue) == reflect.TypeOf(filterReference) && reflect.TypeOf(rightValue) == reflect.TypeOf(filterReference)){
		//It's a join
		clause = leftValue.(sqlparserproject.CommandTree)
		return 0, clause
	} else if (reflect.TypeOf(rightValue) == reflect.TypeOf(filterReference)) || (reflect.TypeOf(rightValue) == reflect.TypeOf(filterReferencePointer)) {
		clause = GetClauseFromValue(rightValue)
		return 1, clause
	}else if (reflect.TypeOf(leftValue) == reflect.TypeOf(filterReference)) || (reflect.TypeOf(leftValue) == reflect.TypeOf(filterReferencePointer)) {
		clause = GetClauseFromValue(leftValue)
		return 2, clause
	}
	
	return -1, clause
}

func GetFilterAndFilter2(operator string, leftValue interface{}, rightValue interface{}, row mem_table_queries) bool{
 
	var newLeftValue, newRightValue interface{}
	belongsToTable := false
	resultComparison := false
	mapRow := row.Rows.(map[string] interface{})

	side, clause := CheckWhichSideContainsColumn(operator, leftValue, rightValue, row)
	//side = 0 - both, side = 1 - right, side = 2 - left, side = -1 - none 

	if (side == 0){
		//It's a join
		clause = leftValue.(sqlparserproject.CommandTree)
		newLeftValue = GetValueFromFilter(mapRow[clause.Clause].(string), mapRow[clause.Clause].(string))

		//TODO for with Table to join on field
	} else if (side == 1) {
		if (clause.Clause == "table_name"){
			newRightValue = GetValueFromFilter(row.TableName, leftValue) //Not passing here anymore, since table logic is being handled elsewhere. 
		}else if (mapRow[clause.Clause] != nil){
			newRightValue = GetValueFromFilter(mapRow[clause.Clause].(string), leftValue)
		}
		newLeftValue = leftValue
	}else if (side == 2) {

		if (clause.Clause == "table_name"){ 
			newLeftValue = GetValueFromFilter(row.TableName, rightValue) //Not passing here anymore, since table logic is being handled elsewhere. 
		}else if (mapRow[clause.Clause] != nil){
			newLeftValue = GetValueFromFilter(mapRow[clause.Clause].(string), rightValue)
		}

		newRightValue = rightValue
	}else{
		belongsToTable = true
		newLeftValue = leftValue
		newRightValue = rightValue
	}

	belongsToTable = CheckBelongsToTable(row, clause)

	switch strings.ToLower(operator){
	case "equals":
		resultComparison = (newLeftValue == newRightValue)
		break
	case "bigger_than":
		resultComparison = getBiggerThan(newLeftValue , newRightValue)
		break
	case "smaller_than":
		resultComparison = getBiggerThan(newRightValue , newLeftValue)
		break
	default:
		resultComparison = false
		break
	}
	return resultComparison && belongsToTable
}

func CheckBelongsToTable(row mem_table_queries, clause sqlparserproject.CommandTree) bool{
	if clause.Prefix != ""{
		if clause.Prefix != row.TableName{
			return false
		}
	}
	
	return true
}


func GetJoinAndJoin(operator string, leftValue interface{}, rightValue interface{}, current_row * mem_table_queries, ctx * map[string] interface{}) {
	_query := (*ctx)["_query"].((map[string] []mem_table_queries))
	mapRow := (*current_row).Rows.(map[string] interface{})
	indexLeft := 0

	clauseRight := GetClauseFromValue(rightValue)
	clauseLeft := GetClauseFromValue(leftValue)

	for indexLeft < len(_query[clauseLeft.Prefix]){
		
		mapRowLeft := _query[clauseLeft.Prefix][indexLeft].Rows.(map[string] interface{})
		indexRight := 0
		for indexRight < len(_query[clauseRight.Prefix]){
			
				mapRowRight := _query[clauseRight.Prefix][indexRight].Rows.(map[string] interface{})

				if mapRowLeft[clauseLeft.Clause] == mapRowRight[clauseRight.Clause] {
					// Could do it with a hash. First doing it with a complex object, unoptimized
					relationship := Relationship{TableNameRight: clauseRight.Prefix, ColumnLeft: clauseLeft.Clause, ColumnRight: clauseRight.Clause, IndexInMemQuery:indexRight, RelatedRow: &(_query[clauseRight.Prefix][indexRight]) }
					
					_query[clauseLeft.Prefix][indexLeft].Relationships = append(_query[clauseLeft.Prefix][indexLeft].Relationships, relationship)
					
					if ((*current_row).TableName == _query[clauseLeft.Prefix][indexLeft].TableName) && mapRow[clauseLeft.Clause] == mapRowLeft[clauseLeft.Clause]{
						*current_row = _query[clauseLeft.Prefix][indexLeft]
					}
					//Add index (or pointer) to join list, so I can find the respective columns of this table in project/summarize. Will create a relationship list on _query_temp_tables. SHould also contain the name of the relationship table. Can be a map
				}
			
			indexRight ++
		}
		
		indexLeft ++
	}
}

func CheckRelationshipExistance(operator string, leftValue interface{}, rightValue interface{}, row mem_table_queries) bool {

	clauseRight := GetClauseFromValue(rightValue)
	clauseLeft := GetClauseFromValue(leftValue)
	mapRow := row.Rows.(map[string] interface{})

	for _, relation := range row.Relationships{
		if relation.TableNameRight == clauseRight.Prefix{
			mapRowRight := (*relation.RelatedRow).Rows.(map[string] interface{})
			if mapRow[clauseLeft.Clause] == mapRowRight[clauseRight.Clause]{
				return true
			}
		}
	}

	return false
}

func GetClauseFromValue(interfaceValue interface{}) sqlparserproject.CommandTree{

	var filterReferencePointer * sqlparserproject.CommandTree
	var clause sqlparserproject.CommandTree

	if reflect.TypeOf(interfaceValue) == reflect.TypeOf(filterReferencePointer){
		clause = *interfaceValue.(*sqlparserproject.CommandTree)
	}else{
		clause = interfaceValue.(sqlparserproject.CommandTree)
	}
	return clause

}

