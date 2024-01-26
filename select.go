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
	"github.com/gorilla/mux"
	"strings"
	"sql_parser"
	"reflect"
	"rsocket_json_requests"
)

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


func select_data_where_worker_contains_rsocket_sql(payload interface{}) interface{}{

	logic_filters := payload.(Filter)
	filteredResult := selectFields(logic_filters)
	return filteredResult
}

func selectFields(logic_filters Filter) interface {} {

	var rows_result []interface{}
	for _, row := range mt.Rows {
		if (applyLogic(row , logic_filters)){

			rowResult := selectField(logic_filters, row)
			rows_result = append(rows_result, rowResult)
		}


	}
	return rows_result
}

func selectField(logic_filters Filter, row mem_row) interface {}{

	if (len(logic_filters.ChildFilters) > 0 && logic_filters.SelectClause == nil){
		return selectField(logic_filters.ChildFilters[0], row)
	}else{
		var cols_result []interface{}
		var clauseValidation sql_parser.CommandTree
		for _, selectableObject := range logic_filters.SelectClause {
			var typedObject interface {}
			if reflect.TypeOf(selectableObject.SelectableObject) == reflect.TypeOf(clauseValidation){
				clause := selectableObject.SelectableObject.(sql_parser.CommandTree)
				typedObject = row.Parsed_Document[clause.Clause].(string)
			}else{
				typedObject = selectableObject.SelectableObject
			}

			cols_result = append(cols_result, typedObject)
		}
		
		return cols_result
	}
}

func applyLogic(current_row mem_row, logicObject Filter) bool{
	result := false
	previousResult := false
	previousGate := ""
	
	for _, filter := range logicObject.ChildFilters{

		if (reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(filter) && reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(current_row)){
			// Still work in progress, not working
			commandFilterLeft := filter.CommandLeft.(Filter)
			commandFilterRight := filter.CommandRight.(Filter)
			if len(commandFilterLeft.ChildFilters) > 0{
				result = applyLogic(current_row, commandFilterLeft)
			}else if len(commandFilterRight.ChildFilters) > 0{
				
			}else{
				result = GetFilterAndFilter(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
			
		}else if (reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(filter)) {
			commandFilterLeft := filter.CommandLeft.(Filter)
			if len(commandFilterLeft.ChildFilters) > 0{
				result = applyLogic(current_row, commandFilterLeft)
			}else{
				result = GetFilterAndFilter(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		}else if (reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(filter)) {
			commandFilterRight := filter.CommandRight.(Filter)
			if len(commandFilterRight.ChildFilters) > 0{
				result = applyLogic(current_row, commandFilterRight)
			}else{
				result = GetFilterAndFilter(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		}else{
			// the above statements should cover cases like "where field IN ()" or some complex subquery logic in where. 

			if filter.CommandLeft == nil {
				if len(filter.ChildFilters) > 0{
					result = applyLogic(current_row, filter)
				}
			}else{
				result = GetFilterAndFilter(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
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



func GetFilterAndFilter(operator string, leftValue interface{}, rightValue interface{}, row mem_row) bool{
	//if 
	var filterLeft sql_parser.CommandTree
	var filterLeftPointer * sql_parser.CommandTree
	var newLeftValue, newRightValue interface{}
	var clause sql_parser.CommandTree


	if (reflect.TypeOf(leftValue) == reflect.TypeOf(filterLeft) && reflect.TypeOf(rightValue) == reflect.TypeOf(filterLeft)){
		//It's a join
		clause = leftValue.(sql_parser.CommandTree)
		newLeftValue = GetValueFromFilter(row.Parsed_Document[clause.Clause].(string), row.Parsed_Document[clause.Clause].(string))

		//TODO for with Table to join on field
	} else if (reflect.TypeOf(rightValue) == reflect.TypeOf(filterLeft)) || (reflect.TypeOf(rightValue) == reflect.TypeOf(filterLeftPointer)) {
		if reflect.TypeOf(rightValue) == reflect.TypeOf(filterLeftPointer){
			clause = *rightValue.(*sql_parser.CommandTree)
		}else{
			clause = rightValue.(sql_parser.CommandTree)
		}
		
		if (clause.Clause == "table_name"){
			newLeftValue = GetValueFromFilter(row.Table_name, leftValue)
		}else if (row.Parsed_Document[clause.Clause] != nil){
			newRightValue = GetValueFromFilter(row.Parsed_Document[clause.Clause].(string), leftValue)
		}
		newLeftValue = leftValue
	}else if (reflect.TypeOf(leftValue) == reflect.TypeOf(filterLeft)) || (reflect.TypeOf(leftValue) == reflect.TypeOf(filterLeftPointer)) {
		if reflect.TypeOf(leftValue) == reflect.TypeOf(filterLeftPointer){
			clause = *leftValue.(*sql_parser.CommandTree)

			
		}else{
			clause = leftValue.(sql_parser.CommandTree)
		}

		if (clause.Clause == "table_name"){
			newLeftValue = GetValueFromFilter(row.Table_name, rightValue)
		}else if (row.Parsed_Document[clause.Clause] != nil){

			newLeftValue = GetValueFromFilter(row.Parsed_Document[clause.Clause].(string), rightValue)
		}

		newRightValue = rightValue
	}else{
		newLeftValue = leftValue
		newRightValue = rightValue
	}
	switch strings.ToLower(operator){
	case "equals":
		return (newLeftValue == newRightValue)
		break
	case "bigger":
		return getBiggerThan(newLeftValue , newRightValue)
		break
	default:
		return false
		break
	}
	return false
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