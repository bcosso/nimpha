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