package main

import (
    "fmt"

	"log"

	"io/ioutil"
	"net/http"
	"strings"

	"encoding/json"
	"strconv"
	"rsocket_json_requests"
)


func delete_data_where(w http.ResponseWriter, r *http.Request) {
	

	table_name := r.URL.Query().Get("table")
	where_field := r.URL.Query().Get("where_field")
	where_content := r.URL.Query().Get("where_content")
	where_operator := r.URL.Query().Get("where_operator") // Method only for = operator. Another one will be created for contains, bigger than and smaller than
	//var result = ""
	for _, node := range it.Index_rows {
		if node.Table_name == table_name {
			//from_method := call_delete_worker(where_operator, where_field, where_content , node)
			call_delete_worker(where_operator, where_field, where_content , node)
			//result += from_method 
			//make it async and combine the results later 
		}
	}

	fmt.Fprintf(w, "Rows Affected: ") //+ result)
}

func call_delete_worker(where_operator string, where_field string, where_content string, ir index_row) string{
	//if (len(mt.rows) > 


	var rows string = ""
	var body []byte
	response, err := http.Get("http://" +  ir.Instance_ip + ":" + ir.Instance_port + "/" + ir.Instance_name +  "/delete_data_where_worker_"+ where_operator +"?table=" + ir.Table_name + "&where_field=" + where_field + "&where_content=" + where_content)
	if err != nil {
		fmt.Println(err)
	}else{


		//dec := json.NewDecoder(response.Body)
		//dec.DisallowUnknownFields()

		body, err = ioutil.ReadAll(response.Body)

		if err != nil {
			log.Fatal(err)
		}

		rows = string(body)
	}

	return rows
}

func delete_data_where_worker_contains(w http.ResponseWriter, r *http.Request) {
	

	table_name := r.URL.Query().Get("table")
	where_field := r.URL.Query().Get("where_field")
	where_content := r.URL.Query().Get("where_content")
	//where_operator := r.URL.Query().Get("where_operator") // Method only for = operator. Another one will be created for contains, bigger than and smaller than


	var rows_result []mem_row
	i := len(mt.Rows)
	rows_result = mt.Rows
	fmt.Println("----- sizeofI: %d", i)
	for i > 0 {
		i--
		if mt.Rows[i].Table_name == table_name{
			if strings.Contains(mt.Rows[i].Parsed_Document[where_field].(string), where_content){
				fmt.Println("----- sizeofI: %d", i)
				rows_result = remove_index(rows_result, i)
			}
		}
	} 

	mt.Rows = rows_result

	fmt.Fprintf(w, "Rows Affected: " + string(len(rows_result)))
}

func remove_index(s []mem_row, index int) []mem_row {
    ret := make([]mem_row, 0)
    ret = append(ret, s[:index]...)
    return append(ret, s[index+1:]...)
}


func delete_data_where_rsocket(payload interface{}) interface{} {
	

	// table_name := r.URL.Query().Get("table")
	// where_field := r.URL.Query().Get("where_field")
	// where_content := r.URL.Query().Get("where_content")
	// where_operator := r.URL.Query().Get("where_operator") // Method only for = operator. Another one will be created for contains, bigger than and smaller than

	payload_content, ok :=  payload.(map[string] interface{})
	if !ok{
		fmt.Println("ERROR!")	
	}
	table_name := payload_content["table"].(string)
	where_field := payload_content["where_field"].(string)
	where_content := payload_content["where_content"].(string)
	where_operator := payload_content["where_operator"].(string)


	//var result = ""
	for _, node := range it.Index_rows {
		if node.Table_name == table_name {
			//from_method := call_delete_worker(where_operator, where_field, where_content , node)
			call_delete_worker_rsocket(where_operator, where_field, where_content , node)
			//result += from_method 
			//make it async and combine the results later 
		}
	}

	//fmt.Fprintf(w, "Rows Affected: ") //+ result)
	return "success"
}

func call_delete_worker_rsocket(where_operator string, where_field string, where_content string, ir index_row) string{
	//if (len(mt.rows) > 


	//var rows string = ""
	//var body []byte
	//response, err := http.Get("http://" +  ir.Instance_ip + ":" + ir.Instance_port + "/" + ir.Instance_name +  "/delete_data_where_worker_"+ where_operator +"?table=" + ir.Table_name + "&where_field=" + where_field + "&where_content=" + where_content)
	
	var jsonStr = `
	{
	"table":"%s",
	"where_field":"%s",
	"where_content":"%s"
	}
	`
	jsonStr = fmt.Sprintf(jsonStr, ir.Table_name, where_field, where_content)

	fmt.Println(jsonStr)

	jsonMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &jsonMap)

	if err != nil {
		panic(err)
	}

	fmt.Println(jsonMap)
	_port, _ := strconv.Atoi(ir.Instance_port)
	rsocket_json_requests.RequestConfigs(ir.Instance_ip, _port)
	result, err1 := rsocket_json_requests.RequestJSON("/" + ir.Instance_name + "/delete_data_where_worker_" + where_operator, jsonMap)
	if (err1!=nil){
		fmt.Println(err1)
	}
	
	if err != nil {
		fmt.Println(err)
	}else{


		//dec := json.NewDecoder(response.Body)
		//dec.DisallowUnknownFields()

		//body, err = ioutil.ReadAll(response.Body)

		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(result)
		//rows = result.(string)
	}

	return ""
}

func delete_data_where_worker_contains_rsocket(payload interface{}) interface{} {
	

	// table_name := r.URL.Query().Get("table")
	// where_field := r.URL.Query().Get("where_field")
	// where_content := r.URL.Query().Get("where_content")

	payload_content, ok :=  payload.(map[string] interface{})
	if !ok{
		fmt.Println("ERROR!")	
	}
	table_name := payload_content["table"].(string)
	where_field := payload_content["where_field"].(string)
	where_content := payload_content["where_content"].(string)
	//where_operator := r.URL.Query().Get("where_operator") // Method only for = operator. Another one will be created for contains, bigger than and smaller than


	var rows_result []mem_row
	rows_affected := 0

	i := len(mt.Rows)
	rows_result = mt.Rows
	fmt.Println("----- sizeofI: %d", i)
	for i > 0 {
		i--
		if mt.Rows[i].Table_name == table_name{
			if strings.Contains(mt.Rows[i].Parsed_Document[where_field].(string), where_content){
				rows_affected ++
				fmt.Println("----- sizeofI: %d", i)
				rows_result = remove_index(rows_result, i)
			}
		}
	} 

	mt.Rows = rows_result

	//fmt.Fprintf(w, "Rows Affected: " + string(len(rows_result)))
	fmt.Println("Rows Affected: " + string(rows_affected))

	return "Rows Affected: " + string(rows_affected)
}