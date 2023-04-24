package main

import (
    "fmt"

	"log"

	"io/ioutil"
	"net/http"
	"strings"
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
				rows_result = RemoveIndex(rows_result, i)
			}
		}
	} 

	mt.Rows = rows_result

	fmt.Fprintf(w, "Rows Affected: " + string(len(rows_result)))
}

func RemoveIndex(s []mem_row, index int) []mem_row {
    ret := make([]mem_row, 0)
    ret = append(ret, s[:index]...)
    return append(ret, s[index+1:]...)
}