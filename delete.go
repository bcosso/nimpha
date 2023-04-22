package main

import (
    "fmt"
	// "errors"	
	//"io"

	"net/http"
	"strings"
)

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