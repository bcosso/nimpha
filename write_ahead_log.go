package main

import (
	"bytes"
    "fmt"
	"log"
	// "errors"	
	"os"
	//"io"
	"io/ioutil"
	"encoding/json"
	"strconv"

	"net/http"
)



func get_wal_disk(){
	configfile, err := os.Open("wal_file.json")
    if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	err = json.Unmarshal([]byte(root), &wal)

	if err!= nil{
		fmt.Println(err)
		log.Fatal(err)
		fmt.Println(err)
	}
}

func get_wal(data_post * []mem_row ) int {

	index_row := it.Index_rows[it.Index_WAL]
	
	json_data, err := json.Marshal(data_post)

    if err != nil {
        log.Fatal(err)
	}

	fmt.Println("get_wal::::::") 
	
	response, err := http.Post("http://" +  index_row.Instance_ip + ":" + index_row.Instance_port + "/" + index_row.Instance_name +  "/read_wal", "application/json", bytes.NewBuffer(json_data))
	if err != nil {
		log.Fatal(err)
		fmt.Println("err::::::") 
	}
	fmt.Println("res::::::") 
	fmt.Println(response.Body) 
	fmt.Println(":::::::::") 

	var p string
	// dec := json.NewDecoder(response.Body)
	// dec.Decode(&p)
	b, err := ioutil.ReadAll(response.Body)
	p = string(b) 
	
	fmt.Println("-----Reponse:" + p)
	iconv, err := strconv.Atoi(p)

	return iconv

	//fmt.Fprintf(w, strconv.Itoa(index_row.Current_index))
}


func read_wal(w http.ResponseWriter, r *http.Request) {
	

	fmt.Println("read_wal::::::") 

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	
	var wo wal_operation
    var p []mem_row
	err := dec.Decode(&p)
	if err != nil {
		log.Fatal(err)
	}
	wo.Rows = p 
	fmt.Println("read_wal::::::") 

	next_node_to_record := 0
	current_node_check := 0
	var alive_nodes []int

	if len(wal) > 0{
		latest_wal := wal[len(wal)-1]
		if latest_wal.Node_index < len(configs_file.Peers) -1 {
			next_node_to_record = latest_wal.Node_index + 1
		}
	}

	//it.Index_WAL
	for _ , index_row := range configs_file.Peers{
		if index_row.Name !=  configs_file.Instance_name{
			response, err := http.Post("http://" +  index_row.Ip + ":" + index_row.Port + "/" + index_row.Name +  "/update_wal", "application/json", r.Body)
			if err != nil {
				//log.Fatal(err)
				fmt.Println("err::::::")
				fmt.Println(err)
				fmt.Println(wal)
				if next_node_to_record == current_node_check{
					if next_node_to_record < len(configs_file.Peers) -1 {
						next_node_to_record ++
					}else{
						fmt.Println("errActiveNodes::::::")
						fmt.Println(alive_nodes)
						next_node_to_record = alive_nodes[0]

					}
				}
			}else{
				alive_nodes = append(alive_nodes, current_node_check)
			}
			fmt.Println(response)
		}else{
			alive_nodes = append(alive_nodes, current_node_check)
		}
		current_node_check ++
	}
	
	

	wo.Node_index = next_node_to_record
	wal = append(wal, wo)



	fmt.Println("WAL::::::") 
	fmt.Println(wal)
	

	//index_row := it.Index_rows[len(it.Index_rows)-1]
	fmt.Fprintf(w, strconv.Itoa(next_node_to_record))
}



func update_wal(w http.ResponseWriter, r *http.Request) {
	

	//insert_count := r.URL.Query().Get("insert_count")

	//get body with the mem_row

	fmt.Println("update_wal::::::") 

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	
	var wo wal_operation

    var p []mem_row
	err := dec.Decode(&p)
	if err != nil {
		log.Fatal(err)
	}
	wo.Rows = p 
	fmt.Println("update_wal::::::") 

	next_node_to_record := 0

	if len(wal) > 0{
		latest_wal := wal[len(wal)-1]
		if latest_wal.Node_index < len(it.Index_rows) -1 {
			next_node_to_record = latest_wal.Node_index + 1
		}
	}

	wo.Node_index = next_node_to_record
	wal = append(wal, wo)
	fmt.Println("WAL::::::") 
	fmt.Println(wal)
	

	//index_row := it.Index_rows[len(it.Index_rows)-1]
	fmt.Fprintf(w, strconv.Itoa(next_node_to_record))
}