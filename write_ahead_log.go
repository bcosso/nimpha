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
	"rsocket_json_requests"
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

func read_wal_rsocket(payload interface{})  interface{} {
	

	fmt.Println("read_wal::::::") 
	fmt.Println(payload)

	payload_content := make(map[string]interface{})
	myString := payload.(string)
	json.Unmarshal([]byte(myString), &payload_content)

	//payload_content, _ :=  payload.(map[string] interface{})
	intermediate_inteface := payload_content["body"].(string)
	json_rows_bytes, _ := json.Marshal(intermediate_inteface)
	reader := bytes.NewReader(json_rows_bytes)
	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()
	
	fmt.Println("intermediate::::::")
	fmt.Println(intermediate_inteface) 

	var wo wal_operation
	//var p []mem_row
	var p string
	err := dec.Decode(&p)
	if err != nil {
		log.Fatal(err)
	}
	//wo.Rows = p 
	fmt.Println("read_wal::::::") 
	fmt.Println(p)
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

			// var param interface{}
			// param = map[string]interface{}{
			// 	"key_id": strconv.Itoa(result.Key_id),
			// 	"table":  result.Table_name,
			// 	"Parents": json_data,
			// }
			// fmt.Println(param)
			// if err != nil {
			// 	log.Fatal(err)
			// }
			
			_port, _ := strconv.Atoi(index_row.Port)
			rsocket_json_requests.RequestConfigs(index_row.Ip, _port)
			response, err := rsocket_json_requests.RequestJSON("/" + index_row.Name +  "/update_wal", intermediate_inteface)
			fmt.Println("BEFORE------------------")
			// if (err != nil){
			// 	fmt.Println(err)
			// }


			// response, err := http.Post("http://" +  index_row.Ip + ":" + index_row.Port + "/" + index_row.Name +  "/update_wal", "application/json", r.Body)
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
	//fmt.Fprintf(w, strconv.Itoa(next_node_to_record))

	var param interface{}
	param = map[string]interface{}{
		"body": next_node_to_record,
	}

	//index_row := it.Index_rows[len(it.Index_rows)-1]
	//fmt.Fprintf(w, strconv.Itoa(next_node_to_record))
	return param
}


func update_wal_rsocket(payload interface{})  interface{} {
	

	//insert_count := r.URL.Query().Get("insert_count")

	//get body with the mem_row

	fmt.Println("update_wal::::::") 

	payload_content, _ :=  payload.(map[string] interface{})
	intermediate_inteface := payload_content["body"].(interface{})
	json_rows_bytes, _ := json.Marshal(intermediate_inteface)
	reader := bytes.NewReader(json_rows_bytes)

	dec := json.NewDecoder(reader)
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

	var param interface{}	
	param = map[string]interface{}{
		"body": next_node_to_record,
	}

	//index_row := it.Index_rows[len(it.Index_rows)-1]
	//fmt.Fprintf(w, strconv.Itoa(next_node_to_record))
	return param
}

func get_wal_rsocket(data_post * []mem_row ) int {

	index_row := it.Index_rows[it.Index_WAL]

	fmt.Println("-------------data_post and JSON DATA ------------------------")

	fmt.Println(data_post)

	
	json_data, err := json.Marshal(data_post)

	fmt.Println(string(json_data))

    if err != nil {
        log.Fatal(err)
	}

	fmt.Println("get_wal::::::") 
	
	//response, err := http.Post("http://" +  index_row.Instance_ip + ":" + index_row.Instance_port + "/" + index_row.Instance_name +  "/read_wal", "application/json", bytes.NewBuffer(json_data))
	

	var param interface{}
	param = map[string]interface{}{
		"body": string(json_data),
	}

	jsonParam, _ := json.Marshal(param)
	fmt.Println(string(jsonParam))
    if err != nil {
        log.Fatal(err)
	}
	
	_port, _ := strconv.Atoi(index_row.Instance_port)
	rsocket_json_requests.RequestConfigs(index_row.Instance_ip, _port)
	response, err := rsocket_json_requests.RequestJSON("/" + index_row.Instance_name +  "/read_wal", string(jsonParam))


	
	if err != nil {
		log.Fatal(err)
		fmt.Println("err::::::") 
	}
	fmt.Println("res::::::") 
	fmt.Println(":::::::::") 

//	var p string
	// dec := json.NewDecoder(response.Body)
	// dec.Decode(&p)
	//b, err := ioutil.ReadAll()
	//p = string(b) 

	payload_content, _ :=  response.(map[string] interface{})
	p := payload_content["body"].(float64)
	fmt.Println("-------Response-------")
	fmt.Println(p)
	//iconv, err := strconv.Atoi(p)
	iconv := int(p)
	return iconv

	//fmt.Fprintf(w, strconv.Itoa(index_row.Current_index))
}