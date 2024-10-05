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
	"github.com/bcosso/rsocket_json_requests"
	"strings"
	// "sync"
	"time"
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

	//fmt.Println(data_post)

	
	json_data, err := json.Marshal(data_post)

	//fmt.Println(string(json_data))

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
	//fmt.Println(p)
	//iconv, err := strconv.Atoi(p)
	iconv := int(p)
	return iconv

	//fmt.Fprintf(w, strconv.Itoa(index_row.Current_index))
}

func GetNextNodesToInsertAndWriteWal(data_post * []mem_row ) {

	index_row := it.Index_rows[it.Index_WAL]
	json_data, err := json.Marshal(data_post)

    if err != nil {
        log.Fatal(err)
	}

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
	_, err = rsocket_json_requests.RequestJSON("/" + index_row.Instance_name +  "/read_wal_strategy", string(jsonParam))


	if err != nil {
		// log.Fatal(err)
		if (it.Index_WAL < len(it.Index_rows) -1){
			it.Index_WAL ++
		}else{
			it.Index_WAL = 0
		}
		fmt.Println("err::::::") 
	}
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////
func read_wal_strategy_rsocket(payload interface{})  interface{} {
	
	p, intermediate_inteface := GetParsedDocumentToMemRow(payload)
	//Check persistence of it (to ensure up to date Index_id everywhere in the cluster)
	it.Index_id ++
	p.Key_id = it.Index_id
	fmt.Println(p)
	var replicationPoints [] peers

	
	if strings.ToLower(configs_file.Sharding_type) == "table"{
		//TODO: GetTableName
		var tableNames []string
		replicationPoints = GetShardingStrategy("", tableNames)
	}else if configs_file.Sharding_column != ""{
		if p.Parsed_Document[configs_file.Sharding_column] != nil{
			replicationPoints = GetShardingStrategy(p.Parsed_Document[configs_file.Sharding_column].(string), nil)
		}else{
			panic("Sharding column does not exist in register.")
		}

	}else{
		replicationPoints = GetReplicationNodesIncludingMyself()
	}

	if (replicationPoints == nil || len(replicationPoints) < 1){ 
		replicationPoints = GetReplicationNodesIncludingMyself()// returns list of nodes where data should be shared if there is no sharding strategy, returns nil. IF nil, just replicate data to all nodes. 
	}

	//Level of Consistency choice : Eventual or full (WHEN SHARDING and/or REPLICATING) If a node is down, it will demand recovery, using the accumulated write ahead log
	// var wg sync.WaitGroup
	var jsonStr = `
	{
	"body":%s,
	"instance_list":%s
	}
	`
	jsonList, _ := json.Marshal(replicationPoints) 
	jsonStr = fmt.Sprintf(jsonStr,intermediate_inteface,string(jsonList))

	var hadError bool = false
	var successfulRow int
	var successfulRows []peers

	for indexCount, index_row := range replicationPoints{	
		// wg.Add(1)		
		_port, _ := strconv.Atoi(index_row.Port)
		rsocket_json_requests.RequestConfigs(index_row.Ip, _port)
		_, err := rsocket_json_requests.RequestJSON("/" + index_row.Name +  "/update_wal_new", jsonStr)

		if err != nil {
			fmt.Println("err::::::")
			fmt.Println(err)
			hadError = true
		}else{
			successfulRow = indexCount
			successfulRows = append(successfulRows, index_row)
		}
	}

	jsonSuccesfulRows, _ := json.Marshal(successfulRows)
	for _, row := range successfulRows{
		_port, _ := strconv.Atoi(row.Port)
		rsocket_json_requests.RequestConfigs(row.Ip, _port)
		_, err := rsocket_json_requests.RequestJSON("/" + row.Name +  "/update_successful_nodes_wal", string(jsonSuccesfulRows))
		if err != nil {
			fmt.Println("err::::::")
			fmt.Println(err)
		}
	}

	// wg.Wait()
	if (hadError){
		_port, _ := strconv.Atoi(replicationPoints[successfulRow].Port)
		rsocket_json_requests.RequestConfigs(replicationPoints[successfulRow].Ip, _port)
		_, err := rsocket_json_requests.RequestJSON("/" + replicationPoints[successfulRow].Name +  "/trigger_recover_data_nodes", "")
		if err != nil {
			fmt.Println("err::::::")
			fmt.Println(err)
		}
	}

	return "Ok"
}

func GetShardingStrategy(columnValue string, tableNames [] string) []peers{

	var shards []peers
	fmt.Println(shards)
	fmt.Println(tableNames)
	if columnValue != ""{

	}else{
		//TODO:
		//Sharded by table
		//for each table in Tablenames, Read from a Hashtable in which node it can reside.Don't repeat nodes in the final result.
	}

	// if (configs_file.ShardingType != ""){
	// 	for replica, iReplica := range configs_file.Sharding.Replicas{
	// 		shards = append(shards, iReplica)
	// 	}
	// 	return shards 
	// }
	return nil
}
func GetReplicationNodesIncludingMyself()[]peers{
	return configs_file.Peers
}

func UpdateWal(payload interface{}) interface{}{

	row, _ := GetParsedDocumentToMemRow(payload)
	listInstances := GetInstanceList(payload)
	it.Key_id = row.Key_id

	var param interface{}
	param = map[string]interface{}{
		"key_id": strconv.Itoa(row.Key_id),
		"table":  row.Table_name,
		"body": row,
	}
	var wo wal_operation
	wo.Rows = append(wo.Rows, row)
	
	for _, instance := range listInstances{
		instanceToUpdate := ActiveInstances{InstanceName: instance.Name}
		wo.InstancesToUpdate = append(wo.InstancesToUpdate, instanceToUpdate)
	}

	wal = append(wal, wo)

	jsonParam, _ := json.Marshal(param)
	insertWorker(string(jsonParam))

	return "Ok"
}


func CommitNodes(payload interface{}) interface{}{
	return "Ok"
}

var _dataInRecovery bool = false
func TriggerRecoverDataInNodes(payload interface{}) interface{}{
	if _dataInRecovery == false{
		_dataInRecovery = true
		go ScheduleRecoverData(&_dataInRecovery)
	}
	
	return "Ok"
}

func TryRecoverData() (bool, []error){

	//try only once per recovery attempt
	var errList []error 
	previousNode := make(map[string]bool)
	// var result bool = false
	var successes int = 0
	var cases int = 0

	for _, wo := range wal{
		if wo.Status != true{
			row := wo.Rows[0]
			for _, node := range wo.InstancesToUpdate{
				if node.Status != true{
					val, found := previousNode[node.InstanceName]

					cases ++
					if (!found || (found && val)){
						previousNode[node.InstanceName] = true
						
						index_row := GetPeerByInstanceName(node.InstanceName)
						_port, _ := strconv.Atoi(index_row.Port)
						rsocket_json_requests.RequestConfigs(index_row.Ip, _port)
						var param interface{}
						param = map[string]interface{}{
							"key_id": strconv.Itoa(row.Key_id),
							"table":  row.Table_name,
							"body": row,
						}
						_, err := rsocket_json_requests.RequestJSON("/" + index_row.Name +  "/update_wal_new", param)
						if err != nil {
							fmt.Println("err::::::")
							fmt.Println(err)
							errList = append(errList, err)
							// result = false
						}else{
							successes ++
						}
					}
				}
			}
		}
	} 
	return (successes == cases), errList
}

func ScheduleRecoverData(dataInRecovery * bool){
	for (true){
		time.Sleep(data_interval * time.Millisecond)
		success, _ := TryRecoverData()
		if (success){
			*dataInRecovery = false
			break
		}
	}
}

func UpdateSuccessfulNodesWal(payload interface{}) interface{}{
	nodesSuccessfulString := payload.(string)
	var nodesSuccessful []peers
	json.Unmarshal([]byte(nodesSuccessfulString), &nodesSuccessful)
	count := 0
	for countNode, node := range wal[len(wal) -1].InstancesToUpdate {
		
		for _, nodeSuccess := range nodesSuccessful{
			if node.InstanceName == nodeSuccess.Name{
				wal[len(wal) -1].InstancesToUpdate[countNode].Status = true
			}
		}
		if node.Status == true {
			count ++
		}
	}
	if len(wal[len(wal) -1].InstancesToUpdate) == count{
		wal[len(wal) -1].Status = true
	}
	return "Ok"
}
