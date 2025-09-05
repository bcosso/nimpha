package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"

	"time"

	"github.com/bcosso/rsocket_json_requests"
	"github.com/google/uuid"
)

func getWalDisk() {
	configfile, err := os.Open("wal_file.json")
	if err != nil {
		fmt.Println("Wal file")
		fmt.Println(err)
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	if err != nil {
		fmt.Println("Wal read")
		fmt.Println(err)
		log.Fatal(err)
		fmt.Println(err)
	}

	// err = jsonIterGlobal.Unmarshal([]byte(root), &wal)

	// if err != nil {
	// 	fmt.Println("Wal unmarshal")
	// 	fmt.Println(err)
	// 	log.Fatal(err)
	// 	fmt.Println(err)
	// }

	singleton.UnmarshalWAL([]byte(root))
}

func readWalRsocket(payload interface{}) interface{} {

	payload_content := make(map[string]interface{})
	myString := payload.(string)
	jsonIterGlobal.Unmarshal([]byte(myString), &payload_content)

	//payload_content, _ :=  payload.(map[string] interface{})
	intermediate_inteface := payload_content["body"].(string)
	json_rows_bytes, _ := jsonIterGlobal.Marshal(intermediate_inteface)
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

	for _, index_row := range configs_file.Peers {
		if index_row.Name != configs_file.Instance_name {
			_port, _ := strconv.Atoi(index_row.Port)
			rsocket_json_requests.RequestConfigs(index_row.Ip, _port)
			response, err := rsocket_json_requests.RequestJSON("/"+index_row.Name+"/update_wal_new", intermediate_inteface)
			if err != nil {
				fmt.Println("err::::::")
				fmt.Println(err)
			}
			fmt.Println(response)
		}
	}

	uuidStr := uuid.New()
	singleton.SetOperationWAL(uuidStr.String(), wo)

	var param interface{}
	param = map[string]interface{}{
		"body": "",
	}

	//index_row := it.Index_rows[len(it.Index_rows)-1]
	//fmt.Fprintf(w, strconv.Itoa(next_node_to_record))
	return param
}

var indexWal int = 0

func GetNextNodesToInsertAndWriteWal(data_post *[]mem_row, query string, operation string) {

	// index_row := it.Index_rows[it.Index_WAL]

	json_data, err := jsonIterGlobal.Marshal(data_post)

	if err != nil {
		log.Fatal(err)
	}

	var param interface{}
	param = map[string]interface{}{
		"body":           string(json_data),
		"query_sql":      query,
		"operation_type": operation,
	}

	jsonParam, _ := jsonIterGlobal.Marshal(param)
	fmt.Println(string(jsonParam))
	if err != nil {
		log.Fatal(err)
	}

	counter := 0

	for true {
		counter++
		index_row := configs_file.Peers[indexWal]
		_port, _ := strconv.Atoi(index_row.Port)
		rsocket_json_requests.RequestConfigs(index_row.Ip, _port)
		_, err = rsocket_json_requests.RequestJSON("/"+index_row.Name+"/read_wal_strategy", string(jsonParam))

		if err != nil {
			if counter > len(configs_file.Peers) {
				fmt.Println("No nodes available as WAL (Write Ahead Log)")
				break
			}
			if indexWal < len(configs_file.Peers)-1 {
				indexWal++
			} else {
				indexWal = 0
			}
			fmt.Println("err::::::")
		} else {
			break
		}
	}
}

func readWalStrategyRsocket(payload interface{}) interface{} {

	fullPayload := make(map[string]interface{})

	var myString string
	if reflect.TypeOf(myString) == reflect.TypeOf(payload) {
		myString = payload.(string)
		jsonIterGlobal.Unmarshal([]byte(myString), &fullPayload)
	} else if reflect.TypeOf(fullPayload) == reflect.TypeOf(payload) {
		fullPayload = payload.(map[string]interface{})
	}

	p, intermediate_inteface := GetParsedDocumentToMemRow(fullPayload["body"])
	//Check persistence of it (to ensure up to date Index_id everywhere in the cluster)
	it.Index_id++
	p.Key_id = it.Index_id

	replicationPoints := GetReplicaPointsShardingStrategy(p)

	query := fullPayload["query_sql"].(string)
	operation := fullPayload["operation_type"].(string)
	//Level of Consistency choice : Eventual or full (WHEN SHARDING and/or REPLICATING) If a node is down, it will demand recovery, using the accumulated write ahead log
	// var wg sync.WaitGroup
	uuidStr := uuid.New()
	guid := uuidStr.String()

	var jsonStr = `
	{
	"body":%s,
	"instance_list":%s,
	"query_sql":"%s",
	"operation_type":"%s",
	"guid":"%s"
	}
	`
	jsonList, _ := jsonIterGlobal.Marshal(replicationPoints)
	jsonStr = fmt.Sprintf(jsonStr, intermediate_inteface, string(jsonList), query, operation, guid)

	// fmt.Println("::::::::::::::::::::")
	// fmt.Println(jsonStr)

	var hadError bool = false
	var successfulRow int
	var successfulNodes []peers

	for indexCount, successfulNode := range replicationPoints {
		// wg.Add(1)
		_port, _ := strconv.Atoi(successfulNode.Port)
		rsocket_json_requests.RequestConfigs(successfulNode.Ip, _port)
		_, err := rsocket_json_requests.RequestJSON("/"+successfulNode.Name+"/update_wal_new", jsonStr)

		if err != nil {
			fmt.Println("err::::::update_wal_new")
			fmt.Println(successfulNode.Port)
			fmt.Println(successfulNode.Ip)
			fmt.Println(err)
			hadError = true
		} else {
			successfulRow = indexCount
			successfulNodes = append(successfulNodes, successfulNode)
		}
	}

	jsonStr = `
	{
	"nodes":%s,
	"guid":"%s"
	}
	`
	jsonSuccesfulNodes, _ := jsonIterGlobal.Marshal(successfulNodes)
	jsonStr = fmt.Sprintf(jsonStr, string(jsonSuccesfulNodes), guid)

	for _, node := range successfulNodes {
		_port, _ := strconv.Atoi(node.Port)
		rsocket_json_requests.RequestConfigs(node.Ip, _port)
		_, err := rsocket_json_requests.RequestJSON("/"+node.Name+"/update_successful_nodes_wal", string(jsonStr))
		if err != nil {
			fmt.Println("err::::::update_successful_nodes_wal")
			fmt.Println(node.Port)
			fmt.Println(node.Ip)

			fmt.Println(err)
		}
	}

	// wg.Wait()
	if hadError {
		_port, _ := strconv.Atoi(replicationPoints[successfulRow].Port)
		rsocket_json_requests.RequestConfigs(replicationPoints[successfulRow].Ip, _port)
		_, err := rsocket_json_requests.RequestJSON("/"+replicationPoints[successfulRow].Name+"/trigger_recover_data_nodes", "")
		if err != nil {
			fmt.Println("err::::::trigger_recover_data_nodes")
			fmt.Println(replicationPoints[successfulRow].Port)
			fmt.Println(replicationPoints[successfulRow].Ip)
			fmt.Println(err)
		}
	} else {
		singleton.SetStatus(guid, true)

	}
	UpdateWalWholeCluster(guid, singleton.Get(guid))

	return "Ok"
}

func GetShardingStrategy(columnValue string, tableNames []string) []peers {

	var shards []peers
	fmt.Println(shards)
	fmt.Println(tableNames)
	if columnValue != "" {

	} else {
		//TODO:
		//Sharded by table
		//for each table in Tablenames, Read from a Hashtable in which node it can reside.Don't repeat nodes in the final result.
		if len(tableNames) > 0 {
			shards, _ = GetShardingForTables(tableNames)
		}
	}

	// if (configs_file.ShardingType != ""){
	// 	for replica, iReplica := range configs_file.Sharding.Replicas{
	// 		shards = append(shards, iReplica)
	// 	}
	// 	return shards
	// }
	return shards
}
func GetReplicationNodesIncludingMyself() []peers {
	return configs_file.Peers
}

func GetReplicaPointsShardingStrategy(p mem_row) []peers {
	var replicationPoints []peers
	if strings.ToLower(configs_file.Sharding_type) == "table" {
		//TODO: GetTableName
		tableNames := []string{p.Table_name}

		replicationPoints = GetShardingStrategy("", tableNames)
	} else if configs_file.Sharding_column != "" {
		if p.Parsed_Document[configs_file.Sharding_column] != nil {
			replicationPoints = GetShardingStrategy(p.Parsed_Document[configs_file.Sharding_column].(string), nil)
		} else {
			panic("Sharding column does not exist in register.")
		}

	} else {
		replicationPoints = GetReplicationNodesIncludingMyself()
	}

	if replicationPoints == nil || len(replicationPoints) < 1 {
		replicationPoints = GetReplicationNodesIncludingMyself() // returns list of nodes where data should be shared if there is no sharding strategy, returns nil. IF nil, just replicate data to all nodes.
	}
	return replicationPoints
}

func UpdateWal(payload interface{}) interface{} {

	fmt.Println("*******************")
	fmt.Println(payload)

	row, _ := GetParsedDocumentToMemRow(payload)
	listInstances := GetInstanceList(payload)
	it.Key_id = row.Key_id
	guid := ""
	var param interface{}
	param = map[string]interface{}{
		"key_id": strconv.Itoa(row.Key_id),
		"table":  row.Table_name,
		"body":   row,
	}
	var wo wal_operation
	wo.Rows = append(wo.Rows, row)
	queryInterface, err := GetAttributeFromPayload("query_sql", payload)
	if err != nil {
		fmt.Println("*******************")
		fmt.Println(payload)
		fmt.Println(err)
	} else {
		wo.Query = queryInterface.(string)
		operationTypeInterface, _ := GetAttributeFromPayload("operation_type", payload)
		wo.Operation_type = operationTypeInterface.(string)
		guidInterface, _ := GetAttributeFromPayload("guid", payload)
		guid = guidInterface.(string)

	}

	for _, instance := range listInstances {
		instanceToUpdate := ActiveInstances{InstanceName: instance.Name}
		wo.InstancesToUpdate = append(wo.InstancesToUpdate, instanceToUpdate)
	}
	// wal[guid] = wo
	singleton.SetOperationWAL(guid, wo)
	jsonParam, _ := jsonIterGlobal.Marshal(param)

	//Check if this instance should be updated
	// if configs_file.Instance_name in  wo.InstancesToUpdate

	if wo.Operation_type == "delete" {
		//call execute_query locally instead with the query
		executeQueryDelete(wo.Query)
		// deleteWorkerOld(string(jsonParam))
	} else {
		insertWorker(string(jsonParam))
	}

	return guid
}

func CommitNodes(payload interface{}) interface{} {
	return "Ok"
}

var _dataInRecovery bool = false

// Add uuid of the specific row in WAL and leave a separate function for every Status = false rows
func TriggerRecoverDataInNodes(payload interface{}) interface{} {
	if _dataInRecovery == false {
		_dataInRecovery = true
		go ScheduleRecoverData(&_dataInRecovery)
	}

	return "Ok"
}

// func TryRecoverData() (bool, []error) {

// 	//try only once per recovery attempt
// 	var errList []error
// 	previousNode := make(map[string]bool)
// 	var successes int = 0
// 	var cases int = 0

// 	for indexWo, wo := range wal {
// 		if wo.Status != true {

// 			//Level of Consistency choice : Eventual or full (WHEN SHARDING and/or REPLICATING) If a node is down, it will demand recovery, using the accumulated write ahead log
// 			// var wg sync.WaitGroup
// 			row := wo.Rows[0]
// 			replicationPoints := GetReplicaPointsShardingStrategy(row)

// 			for indexNode, node := range wo.InstancesToUpdate {
// 				if node.Status != true {
// 					val, found := previousNode[node.InstanceName]

// 					cases++

// 					if !found || (found && val) { //Re-evaluate the utility for this condition - bcosso
// 						previousNode[node.InstanceName] = true

// 						index_row := GetPeerByInstanceName(node.InstanceName)
// 						_port, _ := strconv.Atoi(index_row.Port)
// 						rsocket_json_requests.RequestConfigs(index_row.Ip, _port)
// 						var param interface{}
// 						param = map[string]interface{}{
// 							"key_id":         strconv.Itoa(row.Key_id),
// 							"table":          row.Table_name,
// 							"body":           row,
// 							"query_sql":      wo.Query,
// 							"operation_type": wo.Operation_type,
// 							"instance_list":  replicationPoints,
// 							"guid":           indexWo,
// 						}
// 						// param2 := make(map[string]interface{})
// 						// param2[indexWo] = param
// 						_, err := rsocket_json_requests.RequestJSON("/"+index_row.Name+"/update_wal_new", param)
// 						if err != nil {
// 							fmt.Println("err::::::")
// 							fmt.Println(err)
// 							errList = append(errList, err)
// 						} else {
// 							wal[indexWo].InstancesToUpdate[indexNode].Status = true
// 							successes++
// 						}
// 					}
// 				}
// 			}
// 			if len(errList) < 1 {
// 				key := wal[indexWo]
// 				key.Status = true
// 				wal[indexWo] = key
// 			}
// 			UpdateWalWholeCluster(indexWo, wal[indexWo])
// 		}
// 	}
// 	return (successes == cases), errList
// }

func ScheduleRecoverData(dataInRecovery *bool) {
	for true {
		time.Sleep(data_interval * time.Millisecond)
		success, _ := singleton.TryRecoverData()
		if success {
			*dataInRecovery = false
			break
		}
	}
}

// add to the payload the uuid of the update in the wal
func UpdateSuccessfulNodesWal(payload interface{}) interface{} {
	nodesInterface, err := GetAttributeFromPayload("nodes", payload)
	if err != nil {
		fmt.Println("*******************")
		fmt.Println(payload)
		fmt.Println(err)
		return "Error"
	}
	var nodesSuccessful []peers
	bytesInt, _ := jsonIterGlobal.Marshal(nodesInterface)
	jsonIterGlobal.Unmarshal(bytesInt, &nodesSuccessful)
	guidInterface, err := GetAttributeFromPayload("guid", payload)
	if err != nil {
		fmt.Println("*******************")
		fmt.Println(payload)
		fmt.Println(err)
	}
	guid := guidInterface.(string)
	// count := 0
	// for countNode, node := range wal[guid].InstancesToUpdate {

	// 	for _, nodeSuccess := range nodesSuccessful {
	// 		if node.InstanceName == nodeSuccess.Name {
	// 			wal[guid].InstancesToUpdate[countNode].Status = true
	// 		}
	// 	}
	// 	if node.Status == true {
	// 		count++
	// 	}
	// }
	// if len(wal[guid].InstancesToUpdate) == count {
	// 	wo, _ := wal[guid]
	// 	wo.Status = true
	// 	wal[guid] = wo
	// }

	singleton.AddItemWAL(guid, nodesSuccessful)

	// UpdateWalWholeCluster(guid, wal[guid])
	return "Ok"
}

func UpdateWalWholeCluster(guid string, wo wal_operation) {
	for _, peerNode := range configs_file.Peers {
		var param interface{}
		param = map[string]interface{}{
			"guid": guid,
			"body": wo,
		}
		_, err := rsocket_json_requests.RequestJSON("/"+peerNode.Name+"/update_wal_only", param)
		if err != nil {
			fmt.Println("err::::::")
			fmt.Println(err)
			// result = false
		}
	}

}

func UpdateWalOnly(payload interface{}) interface{} {
	guidInterface, err := GetAttributeFromPayload("guid", payload)
	if err != nil {
		fmt.Println("*******************")
		fmt.Println(payload)
		fmt.Println(err)
	}
	guid := guidInterface.(string)
	walOperationInterface, err := GetAttributeFromPayload("body", payload)
	if err != nil {
		fmt.Println("*******************")
		fmt.Println(payload)
		fmt.Println(err)
	}
	var walOperation wal_operation
	bytesInt, _ := jsonIterGlobal.Marshal(walOperationInterface)
	jsonIterGlobal.Unmarshal(bytesInt, &walOperation)
	// wal[guid] = walOperation
	singleton.SetOperationWAL(guid, walOperation)

	return ""

}
