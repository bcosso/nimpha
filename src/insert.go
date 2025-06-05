package main

import (
	// "bytes"
	// "fmt"
	// "log"

	// "errors"
	//"io"
	"encoding/json"
	// "strconv"
	// "github.com/bcosso/rsocket_json_requests"
)

func insertData(payload interface{}) interface{} {

	payload_content := make(map[string]interface{})
	myString := payload.(string)
	json.Unmarshal([]byte(myString), &payload_content)

	// ConsistencyStrategy := ""
	// _, found := payload_content["connectionConfig"]
	// if (found){
	// 	connectionConfig := payload_content["connectionConfig"].(map[string]string)
	// 	_, found = connectionConfig["consistency"]
	// 	if (found){
	// 		ConsistencyStrategy = connectionConfig["consistency"]
	// 	}
	// }

	var result mem_row
	var coll []mem_row
	// result.Key_id = ikey_id
	result.Table_name = payload_content["table"].(string)
	intermediate_inteface := payload_content["body"].(map[string]interface{})
	result.Parsed_Document = intermediate_inteface

	//Check if IndexRow is full. Then create another and append.Otherwise, just append to the mem_table and ++ the counter.
	//The next One should be rotational list of available servers
	//create keep alive
	//aftter that, create method to update INDEX TABLES through the servers and create WRITE AHEAD LOG to be shared among the servers and order the indexes according to the request.

	coll = append(coll, result)
	//Need to add multiple sharding strategies: per table, per range and per alphabetical order.
	//Add eventual consistency and replication:
	//Replication triggered at the same time to a different node in either eventual consistency or strong consistency
	GetNextNodesToInsertAndWriteWal(&coll, "", "insert")

	return "ok"
}

func insertDataJsonBody(payload interface{}) interface{} {

	payload_content := make(map[string]interface{})
	myString := payload.(string)
	json.Unmarshal([]byte(myString), &payload_content)
	query := ""
	operationType := ""

	// ConsistencyStrategy := ""
	// _, found := payload_content["connectionConfig"]
	// if (found){
	// 	connectionConfig := payload_content["connectionConfig"].(map[string]string)
	// 	_, found = connectionConfig["consistency"]
	// 	if (found){
	// 		ConsistencyStrategy = connectionConfig["consistency"]
	// 	}
	// }

	var result mem_row
	var coll []mem_row
	// result.Key_id = ikey_id
	result.Table_name = payload_content["table"].(string)
	_, hasQuery := payload_content["query_sql"]
	if hasQuery {
		query = payload_content["query_sql"].(string)
		operationType = payload_content["operation_type"].(string)
	}

	// mapResult := make(map[string]interface{})
	// err := json.Unmarshal([]byte(payload_content["body"].(string)), &mapResult)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	intermediate_inteface := payload_content["body"].(map[string]interface{})
	result.Parsed_Document = intermediate_inteface
	coll = append(coll, result)
	GetNextNodesToInsertAndWriteWal(&coll, query, operationType)

	return "ok"
}

func insertWorker(payload interface{}) interface{} {

	p, _ := GetParsedDocumentToMemRow(payload)
	singletonTable.InsertWorker(p)
	return "Success"
}

func (sing *SingletonTable) InsertWorker(p mem_row) string {
	pointerMemRow := &p
	sing.mu.Lock()
	sing.mt[p.Table_name] = append(sing.mt[p.Table_name], pointerMemRow)
	sing.mu.Unlock()
	return "Success"
}
