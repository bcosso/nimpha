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
	"github.com/gorilla/mux"
	"net/http"
	"github.com/bcosso/rsocket_json_requests"
	"reflect"
	"strconv"
)


type config struct {
    Folder_Path   string `json:"path"`
	Peers   []peers `json:"peers"`
	Number_Replicas string `json:"number_replicas"`
	Max_Heap_Size string `json:"max_heap_size"`
	Instance_ip   string `json:"instance_ip"`
	Instance_name   string `json:"instance_name"`
	Instance_Port string `json:"instance_port"`
	//RANGE, ALPHABETICAL, TABLE
	ShardingType string `json:sharding_type`
	ShardingColumn string `json:sharding_column`
	ShardingGroup []ShardGroup `json:sharding_group`
	ShardingStrategy []ShardStrategy `json:sharding_strategy`
}

type peers struct {
	Ip string `json:"ip"`
	Name string `json:"name"`
	Port string `json:"port"`
	Replicas   []peers `json:"replicas"`
}

type ShardStrategy struct {
	//RANGE, ALPHABETICAL, TABLE
	From string `json:"from"`
	To string `json:"to"`
	ShardingGroupId string `json:"sharding_group_id"`
}

type ShardGroup struct{
	ShardingGroupId string `json:"sharding_group_id"`
	Replicas   []peers `json:"replicas"`
}

type index_table struct {
	Index_id   int `json:"index_id"`
	Key_id  int `json:"key_id"`
	Index_WAL int  `json:"index_wal"`
	Latest_Node_Insert int  `json:"latest_node_isert"`
	Index_rows   []index_row `json:"index_row"`
}


type index_row struct {
	Index_from   int `json:"index_from"`
	Index_to   int `json:"index_to"`
	Current_index   int `json:"Current_index"`
	Instance_name   string `json:"instance_name"`
	Instance_ip   string `json:"instance_ip"`
	Instance_port string `json:"instance_port"`
	Table_name string `json:"table_name"`
}

type mem_table struct {
	Key_id   int `json:"key_id"`
	Rows []mem_row `json:"mem_row"`
}

type mem_row struct {
	Key_id   int `json:"key_id"`
	Table_name string `json:"table_name"`
	//Document interface{} `json:"document"`
	Parsed_Document map[string]interface{}
}

type wal_file struct {
	Key_id   int `json:"key_id"`
	Rows []wal_operation `json:"wal_operation"`
}

type wal_operation struct {
	Key_id   int `json:"key_id"`
	Node_name string `json:"node_name"`
	Node_index int `json:"node_index"`
	InstancesToUpdate []ActiveInstances `json:"active_instances"`
	Rows []mem_row `json:"mem_row"`
	Status bool `json:"status"`
}
type ActiveInstances struct {
	InstanceName string `json:"instance_name"`
	Status bool `json:"status"`
}

var it index_table =  get_index_table()
var mt mem_table
var wal []wal_operation
var configs_file config

//Client Facing Methods //



func load_mem_table(w http.ResponseWriter, r *http.Request) {
	
	get_mem_table()
	
}


func load_mem_table_rsocket(payload interface{}) interface{}{
	get_mem_table()
	return payload
}


func update_index_table(w http.ResponseWriter, r *http.Request) {
	
	//key_id := r.URL.Query().Get("key_id")
	//ikey_id, err := strconv.Atoi(key_id)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//var row index_row
	//result.Current_index = ikey_id
	//result.Document = r.URL.Query().Get("document")
	//result.Table_name = r.URL.Query().Get("table_from")
	//mt.Rows = append(mt.Rows, result)

	//fmt.Println(mt)
	//Check if IndexRow is full. Then create another and append.Otherwise, just append to the mem_table and ++ the counter.
	//The next One should be rotational list of available servers
	//create keep alive



	//fmt.Fprintf(w, "Success")
}





func handleRequests(configs *config ) {
	
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_all", get_all)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_rows", get_rows)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_range", get_range)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_slices_worker", get_slices_worker)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/update_index_manager", update_index_manager)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/load_mem_table", load_mem_table)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/read_wal", read_wal)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/update_wal", update_wal)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/select_data", select_data)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/select_data_where_worker_equals", select_data_where_worker_equals)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/select_data_where_worker_contains", select_data_where_worker_contains)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/delete_data_where", delete_data_where)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/delete_data_where_worker_contains", delete_data_where_worker_contains)

	log.Fatal(http.ListenAndServe(":"+ configs.Instance_Port, myRouter))
}



func handleRequests_rsocket(configs *config ) {
	
	// Exec("select name_client, client_number, 120 from table1 where client_number = 3 or (name_client = 'teste4' or  client_number = 2 or client_number = 5)")
	// rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/get_all", get_all_rsocket)
	// rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/get_rows", get_rows_rsocket)
	// rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/get_range", get_range_rsocket)
	// rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/get_slices_worker", get_slices_worker_rsocket)
	// rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/update_index_manager", update_index_manager)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/load_mem_table", load_mem_table_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/read_wal", read_wal_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/update_wal", update_wal_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/insert", insert_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/insert_worker", insertWorker) 
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/insert_worker_rsocket", insert_worker_rsocket) 
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/select_data", select_data_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/select_data_where_worker_equals", select_data_where_worker_equals_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/select_data_where_worker_contains", select_data_where_worker_contains_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/delete_data_where", delete_data_where_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/delete_data_where_worker_contains", delete_data_where_worker_contains_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/select_table", select_table)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/execute_query", execute_query)

	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/insert_data", insertData)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/read_wal_strategy", read_wal_strategy_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/update_wal_new", UpdateWal) 
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/update_successful_nodes_wal", UpdateSuccessfulNodesWal) 
	rsocket_json_requests.AppendFunctionHandler("/"+ configs.Instance_name + "/trigger_recover_data_nodes", TriggerRecoverDataInNodes)
	// 
	// TriggerRecoverDataInNodes



	//	rsocket_json_requests.SetTLSConfig("cert.pem", "key.pem")

	_port, _ := strconv.Atoi(configs.Instance_Port)
	rsocket_json_requests.RequestConfigsServer(_port)

	rsocket_json_requests.ServeCalls()
}


// Nimpha Facing Methods //

 

//Core Methods
func main() {
	get_wal_disk()
	get_mem_table()
	// go dump_wal("------------------------------WAL---------------------------------")
	go dump_data("------------------------------Data---------------------------------")
	configfile, err := os.Open("configfile.json")
    if err != nil {
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	
	json.Unmarshal(root, &configs_file)
	handleRequests_rsocket(&configs_file)
}

func get_index_table() index_table{
	configfile, err := os.Open("index_table.json")
    if err != nil {
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	var _it index_table
	_it.Index_rows = make([]index_row, 0)
	if err := json.Unmarshal([]byte(string(root)), &_it) ; err != nil {
        log.Fatal(err)
    }
	return _it;
}


//Param: mem_table_from_to
func get_mem_table(){
	configfile, err := os.Open("mem_table.json")
    if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	err = json.Unmarshal([]byte(root), &mt)

	if err!= nil{
		fmt.Println(err)
		log.Fatal(err)
	}
	// current_index_row := 0
	// for _, row := range mt.Rows {
	// 	current_document := row.Document
	// 	//fmt.Println(current_document)
	// 	parsed_document, ok :=  current_document.(map[string] interface{})
	// 	//err = json.Unmarshal(current_document, &parsed_document)

	// 	if !ok{
	// 		fmt.Println("ERROR!")
			
	// 	}
	// 	row.Parsed_Document = parsed_document
	// 	//fmt.Println(row.Parsed_Document["name_client"])
	// 	mt.Rows[current_index_row].Parsed_Document = parsed_document
	// 	current_index_row ++
	// }
}

func check_index_manager(){}
func replicate_index_manager(){}
func update_index_manager(w http.ResponseWriter, r *http.Request){
    fmt.Fprintf(w, "update current index manager")
    fmt.Println("Endpoint Hit: get_rows")
}


func GetParsedDocumentToMemRow(payload interface{}) (mem_row, interface{}){

	payload_content := make(map[string]interface{})
	var myString string 
	if (reflect.TypeOf(myString) == reflect.TypeOf(payload)){
		myString = payload.(string)
		json.Unmarshal([]byte(myString), &payload_content)
	}else if reflect.TypeOf(payload_content) == reflect.TypeOf(payload){
		payload_content = payload.(map[string]interface{})
	}
	
	var ii []byte

    var p []mem_row
	intermediate_inteface := payload_content["body"]
	if (intermediate_inteface == nil){
		intermediate_inteface = payload
	}

	if reflect.TypeOf(myString) == reflect.TypeOf(intermediate_inteface){
		
		ii = []byte(intermediate_inteface.(string))
		err := json.Unmarshal(ii, &p)
		if err != nil {
			fmt.Println(err)
		}
	
	}else if (reflect.TypeOf(payload_content) == reflect.TypeOf(intermediate_inteface)){
		var p_result mem_row
		json_rows_bytes, err1 := json.Marshal(intermediate_inteface.(map[string]interface{}))
		if err1 != nil {
			fmt.Println(err1)
		}

		err := json.Unmarshal(json_rows_bytes, &p_result)
		if err != nil {
			fmt.Println(err)
		}
		
		return p_result, intermediate_inteface
	}else{	
		json_rows_bytes, err1 := json.Marshal(intermediate_inteface.([]interface{}))
		if err1 != nil {
			fmt.Println(err1)
		}

		err := json.Unmarshal(json_rows_bytes, &p)
		if err != nil {
			fmt.Println(err)
		}
	}
	
	return p[0], intermediate_inteface
}


func GetInstanceList(payload interface{}) ([]peers){
	var result []peers
	payload_content := make(map[string]interface{})
	var myString string 
	if (reflect.TypeOf(myString) == reflect.TypeOf(payload)){
		myString = payload.(string)
		json.Unmarshal([]byte(myString), &payload_content)
	}else if reflect.TypeOf(payload_content) == reflect.TypeOf(payload){
		payload_content = payload.(map[string]interface{})
	}

	intermediate_inteface := payload_content["instance_list"]
	json_rows_bytes, _ := json.Marshal(intermediate_inteface)
	
	fmt.Println(intermediate_inteface)
	reader := bytes.NewReader(json_rows_bytes)

	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()	
	err := dec.Decode(&result)
	if err != nil {
		fmt.Println(err)
	}
	return result
}

func GetPeerByInstanceName(instanceName string) peers{
	var emptyPeer peers
	for _, peer := range configs_file.Peers{
		if peer.Name == instanceName{
			return peer
		}
	}
	return emptyPeer
}
