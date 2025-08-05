package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	// "errors"
	"encoding/json"
	// "io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bcosso/rsocket_json_requests"
	"github.com/bcosso/sqlparserproject"
	"github.com/gorilla/mux"
)

type config struct {
	Folder_Path     string                  `json:"path"`
	Peers           []peers                 `json:"peers"`
	Number_Replicas string                  `json:"number_replicas"`
	Max_Heap_Size   string                  `json:"max_heap_size"`
	Instance_ip     string                  `json:"instance_ip"`
	Instance_name   string                  `json:"instance_name"`
	Instance_Port   string                  `json:"instance_port"`
	Sync_Port       string                  `json:"sync_port"`
	Data_Interval   string                  `json:"data_interval"`
	Wal_Interval    string                  `json:"wal_interval"`
	Wal_Limit       string                  `json:"wal_limit"`
	Index           map[string][]TableIndex `json:"index_definition"`
	//RANGE, ALPHABETICAL, TABLE
	Sharding_type     string                   `json:sharding_type`
	Sharding_column   string                   `json:sharding_column`
	Sharding_groups   []ShardGroup             `json:sharding_groups`
	Sharding_strategy map[string]ShardStrategy `json:sharding_strategy`
}

type TableIndex struct {
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
	//HASH, btree
	IndexType string `json:"index_type"`
}

type peers struct {
	Ip       string  `json:"ip"`
	Name     string  `json:"name"`
	Port     string  `json:"port"`
	Replicas []peers `json:"replicas"`
}

type ShardStrategy struct {
	//RANGE, ALPHABETICAL, TABLE
	From              string `json:"from"`
	To                string `json:"to"`
	Sharding_group_id int    `json:"sharding_group_id"`
}

type ShardGroup struct {
	Sharding_group_id int   `json:"sharding_group_id"`
	Replicas          []int `json:"replicas"`
}

type index_table struct {
	Index_id           int         `json:"index_id"`
	Key_id             int         `json:"key_id"`
	Index_WAL          int         `json:"index_wal"`
	Latest_Node_Insert int         `json:"latest_node_isert"`
	Index_rows         []index_row `json:"index_row"`
}

type index_row struct {
	Index_from    int    `json:"index_from"`
	Index_to      int    `json:"index_to"`
	Current_index int    `json:"Current_index"`
	Instance_name string `json:"instance_name"`
	Instance_ip   string `json:"instance_ip"`
	Instance_port string `json:"instance_port"`
	Table_name    string `json:"table_name"`
}

type mem_table struct {
	Key_id int       `json:"key_id"`
	Rows   []mem_row `json:"mem_row"`
}

type mem_row struct {
	Key_id     int    `json:"key_id"`
	Table_name string `json:"table_name"`
	//Document interface{} `json:"document"`
	Parsed_Document map[string]interface{}
}

type wal_file struct {
	Key_id int             `json:"key_id"`
	Rows   []wal_operation `json:"wal_operation"`
}

type wal_operation struct {
	Key_id            int               `json:"key_id"`
	Node_name         string            `json:"node_name"`
	Operation_type    string            `json:"operation_type"`
	Query             string            `json:"query"`
	Node_index        int               `json:"node_index"`
	InstancesToUpdate []ActiveInstances `json:"active_instances"`
	Rows              []mem_row         `json:"mem_row"`
	Status            bool              `json:"status"`
}
type ActiveInstances struct {
	InstanceName string `json:"instance_name"`
	Status       bool   `json:"status"`
}

type SingletonWal struct {
	wal map[string]wal_operation
	mu  sync.RWMutex
}

type SingletonIndex struct {
	mt map[string][]*mem_row
	//		       table     column     value
	hashIndex  map[string]map[string]map[string]*mem_row
	btreeIndex map[string]map[string][]*mem_row
	mu         sync.RWMutex
}

type SingletonTable struct {
	mt map[string][]*mem_row
	//		       table     column     value
	hashIndex map[string]map[string]map[string]*mem_row
	mu        sync.RWMutex
}

var it index_table = getIndexTable()
var configs_file config

func loadMemTableRsocket(payload interface{}) interface{} {
	getMemTable()
	return payload
}

func syncSendData(nodeName string) {
	conn, err := net.Dial("tcp", nodeName+":"+configs_file.Sync_Port)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	file, err := os.Open("mem_table.json")
	defer file.Close()
	io.Copy(conn, file)

	// file2, err := os.Open("wal_file.json")
	// defer file2.Close()
	// io.Copy(conn, file2)

	// file3, err := os.Open("index_table.json")
	// defer file3.Close()
	// io.Copy(conn, file3)
}

// Updates cluster configuration on the fly
func updateConfiguration(payload interface{}) interface{} {
	operationInterface, err := GetAttributeFromPayload("operation", payload)
	if err != nil {
		fmt.Println("*******************")
		fmt.Println(payload)
		fmt.Println(err)
		return "Error"
	}
	operation := operationInterface.(string)
	switch operation {
	case "add_node":
		nodeInterface, err := GetAttributeFromPayload("node", payload)
		if err != nil {
			fmt.Println("*******************")
			fmt.Println(payload)
			fmt.Println(err)
			return "Error"
		}
		var node peers
		bytesInt, _ := json.Marshal(nodeInterface)
		json.Unmarshal(bytesInt, &node)
		configs_file.Peers = append(configs_file.Peers, node)
		//save config file to disk
		syncSendData(node.Ip)
		dumpConfig("------------------------------Config File Updated---------------------------------")
		break
	default:
		break
	}

	return payload
}

func handleRequests(configs *config) {

	myRouter := mux.NewRouter().StrictSlash(true)
	// myRouter.HandleFunc("/"+configs.Instance_name+"/get_all", get_all)
	// myRouter.HandleFunc("/"+configs.Instance_name+"/get_rows", get_rows)
	// myRouter.HandleFunc("/"+configs.Instance_name+"/get_range", get_range)
	// myRouter.HandleFunc("/"+configs.Instance_name+"/delete_data_where", delete_data_where)
	// myRouter.HandleFunc("/"+configs.Instance_name+"/delete_data_where_worker_contains", delete_data_where_worker_contains)

	log.Fatal(http.ListenAndServe(":"+configs.Instance_Port, myRouter))
}

func handleRequestsRsocket(configs *config) {

	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/load_mem_table", loadMemTableRsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/read_wal", readWalRsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/update_configuration", updateConfiguration)
	// rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/update_wal", update_wal_rsocket)
	// rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/insert", insert_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/insert_worker", insertWorker)
	// rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/insert_worker_rsocket", insert_worker_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/select_data", selectDataRsocket)

	// Temporarely commented
	// rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/select_data_where_worker_equals", select_data_where_worker_equals_rsocket)
	// rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/select_data_where_worker_contains", select_data_where_worker_contains_rsocket)
	// rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/delete_data_where", delete_data_where_rsocket)
	// rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/delete_data_where_worker_contains", delete_data_where_worker_contains_rsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/select_table", selectTable)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/execute_query", executeQuery)

	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/select_data_where_worker_equals_rsocket", selectDataWhereWorkerEquals)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/select_data_where_worker_between_rsocket", selectDataWhereWorkerBetween)

	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/insert_data", insertData)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/read_wal_strategy", readWalStrategyRsocket)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/update_wal_new", UpdateWal)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/update_successful_nodes_wal", UpdateSuccessfulNodesWal)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/trigger_recover_data_nodes", TriggerRecoverDataInNodes)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/get_server_free_memory", getServerFreeMemory)
	rsocket_json_requests.AppendFunctionHandler("/"+configs.Instance_name+"/update_wal_only", UpdateWalOnly)

	fmt.Println(configs.Instance_Port)

	//	rsocket_json_requests.SetTLSConfig("cert.pem", "key.pem")

	_port, _ := strconv.Atoi(configs.Instance_Port)
	rsocket_json_requests.RequestConfigsServer(_port)
	rsocket_json_requests.ServeCalls()
}

func syncMode() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	listener, err := net.Listen("tcp", ":"+configs_file.Sync_Port)
	if err != nil {
		log.Fatal(err)
	}
	// fileCounter := 0
	fileName := "mem_table.json"
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Print(err)

			continue
		} else {
			handleConn(conn, fileName)
			break
			// fileCounter++
			// if fileCounter > 3 {
			// 	break
			// }
			// switch fileCounter {
			// case 1:
			// 	fileName = "mem_table.json"
			// 	break
			// // case 2:
			// // 	fileName = "wal_file.json"
			// // 	break
			// // case 3:
			// // 	fileName = "index_table.json"
			// // 	break

			// }

		}

	}
}

func handleConn(c net.Conn, fileName string) {
	input := bufio.NewScanner(c)
	var file []byte
	for input.Scan() {
		log.Println(input.Text())
		file = append(file, input.Bytes()...)

		// echo(c, input.Text(), 1*time.Second)
	}
	if len(file) > 0 {
		dumpGeneric(fileName, file)
	}
	c.Close()
}

func echo(c net.Conn, shout string, delay time.Duration) {
	fmt.Fprintln(c, "\t", strings.ToUpper(shout))
	time.Sleep(delay)
	fmt.Fprintln(c, "\t", shout)
	time.Sleep(delay)
	fmt.Fprintln(c, "\t", strings.ToLower(shout))
}

// Core Methods
func main() {
	configfile, err := os.Open("configfile.json")
	if err != nil {
		fmt.Println("Error on config file found or config file not existent")
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	if err != nil {
		fmt.Println("Here Read Config")
		log.Fatal(err)
	}

	err = json.Unmarshal(root, &configs_file)
	if err != nil {
		fmt.Println("unmarshal of config file failed")
		log.Fatal(err)
	}

	if len(os.Args) > 1 {
		fmt.Println("SYNC MODE")
		syncMode()
	}
	if configs_file.Wal_Limit != "" {
		wal_limit, _ = strconv.Atoi(configs_file.Wal_Limit)
		wal_limit = wal_limit * 1024 * 1024
	}
	if configs_file.Data_Interval != "" {
		interval, _ := strconv.Atoi(configs_file.Data_Interval)
		dur := time.Duration(interval)
		data_interval = dur * time.Millisecond
	}
	if configs_file.Wal_Interval != "" {
		interval, _ := strconv.Atoi(configs_file.Wal_Interval)
		dur := time.Duration(interval)
		wal_interval = dur * time.Millisecond
	}

	getWalDisk()
	getMemTable()
	singletonTable.AttachAllIndexesToData()
	singletonIndex.AttachAllIndexes()
	go singleton.dumpWal("------------------------------WAL---------------------------------")
	go dump_data("------------------------------Data---------------------------------")

	handleRequestsRsocket(&configs_file)
}

func getIndexTable() index_table {
	configfile, err := os.Open("index_table.json")
	if err != nil {
		fmt.Println("Index Table")
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	var _it index_table
	_it.Index_rows = make([]index_row, 0)
	if err := json.Unmarshal([]byte(string(root)), &_it); err != nil {
		fmt.Println("Index Table Unmarshal")
		log.Fatal(err)
	}
	return _it
}

// Param: mem_table_from_to
func getMemTable() {
	configfile, err := os.Open("mem_table.json")
	if err != nil {
		fmt.Println("Mem_table")
		fmt.Println(err)
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	singletonTable.UnmarshalMT([]byte(root))

}

func GetParsedDocumentToMemRow(payload interface{}) (mem_row, interface{}) {

	payload_content := make(map[string]interface{})
	var myString string
	if reflect.TypeOf(myString) == reflect.TypeOf(payload) {
		myString = payload.(string)
		json.Unmarshal([]byte(myString), &payload_content)
	} else if reflect.TypeOf(payload_content) == reflect.TypeOf(payload) {
		payload_content = payload.(map[string]interface{})
	}

	var ii []byte

	var p []mem_row
	intermediate_inteface := payload_content["body"]
	if intermediate_inteface == nil {
		intermediate_inteface = payload
	}

	if reflect.TypeOf(myString) == reflect.TypeOf(intermediate_inteface) {

		ii = []byte(intermediate_inteface.(string))
		err := json.Unmarshal(ii, &p)
		if err != nil {
			fmt.Println(err)
		}

	} else if reflect.TypeOf(payload_content) == reflect.TypeOf(intermediate_inteface) {
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
	} else {
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

func GetInstanceList(payload interface{}) []peers {
	var result []peers
	payload_content := make(map[string]interface{})
	var myString string
	if reflect.TypeOf(myString) == reflect.TypeOf(payload) {
		myString = payload.(string)
		json.Unmarshal([]byte(myString), &payload_content)
	} else if reflect.TypeOf(payload_content) == reflect.TypeOf(payload) {
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

func GetPeerByInstanceName(instanceName string) peers {
	var emptyPeer peers
	for _, peer := range configs_file.Peers {
		if peer.Name == instanceName {
			return peer
		}
	}
	return emptyPeer
}

func GetAttributeFromPayload(attribute string, payload interface{}) (interface{}, error) {

	payload_content := make(map[string]interface{})
	var myString string
	if reflect.TypeOf(myString) == reflect.TypeOf(payload) {
		myString = payload.(string)
		json.Unmarshal([]byte(myString), &payload_content)
	} else if reflect.TypeOf(payload_content) == reflect.TypeOf(payload) {
		payload_content = payload.(map[string]interface{})
	} else {
		newError := fmt.Errorf("not a map[string]")
		return nil, newError
	}

	result, err := payload_content[attribute]
	if err != true {
		newError := fmt.Errorf("Not found in the map")
		return nil, newError
	}
	return result, nil

}

type TypeContract struct {
	NameType     string         `json:"nametype"`
	ChildrenType []TypeContract `json:"childtype"`
}

func ParseObjectTypeToContract(object interface{}, objectType *TypeContract) interface{} {
	// , objectType * TypeContract

	// var contract TypeContract

	// objectMap := object.(map[string] interface{})
	// for k, v := range m {
	// 	fmt.Printf("key[%s] value[%s]\n", k, v)
	// }
	v := reflect.ValueOf(object)

	fmt.Println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	fmt.Println("Debug ParseObjectTypeToContract")
	fmt.Println(v)
	fmt.Println(v.Interface())
	fmt.Println(reflect.TypeOf(v.Interface()).Name())
	fmt.Println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	(*objectType).NameType = reflect.TypeOf(v.Interface()).String()

	if v.Kind() == reflect.Slice {
		fmt.Println("Size of slice:")
		fmt.Println(v.Len())
		for i := 0; i < v.Len(); i++ {
			var innerObjectType TypeContract
			object = ParseObjectTypeToContract(v.Index(i).Interface(), &innerObjectType)
			(*objectType).ChildrenType = append((*objectType).ChildrenType, innerObjectType)

		}
	} else if v.Kind() == reflect.Struct {
		values := make([]interface{}, v.NumField())

		for i := 0; i < v.NumField(); i++ {
			values[i] = v.Field(i).Interface()
			vValue := reflect.ValueOf(values[i])
			fmt.Println("KindOf- TYpeOf")
			fmt.Println(vValue.Kind())
			fmt.Println(reflect.TypeOf(values[i]))
			fmt.Println(vValue.Kind().String())
			fmt.Println(values[i] == nil)

			if values[i] != nil && reflect.TypeOf(values[i]).Name() == "ptr" {
				v.Field(i).Set(vValue)
				values[i] = v.Field(i).Interface()
				vValue = reflect.ValueOf(values[i])
			}
			if vValue.Kind() == reflect.Struct || vValue.Kind() == reflect.Slice || vValue.Kind() == reflect.Map {
				var innerObjectType TypeContract
				ParseObjectTypeToContract(values[i], &innerObjectType)
				(*objectType).ChildrenType = append((*objectType).ChildrenType, innerObjectType)
			} else {
				var innerObjectType TypeContract
				innerObjectType.NameType = vValue.Kind().String()
				(*objectType).ChildrenType = append((*objectType).ChildrenType, innerObjectType)
			}
		}
		fmt.Println(values)
	} else {

	}

	return object
}

func validateTypeName(name string) string {
	return strings.Replace(name, "main.", "", -1)
}

var typeRegistry = make(map[string]reflect.Type)

func makeInstance(name string) interface{} {
	v := reflect.New(typeRegistry[name]).Elem()
	// Maybe fill in fields here if necessary
	return v.Interface()
}

func InitTypes() {
	myTypes := []interface{}{Filter{}, SqlClause{}, Condition{}, sqlparserproject.CommandTree{}, "string", 0, 0.0, false, []Filter{}, []SqlClause{}, []Condition{}, []sqlparserproject.CommandTree{}}
	for _, v := range myTypes {
		// typeRegistry["MyString"] = reflect.TypeOf(MyString{})
		typeRegistry[fmt.Sprintf("%T", v)] = reflect.TypeOf(v)
	}
}

func ConvertTypes(object interface{}, name string) interface{} {
	newValue := makeInstance(name)
	jsonFilter, _ := json.Marshal(&object)
	err1 := json.Unmarshal([]byte(string(jsonFilter)), &newValue)
	if err1 != nil {
		panic(err1)
	}
	return newValue
}

// ///////////////////////////////////////////////////////////////////////Mutex//////////////////////////////////////////////////////////////////
var singleton SingletonWal
var singletonTable SingletonTable
var singletonIndex SingletonIndex

func (sing *SingletonWal) UnmarshalWAL(fileData []byte) {
	sing.mu.Lock()
	defer sing.mu.Unlock()
	err := json.Unmarshal(fileData, &(sing.wal))

	if err != nil {
		fmt.Println("Mutex Wal unmarshal")
		fmt.Println(err)
		log.Fatal(err)
		fmt.Println(err)
	}
}

func (sing *SingletonTable) UnmarshalMT(fileData []byte) {
	sing.mu.Lock()
	defer sing.mu.Unlock()
	err := json.Unmarshal(fileData, &(sing.mt))

	if err != nil {
		fmt.Println("Mutex MT unmarshal")
		fmt.Println(err)
		log.Fatal(err)
		fmt.Println(err)
	}
}

func (sing *SingletonWal) AddItemWAL(guid string, nodesSuccessful []peers) {
	count := 0
	sing.mu.Lock()
	defer sing.mu.Unlock()
	for countNode, node := range sing.wal[guid].InstancesToUpdate {

		for _, nodeSuccess := range nodesSuccessful {
			if node.InstanceName == nodeSuccess.Name {
				sing.wal[guid].InstancesToUpdate[countNode].Status = true
			}
		}
		if node.Status == true {
			count++
		}
	}
	if len(sing.wal[guid].InstancesToUpdate) == count {
		wo, _ := sing.wal[guid]
		wo.Status = true
		sing.wal[guid] = wo
	}
}

func (sing *SingletonWal) SetOperationWAL(guid string, wo wal_operation) {
	sing.mu.Lock()
	defer sing.mu.Unlock()
	sing.wal[guid] = wo
}

func (sing *SingletonWal) TryRecoverData() (bool, []error) {

	//try only once per recovery attempt
	var errList []error
	previousNode := make(map[string]bool)
	var successes int = 0
	var cases int = 0

	sing.mu.Lock()

	for indexWo, wo := range sing.wal {
		if wo.Status != true {

			//Level of Consistency choice : Eventual or full (WHEN SHARDING and/or REPLICATING) If a node is down, it will demand recovery, using the accumulated write ahead log
			// var wg sync.WaitGroup
			row := wo.Rows[0]
			replicationPoints := GetReplicaPointsShardingStrategy(row)

			for indexNode, node := range wo.InstancesToUpdate {
				if node.Status != true {
					val, found := previousNode[node.InstanceName]

					cases++

					if !found || (found && val) { //Re-evaluate the utility for this condition - bcosso
						previousNode[node.InstanceName] = true

						index_row := GetPeerByInstanceName(node.InstanceName)
						_port, _ := strconv.Atoi(index_row.Port)
						rsocket_json_requests.RequestConfigs(index_row.Ip, _port)
						var param interface{}
						param = map[string]interface{}{
							"key_id":         strconv.Itoa(row.Key_id),
							"table":          row.Table_name,
							"body":           row,
							"query_sql":      wo.Query,
							"operation_type": wo.Operation_type,
							"instance_list":  replicationPoints,
							"guid":           indexWo,
						}
						// param2 := make(map[string]interface{})
						// param2[indexWo] = param
						_, err := rsocket_json_requests.RequestJSON("/"+index_row.Name+"/update_wal_new", param)
						if err != nil {
							fmt.Println("err::::::")
							fmt.Println(err)
							errList = append(errList, err)
						} else {
							sing.wal[indexWo].InstancesToUpdate[indexNode].Status = true
							successes++
						}
					}
				}
			}
			if len(errList) < 1 {
				key := sing.wal[indexWo]
				key.Status = true
				sing.wal[indexWo] = key
			}
			UpdateWalWholeCluster(indexWo, sing.wal[indexWo])
		}
	}
	sing.mu.Unlock()
	return (successes == cases), errList
}

func (sing *SingletonWal) SetStatus(guid string, status bool) {
	sing.mu.Lock()
	wo := sing.wal[guid]
	wo.Status = status
	sing.wal[guid] = wo
	sing.mu.Unlock()
}

func (sing *SingletonWal) Get(guid string) wal_operation {
	sing.mu.RLock()
	defer sing.mu.RUnlock()
	return sing.wal[guid]
}

func (sing *SingletonTable) AttachAllIndexesToData() {
	for _, indexTable := range configs_file.Index {
		for _, index := range indexTable {
			sing.AttachNewIndex(index)
		}
	}
}

func (sing *SingletonTable) AttachNewIndex(index TableIndex) {
	sing.mu.Lock()
	defer sing.mu.Unlock()

	if sing.hashIndex == nil {
		sing.hashIndex = make(map[string]map[string]map[string]*mem_row)
	}

	if _, ok := sing.hashIndex[index.TableName]; !ok {
		sing.hashIndex[index.TableName] = make(map[string]map[string]*mem_row)
	}

	if _, ok := sing.hashIndex[index.TableName][index.ColumnName]; !ok {
		sing.hashIndex[index.TableName][index.ColumnName] = make(map[string]*mem_row)
	}

	for iRow, row := range sing.mt[index.TableName] {
		str := fmt.Sprintf("%v", row.Parsed_Document[index.ColumnName])
		fmt.Println("Index!!")
		sing.hashIndex[index.TableName][index.ColumnName][str] = sing.mt[index.TableName][iRow]
	}
}

func (sing *SingletonIndex) AttachAllIndexes() {
	for _, indexTable := range configs_file.Index {
		for _, index := range indexTable {
			if index.IndexType == "HASH" {
				sing.AttachNewHashIndex(index)
			} else if index.IndexType == "BTREE" {
				fmt.Println("Index!!BTREE!!")
				sing.AttachNewBTreeIndex(index)
			}
		}
	}
}

func (sing *SingletonIndex) AttachNewHashIndex(index TableIndex) {
	sing.mu.Lock()
	defer sing.mu.Unlock()

	if sing.hashIndex == nil {
		sing.hashIndex = make(map[string]map[string]map[string]*mem_row)
	}

	if _, ok := sing.hashIndex[index.TableName]; !ok {
		sing.hashIndex[index.TableName] = make(map[string]map[string]*mem_row)
	}

	if _, ok := sing.hashIndex[index.TableName][index.ColumnName]; !ok {
		sing.hashIndex[index.TableName][index.ColumnName] = make(map[string]*mem_row)
	}

	for iRow, row := range sing.mt[index.TableName] {
		str := fmt.Sprintf("%v", row.Parsed_Document[index.ColumnName])
		fmt.Println("Index!!")
		sing.hashIndex[index.TableName][index.ColumnName][str] = sing.mt[index.TableName][iRow]
	}
}

func (sing *SingletonIndex) AttachNewBTreeIndex(index TableIndex) {
	sing.mu.Lock()
	defer sing.mu.Unlock()
	if sing.btreeIndex == nil {
		sing.btreeIndex = make(map[string]map[string][]*mem_row)
	}

	if _, ok := sing.btreeIndex[index.TableName]; !ok {
		sing.btreeIndex[index.TableName] = make(map[string][]*mem_row)
	}

	fmt.Println("Index!! btree1111!!")
	for iRow, _ := range singletonTable.mt[index.TableName] {
		// str := fmt.Sprintf("%v", row.Parsed_Document[index.ColumnName])
		fmt.Println("Index!! btree!!")
		sing.btreeIndex[index.TableName][index.ColumnName] = append(sing.btreeIndex[index.TableName][index.ColumnName], singletonTable.mt[index.TableName][iRow])
	}

	sort.Slice(sing.btreeIndex[index.TableName][index.ColumnName], func(i, j int) bool {
		first := fmt.Sprintf("%v", sing.btreeIndex[index.TableName][index.ColumnName][i].Parsed_Document[index.ColumnName])
		second := fmt.Sprintf("%v", sing.btreeIndex[index.TableName][index.ColumnName][j].Parsed_Document[index.ColumnName])
		iFirst, _ := strconv.Atoi(first)
		iSecond, _ := strconv.Atoi(second)
		fmt.Println("Sort!!")
		return iFirst < iSecond
	})

}
