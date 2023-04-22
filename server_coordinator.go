package main

import (

    "fmt"
	"log"
	// "errors"	
	"os"
	//"io"
	"io/ioutil"
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
)


type config struct {
    Folder_Path   string `json:"path"`
	Peers   []peers `json:"peers"`
	Number_Replicas string `json:"number_replicas"`
	Max_Heap_Size string `json:"max_heap_size"`
	Instance_name string `json:"instance_name"`
	Instance_Port string `json:"instance_port"`
}

type peers struct {
	Ip string `json:"ip"`
	Name string `json:"name"`
	Port string `json:"port"`

}


type index_table struct {
	Index_id   int `json:"index_id"`
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
	Document interface{} `json:"document"`
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
	Rows []mem_row `json:"mem_row"`
}

var it index_table =  get_index_table()
var mt mem_table
var wal []wal_operation
var configs_file config

//Client Facing Methods //



func load_mem_table(w http.ResponseWriter, r *http.Request) {
	
	get_mem_table()
	
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
	myRouter.HandleFunc("/"+ configs.Instance_name + "/insert", insert)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/insert_worker", insert_worker) 
	myRouter.HandleFunc("/"+ configs.Instance_name + "/select_data", select_data)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/select_data_where_worker_equals", select_data_where_worker_equals)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/select_data_where_worker_contains", select_data_where_worker_contains)

	myRouter.HandleFunc("/"+ configs.Instance_name + "/delete_data_where_worker_contains", delete_data_where_worker_contains)

	

	log.Fatal(http.ListenAndServe(":"+ configs.Instance_Port, myRouter))
}

// Nimpha Facing Methods //

 

//Core Methods
func main() {
	get_wal_disk()
	//go dump_wal("------------------------------WAL---------------------------------")
	//go dump_data("------------------------------Data---------------------------------")
	configfile, err := os.Open("configfile.json")
    if err != nil {
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	
	json.Unmarshal(root, &configs_file)
	handleRequests(&configs_file)
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
	current_index_row := 0
	for _, row := range mt.Rows {
		current_document := row.Document
		fmt.Println(current_document)
		parsed_document, ok :=  current_document.(map[string] interface{})
		//err = json.Unmarshal(current_document, &parsed_document)

		if !ok{
			fmt.Println("ERROR!")
			
		}
		row.Parsed_Document = parsed_document
		fmt.Println(row.Parsed_Document["name_client"])
		mt.Rows[current_index_row].Parsed_Document = parsed_document
		current_index_row ++
	}
}

func check_index_manager(){}
func replicate_index_manager(){}
func update_index_manager(w http.ResponseWriter, r *http.Request){
    fmt.Fprintf(w, "update current index manager")
    fmt.Println("Endpoint Hit: get_rows")
}


