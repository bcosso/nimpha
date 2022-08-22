package main

import (
    "fmt"
	"log"
	// "errors"	
	"os"
	"io/ioutil"
	"encoding/json"
	"strconv"

	"net/http"
	"github.com/gorilla/mux"
	// "strings"
)


type config struct {
    Folder_Path   string `json:"path"`
	Peers   peers `json:"peers"`
	Number_Replicas string `json:"number_replicas"`
	Max_Heap_Size string `json:"max_heap_size"`
	Instance_name string `json:"instance_name"`
	Instance_Port string `json:"instance_port"`
}

type peers struct {
	ip string `json:"ip"`
	name string `json:"name"`
}


type index_table struct {
	Index_id   int `json:"index_id"`
	Index_rows   []index_row `json:"index_row"`
}


type index_row struct {
	Index_from   int `json:"index_from"`
	Index_to   int `json:"index_to"`
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
	Document string `json:"document"`
}

var it index_table =  get_index_table()
var mt mem_table

//Client Facing Methods //
func homePage(w http.ResponseWriter, r *http.Request){
    fmt.Fprintf(w, "Welcome to the HomePage!")
    fmt.Println("Endpoint Hit: homePage")
}


func get_all(w http.ResponseWriter, r *http.Request){
    fmt.Fprintf(w, "retrieve all data from table")
    fmt.Println("Endpoint Hit: get_all")
}

func get_rows(w http.ResponseWriter, r *http.Request){
    fmt.Fprintf(w, "retrieve rows from table")
	fmt.Println("Endpoint Hit: get_rows")
	
	vars := mux.Vars(r)
	key := vars["id"]
	operation := vars["op"]
	table_from := vars["table_from"]
	i, err := strconv.Atoi(key)
	if err != nil {
		log.Fatal(err)
	}

	get_rows_op(i, operation, table_from)
	
}


func get_range(w http.ResponseWriter, r *http.Request){
	fromStr := r.URL.Query().Get("from")

	fmt.Println(" FROM: " + fromStr)

	toStr := r.URL.Query().Get("to")
	fmt.Println(" TO: " + toStr)
	table_from := r.URL.Query().Get("table_from")

	from, err := strconv.Atoi(fromStr)
	if err != nil {
		log.Fatal(err)
	}
	to, err := strconv.Atoi(toStr)
	if err != nil {
		log.Fatal(err)
	}
	
	var result []mem_row
	for _, node := range it.Index_rows {
		if node.Table_name == table_from && 
		((node.Index_to >= to && to >= node.Index_from) ||  (node.Index_to >= from && from >= node.Index_from)) {
			from_method := get_slices(from, to, node)
			result = append(result, from_method...) 
			//make it async and combine the results later 
		}
	}
	json_rows_bytes, _ := json.Marshal(result)
	fmt.Fprintf(w, string(json_rows_bytes))


}

func get_rows_op(key int, operation string, table_from string){
	switch (operation){
		case "eq":
			get_rows_equals_to(key, table_from)
			break
		case "gt":
			get_rows_greater_than(key)
			break
		case "st":
			get_rows_smaller_than(key)
			break
	}
}

func get_rows_equals_to(key int, table_from string) []mem_row {

	//get which node I am
	//check range for the current node
	//chech it if this node can satisfy	this clause		

	var result []mem_row
	for _, node := range it.Index_rows {
		if node.Table_name == table_from && node.Index_to <= key && node.Index_from >= key {
			result = append(get_slices(key, key, node)) 
			//make it async and combine the results later 
		}
	}
	return result;

}

func get_rows_greater_than(key int) []mem_row{
	var result []mem_row
	for _, node := range it.Index_rows {
		if node.Index_to >= key && node.Index_from <= key {
			result = append(get_slices(key, node.Index_to, node)) 
			//make it async and combine the results later 
		}else if node.Index_from >= key {
			result = append(get_slices(node.Index_from, node.Index_to, node))
		}
	}
	return result;
}

func get_rows_smaller_than(key int) []mem_row{
	var result []mem_row
	for _, node := range it.Index_rows {
		if node.Index_to >= key && node.Index_from <= key {
			result = append(get_slices(node.Index_from, key, node)) 
			//make it async and combine the results later 
		}else if node.Index_to <= key {
			result = append(get_slices(node.Index_from, node.Index_to, node))
		}
	}
	return result;
}


func get_slices(from int, to int, ir index_row) []mem_row{
	//if (len(mt.rows) > 
	var rows []mem_row
	response, err := http.Get("http://" +  ir.Instance_ip + ":" + ir.Instance_port + "/" + ir.Instance_name +  "/get_slices_worker?from="+ strconv.Itoa(from) + "&to=" + strconv.Itoa(to) + "&table_from=" + ir.Table_name)
	if err != nil {
		log.Fatal(err)
	}


	dec := json.NewDecoder(response.Body)
    dec.DisallowUnknownFields()

    err = dec.Decode(&rows)
    if err != nil {
		log.Fatal(err)
    }

	return rows
}

func get_slices_worker(w http.ResponseWriter, r *http.Request) {
	

	fromStr := r.URL.Query().Get("from")

	fmt.Println(" FROM: " + fromStr)

	toStr := r.URL.Query().Get("to")
	fmt.Println(" TO: " + toStr)
	table_from := r.URL.Query().Get("table_from")

	from, err := strconv.Atoi(fromStr)
	if err != nil {
		log.Fatal(err)
	}
	to, err := strconv.Atoi(toStr)
	if err != nil {
		log.Fatal(err)
	}

	var rows_result []mem_row
	for _, row := range mt.Rows {
		if row.Key_id >= from && row.Key_id <= to && row.Table_name == table_from{
			rows_result = append(rows_result, row)
		}  
	}

	json_rows_bytes, _ := json.Marshal(rows_result)
	fmt.Fprintf(w, string(json_rows_bytes))
}


func load_mem_table(w http.ResponseWriter, r *http.Request) {
	
	get_mem_table()
	
}


func handleRequests(configs *config ) {

	myRouter := mux.NewRouter().StrictSlash(true)
	//myRouter.HandleFunc("/"+ configs.Instance_name + "/", homePage)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_all", get_all)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_rows", get_rows)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_range", get_range)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_slices_worker", get_slices_worker)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/update_index_manager", update_index_manager)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/load_mem_table", load_mem_table)


	log.Fatal(http.ListenAndServe(":"+ configs.Instance_Port, myRouter))
}

// Nimpha Facing Methods //

 

//Core Methods
func main() {
	configfile, err := os.Open("configfile.json")
    if err != nil {
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	var configs config
	json.Unmarshal(root, &configs)
	handleRequests(&configs)
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
	
	fmt.Println("IndexTable:::::::get_index_table")
	fmt.Println(_it)
	fmt.Println("Pointer:::::::get_index_table")
	fmt.Println(&_it)
	fmt.Println("JSON:::::::get_index_table")
	fmt.Println(string(root))
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
	err = json.Unmarshal(root, &mt)
	if err!= nil{
		fmt.Println(err)
		log.Fatal(err)
		fmt.Println(err)
	}

	fmt.Println("MemoryTable:::::::get_mem_table")
	//fmt.Println(mt)
	fmt.Println("Pointer:::::::get_mem_table")
	//fmt.Println(&mt)
	fmt.Println("JSON:::::::get_mem_table")
	//fmt.Println(root)
}

func check_index_manager(){}
func replicate_index_manager(){}
func update_index_manager(w http.ResponseWriter, r *http.Request){
    fmt.Fprintf(w, "update current index manager")
    fmt.Println("Endpoint Hit: get_rows")
}


