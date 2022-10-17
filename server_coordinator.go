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
	"github.com/gorilla/mux"
	"strings"
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

//Client Facing Methods //

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


func select_data(w http.ResponseWriter, r *http.Request){
	//if (len(mt.rows) > 
	var rows []mem_row
	
	// table_name := r.URL.Query().Get("table")
	// where_field := r.URL.Query().Get("where_field")
	// where_content := r.URL.Query().Get("where_content")
	// where_operator := r.URL.Query().Get("where_operator")

	for _, ir := range it.Index_rows {
		response, err := http.Get("http://" +  ir.Instance_ip + ":" + ir.Instance_port + "/" + ir.Instance_name +  "/get_slices_worker?from=")
		if err != nil {
			log.Fatal(err)
		}


		dec := json.NewDecoder(response.Body)
		dec.DisallowUnknownFields()

		err = dec.Decode(&rows)
		if err != nil {
			log.Fatal(err)
		}
	}

}

func select_data_where_worker_equals(w http.ResponseWriter, r *http.Request) {
	

	table_name := r.URL.Query().Get("table")
	where_field := r.URL.Query().Get("where_field")
	where_content := r.URL.Query().Get("where_content")
	//where_operator := r.URL.Query().Get("where_operator") // Method only for = operator. Another one will be created for contains, bigger than and smaller than


	var rows_result []mem_row
	for _, row := range mt.Rows {
		if row.Table_name == table_name{
			if row.Parsed_Document[where_field] == where_content{
				rows_result = append(rows_result, row)
			}
		}
			//var result_document 
		// Unmarshal or Decode the JSON to the interface.
	}

	json_rows_bytes, _ := json.Marshal(rows_result)
	fmt.Fprintf(w, string(json_rows_bytes))
}

func select_data_where_worker_contains(w http.ResponseWriter, r *http.Request) {
	

	table_name := r.URL.Query().Get("table")
	where_field := r.URL.Query().Get("where_field")
	where_content := r.URL.Query().Get("where_content")
	//where_operator := r.URL.Query().Get("where_operator") // Method only for = operator. Another one will be created for contains, bigger than and smaller than


	var rows_result []mem_row
	for _, row := range mt.Rows {
		if row.Table_name == table_name{
			if strings.Contains(row.Parsed_Document[where_field].(string), where_content){
				rows_result = append(rows_result, row)
			}
		}
			//var result_document 
		// Unmarshal or Decode the JSON to the interface.
	}

	json_rows_bytes, _ := json.Marshal(rows_result)
	fmt.Fprintf(w, string(json_rows_bytes))
}

func load_mem_table(w http.ResponseWriter, r *http.Request) {
	
	get_mem_table()
	
}


// func write_wal() chan int {
// 	r := make(chan int)
// 	fmt.Println("Warming up ...")
// 	go func() {

// 		wal_file.Rows = 
// 		for _, row := range wal_file.Rows {
// 			if row.Key_id >= from && row.Key_id <= to && row.Table_name == table_from{
// 				rows_result = append(rows_result, row)
// 			}  
// 		}
		

// 		fmt.Println("Done ...")
// 	}()
// 	return r
// }

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

	return iconv;

	//fmt.Fprintf(w, strconv.Itoa(index_row.Current_index))
}


func read_wal(w http.ResponseWriter, r *http.Request) {
	

	//insert_count := r.URL.Query().Get("insert_count")

	//get body with the mem_row

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



func insert(w http.ResponseWriter, r *http.Request) {
	

	key_id := r.URL.Query().Get("key_id")
	ikey_id, err := strconv.Atoi(key_id)
	if err != nil {
		log.Fatal(err)
	}
	var result mem_row
	var coll []mem_row
	result.Key_id = ikey_id
	result.Table_name = r.URL.Query().Get("table")
	
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

    var p interface {}
	err = dec.Decode(&p)
	result.Document = p
	result.Parsed_Document = p.(map[string]interface{})

	//Check if IndexRow is full. Then create another and append.Otherwise, just append to the mem_table and ++ the counter.
	//The next One should be rotational list of available servers
	//create keep alive
	//aftter that, create method to update INDEX TABLES through the servers and create WRITE AHEAD LOG to be shared among the servers and order the indexes according to the request.

	coll = append(coll, result)
	index_it := get_wal(&coll)
	index_row := it.Index_rows[index_it] 
	json_data, err := json.Marshal(coll)

	fmt.Println("JSON_DATA")
	fmt.Println(coll)


    if err != nil {
        log.Fatal(err)
	}
	
	response, err := http.Post("http://" +  index_row.Instance_ip + ":" + index_row.Instance_port + "/" + index_row.Instance_name +  "/insert_worker?table=" + result.Table_name + "&key_id=" + strconv.Itoa(result.Key_id), "application/json", bytes.NewBuffer(json_data))
	if err != nil {
	 	log.Fatal(err)
	}

	fmt.Println(response)

	fmt.Fprintf(w,"Success")
}

func insert_worker(w http.ResponseWriter, r *http.Request) {
	
	key_id := r.URL.Query().Get("key_id")
	ikey_id, err := strconv.Atoi(key_id)
	if err != nil {
		log.Fatal(err)
	}
	var result mem_row
	result.Key_id = ikey_id
	//result.Document = r.URL.Query().Get("document")
	result.Table_name = r.URL.Query().Get("table")
	
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	

    var p []mem_row
	err = dec.Decode(&p)
	
	fmt.Println("Output-----------")
	fmt.Println(p)

	result.Document = p[0].Document
	result.Parsed_Document = p[0].Document.(map[string]interface{})
	mt.Rows = append(mt.Rows, result)

	fmt.Println(mt)
	//Check if IndexRow is full. Then create another and append.Otherwise, just append to the mem_table and ++ the counter.
	//The next One should be rotational list of available servers
	//create keep alive



	fmt.Fprintf(w, "Success")
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
	//myRouter.HandleFunc("/"+ configs.Instance_name + "/", homePage)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_all", get_all)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_rows", get_rows)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_range", get_range)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_slices_worker", get_slices_worker)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/update_index_manager", update_index_manager)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/load_mem_table", load_mem_table)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/read_wal", read_wal)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/insert", insert)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/insert_worker", insert_worker) 
	myRouter.HandleFunc("/"+ configs.Instance_name + "/select_data_where_worker_equals", select_data_where_worker_equals)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/select_data_where_worker_contains", select_data_where_worker_contains)

	log.Fatal(http.ListenAndServe(":"+ configs.Instance_Port, myRouter))
}

// Nimpha Facing Methods //

 

//Core Methods
func main() {
	go dump_wal("------------------------------WAL---------------------------------")
	//go dump_data("------------------------------Data---------------------------------")
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
	err = json.Unmarshal([]byte(root), &mt)

	if err!= nil{
		fmt.Println(err)
		log.Fatal(err)
		fmt.Println(err)
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
			//var result_document 

	// Unmarshal or Decode the JSON to the interface.
			
		
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


