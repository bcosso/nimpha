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
	Instance_Name string `json:"instance_name"`
	Instance_Port string `json:"instance_port"`
}

type peers struct {
	ip string `json:"ip"`
	name string `json:"name"`
}


type index_table struct {
	index_id   int `json:"index_id"`
	index_rows   []index_row `json:"index_row"`
}


type index_row struct {
	index_from   int `json:"index_from"`
	index_to   int `json:"index_to"`
	instance_name   string `json:"instance_name"`
	instance_ip   string `json:"instance_ip"`
	instance_port string `json:"instance_port"`
	table_name string `json:"table_name"`
}

type mem_table struct {
	key_id   int `json:"key_id"`
	rows []mem_row `json:"mem_row"`
}

type mem_row struct {
	key_id   int `json:"key_id"`
	document string `json:"document"`
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
	i, err := strconv.Atoi(key)
	if err != nil {
		log.Fatal(err)
	}

	get_rows_op(i, operation)
	
}

func get_rows_op(key int, operation string){
	switch (operation){
		case "eq":
			get_rows_equals_to(key)
			break
		case "gt":
			get_rows_greater_than(key)
			break
		case "st":
			get_rows_smaller_than(key)
			break
	}
}

func get_rows_equals_to(key int) []mem_row {

		//get which node I am
		//check range for the current node
		//chech it if this node can satisfy	this clause		

		var result []mem_row
		for _, node := range it.index_rows {
			if node.index_to <= key && node.index_from >= key {
				result = append(get_slices(key, key, node)) 
				//make it async and combine the results later 
			}
		}
		return result;

}

func get_rows_greater_than(key int) []mem_row{
	var result []mem_row
		for _, node := range it.index_rows {
			if node.index_to >= key && node.index_from <= key {
				result = append(get_slices(key, node.index_to, node)) 
				//make it async and combine the results later 
			}else if node.index_from >= key {
				result = append(get_slices(node.index_from, node.index_to, node))
			}
		}
		return result;
}

func get_rows_smaller_than(key int) []mem_row{
	var result []mem_row
	for _, node := range it.index_rows {
		if node.index_to >= key && node.index_from <= key {
			result = append(get_slices(node.index_from, key, node)) 
			//make it async and combine the results later 
		}else if node.index_to <= key {
			result = append(get_slices(node.index_from, node.index_to, node))
		}
	}
	return result;
}


func get_slices(from int, to int, ir index_row) []mem_row{
	//if (len(mt.rows) > 
	var rows []mem_row
	response, err := http.Get("http://" + ir.instance_ip + "/" + ir.instance_name + "/get_slices_worker?from="+ strconv.Itoa(from) + "&to=" + strconv.Itoa(to))
	if err != nil {
		log.Fatal(err)
	}


	dec := json.NewDecoder(response.Body)
    dec.DisallowUnknownFields()

    err = dec.Decode(&rows)
    if err != nil {
		log.Fatal(err)
        // var syntaxError *json.SyntaxError
        // var unmarshalTypeError *json.UnmarshalTypeError

        // switch {
        // // Catch any syntax errors in the JSON and send an error message
        // // which interpolates the location of the problem to make it
        // // easier for the client to fix.
        // case errors.As(err, &syntaxError):
        //     msg := fmt.Sprintf("Request body contains badly-formed JSON (at position %d)", syntaxError.Offset)
        //     http.Error(w, msg, http.StatusBadRequest)

        // // In some circumstances Decode() may also return an
        // // io.ErrUnexpectedEOF error for syntax errors in the JSON. There
        // // is an open issue regarding this at
        // // https://github.com/golang/go/issues/25956.
        // case errors.Is(err, io.ErrUnexpectedEOF):
        //     msg := fmt.Sprintf("Request body contains badly-formed JSON")
        //     http.Error(w, msg, http.StatusBadRequest)

        // // Catch any type errors, like trying to assign a string in the
        // // JSON request body to a int field in our Person struct. We can
        // // interpolate the relevant field name and position into the error
        // // message to make it easier for the client to fix.
        // case errors.As(err, &unmarshalTypeError):
        //     msg := fmt.Sprintf("Request body contains an invalid value for the %q field (at position %d)", unmarshalTypeError.Field, unmarshalTypeError.Offset)
        //     http.Error(w, msg, http.StatusBadRequest)

        // // Catch the error caused by extra unexpected fields in the request
        // // body. We extract the field name from the error message and
        // // interpolate it in our custom error message. There is an open
        // // issue at https://github.com/golang/go/issues/29035 regarding
        // // turning this into a sentinel error.
        // case strings.HasPrefix(err.Error(), "json: unknown field "):
        //     fieldName := strings.TrimPrefix(err.Error(), "json: unknown field ")
        //     msg := fmt.Sprintf("Request body contains unknown field %s", fieldName)
        //     http.Error(w, msg, http.StatusBadRequest)

        // // An io.EOF error is returned by Decode() if the request body is
        // // empty.
        // case errors.Is(err, io.EOF):
        //     msg := "Request body must not be empty"
        //     http.Error(w, msg, http.StatusBadRequest)

        // // Catch the error caused by the request body being too large. Again
        // // there is an open issue regarding turning this into a sentinel
        // // error at https://github.com/golang/go/issues/30715.
        // case err.Error() == "http: request body too large":
        //     msg := "Request body must not be larger than 1MB"
        //     http.Error(w, msg, http.StatusRequestEntityTooLarge)

        // // Otherwise default to logging the error and sending a 500 Internal
        // // Server Error response.
        // default:
        //     log.Println(err.Error())
        //     http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
        // }
        
    }



	return rows
}

func get_slices_worker(w http.ResponseWriter, r *http.Request) []mem_row{
	
	
	vars := mux.Vars(r)
	fromStr := vars["from"]
	toStr := vars["to"]
	from, err := strconv.Atoi(fromStr)
	if err != nil {
		log.Fatal(err)
	}
	to, err := strconv.Atoi(toStr)
	if err != nil {
		log.Fatal(err)
	}

	//compare first and last to from and to. Check the range. If it's high perform binary search (one alternative).
	//var rows []mem_row
	// if (len(mt.rows)+ baseRow < to){
	// 	to = 
	// }


	var real_index_from int
	var real_index_to int
	// var local_index_from int
	// var local_index_to int

	fromLocal := mt.rows[0].key_id
	toLocal := mt.rows[len(mt.rows) - 1].key_id



	if (fromLocal <= from && to  >= toLocal ){
		real_index_from = 0
		real_index_to = len(mt.rows) - 1
	}else{
		real_index_from = 0
		real_index_to = len(mt.rows) - 1
		searchInMemTableFrom(&real_index_from, &real_index_to, &fromLocal)
		var temp_real_index_from int
		temp_real_index_from = real_index_from
		real_index_to = len(mt.rows) - 1
		searchInMemTableTo(&temp_real_index_from, &real_index_to, &toLocal)
	}
		

	//rows = mt.rows[from:to]
	return mt.rows[real_index_from:real_index_to]
}

func searchInMemTableFrom(real_index_from *int, real_index_to *int, key_id_from *int){
	// + or -1
	var current_real_index_from int;
	if (mt.rows[*real_index_from].key_id!=*key_id_from){
		
		current_real_index_from = (*real_index_from + *real_index_to)/2
		if (mt.rows[current_real_index_from].key_id>*key_id_from){
			*real_index_to = current_real_index_from
			searchInMemTableFrom(real_index_from, real_index_to, key_id_from)
		}else if (mt.rows[current_real_index_from].key_id<*key_id_from){
			*real_index_from = current_real_index_from
			searchInMemTableFrom(real_index_from, real_index_to, key_id_from)
		}
	}
}

func searchInMemTableTo(real_index_from *int, real_index_to *int, key_id_to *int){
	// + or -1
	var current_real_index_from int;
	if (mt.rows[*real_index_to].key_id!=*key_id_to){
		
		current_real_index_from = (*real_index_from + *real_index_to)/2
		if (mt.rows[current_real_index_from].key_id>*key_id_to){
			*real_index_to = current_real_index_from
			searchInMemTableTo(real_index_from, real_index_to, key_id_to)
		}else if (mt.rows[current_real_index_from].key_id<*key_id_to){
			*real_index_from = current_real_index_from
			searchInMemTableTo(real_index_from, real_index_to, key_id_to)
		}
	}
}

func handleRequests(configs *config ) {

	myRouter := mux.NewRouter().StrictSlash(true)
	//myRouter.HandleFunc("/"+ configs.Instance_Name + "/", homePage)
	myRouter.HandleFunc("/"+ configs.Instance_Name + "/get_all", get_all)
	myRouter.HandleFunc("/"+ configs.Instance_Name + "/get_rows", get_rows)

	myRouter.HandleFunc("/"+ configs.Instance_Name + "/update_index_manager", update_index_manager)


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
	get_mem_table()

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
	json.Unmarshal(root, &_it)

	return _it;
}


func get_mem_table(){
	configfile, err := os.Open("mem_table.json")
    if err != nil {
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	json.Unmarshal(root, &mt)
}

func check_index_manager(){}
func replicate_index_manager(){}
func update_index_manager(w http.ResponseWriter, r *http.Request){
    fmt.Fprintf(w, "update current index manager")
    fmt.Println("Endpoint Hit: get_rows")
}


// func Poller(in, out chan *Resource) {
//     for r := range in {
//         // poll the URL

//         // send the processed Resource to out
//         out <- r
//     }
// }