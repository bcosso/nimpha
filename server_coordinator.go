package main

import (
    "fmt"
	"log"
	// "errors"	
	"math"
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
	response, err := http.Get("http://" + ir.Instance_ip + "/" + ir.Instance_name + "/get_slices_worker?from="+ strconv.Itoa(from) + "&to=" + strconv.Itoa(to) + "&table_from=" + ir.Table_name)
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

func get_slices_worker(w http.ResponseWriter, r *http.Request) {
	
	
	//vars := mux.Vars(r)
	
	//fmt.Println(vars)

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

	//compare first and last to from and to. Check the range. If it's high perform binary search (one alternative).
	//var rows []mem_row
	// if (len(mt.rows)+ baseRow < to){
	// 	to = 
	// }


	var real_Index_from int
	var real_Index_to int
	var result_from int

	var result_to int

	// var local_Index_from int
	// var local_Index_to int
	fmt.Println("MemoryTable:::::::")
	fmt.Println(mt)
	fmt.Println("Pointer:::::::")
	fmt.Println(&mt)

	fromLocal := from
	toLocal := to

	fmt.Println("IndexFrom:::::::")
	fmt.Println(fromLocal)
	fmt.Println("IndexTo:::::::")
	fmt.Println(toLocal)

	

	// if (fromLocal <= from && to  >= toLocal ){
	// 	real_Index_from = 0
	// 	real_Index_to = len(mt.Rows) - 1
	// }else{
		real_Index_from = 0
		real_Index_to = len(mt.Rows) - 1
		searchInMemTable(&real_Index_from, &real_Index_to, &fromLocal, table_from, &result_from)

		fmt.Println("IntermediateIndexFromResult:::::::")
		fmt.Println(result_from)
		
		
		var temp_real_Index_from int
		temp_real_Index_from = real_Index_from
		real_Index_to = len(mt.Rows) - 1
		searchInMemTable(&temp_real_Index_from, &real_Index_to, &toLocal, table_from, &result_to)
	// }
	
	fmt.Println("IndexFromResult:::::::")
	fmt.Println(real_Index_from)
	fmt.Println("IndexToResult:::::::")
	fmt.Println(real_Index_to)

	rows_result := mt.Rows[result_from:result_to]
	json_rows_bytes, _ := json.Marshal(rows_result)
	fmt.Fprintf(w, string(json_rows_bytes))
}

func searchInMemTable(real_Index_from *int, real_Index_to *int, Key_id_from *int, table_from string, result_from *int){
	// + or -1
	var current_real_Index_from int;
	if (mt.Rows[*real_Index_from].Key_id!=*Key_id_from || mt.Rows[*real_Index_from].Table_name != table_from){
		
		var binRound float64 = (float64(*real_Index_from) + float64(*real_Index_to))/2.0
		current_real_Index_from = int(math.Round(binRound))
		fmt.Println("current_real_Index_from:::::::BinarySearch")
		fmt.Println(current_real_Index_from)
		fmt.Println(*result_from)
		*result_from = current_real_Index_from
		
			if (mt.Rows[current_real_Index_from].Key_id>*Key_id_from && mt.Rows[current_real_Index_from].Table_name == table_from && *real_Index_to != current_real_Index_from){
				*real_Index_to = current_real_Index_from
				searchInMemTable(real_Index_from, real_Index_to, Key_id_from, table_from, result_from)
				fmt.Println("WayBack:::::::BinarySearch::::real_Index_to")
				fmt.Println(*real_Index_to)
			}else if (mt.Rows[current_real_Index_from].Key_id<*Key_id_from  && mt.Rows[current_real_Index_from].Table_name == table_from && *real_Index_from != current_real_Index_from){
				*real_Index_from = current_real_Index_from
				searchInMemTable(real_Index_from, real_Index_to, Key_id_from, table_from, result_from)
				fmt.Println("WayBack:::::::BinarySearch:::real_Index_from")
				fmt.Println(*real_Index_from)
			}
		
	}
}

// func searchInMemTableTo(real_Index_from *int, real_Index_to *int, Key_id_to *int, table_from string, result_to *int ){
// 	// + or -1
// 	var current_real_Index_from int;
// 	if (mt.Rows[*real_Index_to].Key_id!=*Key_id_to || mt.Rows[*real_Index_to].Table_name == table_from){
		
// 		var binRound float64 = (float64(*real_Index_from) + float64(*real_Index_to))/2.0
// 		current_real_Index_from = int(math.Round(binRound))
// 		fmt.Println("current_real_Index_to:::::::BinarySearch")
// 		fmt.Println(current_real_Index_from)
// 		*result_to = current_real_Index_from

// 		if (mt.Rows[current_real_Index_from].Key_id>*Key_id_to && mt.Rows[current_real_Index_from].Table_name == table_from && *real_Index_to != current_real_Index_from){
// 			*real_Index_to = current_real_Index_from
// 			searchInMemTableTo(real_Index_from, real_Index_to, Key_id_to, table_from, result_to)
// 		}else if (mt.Rows[current_real_Index_from].Key_id<*Key_id_to && mt.Rows[current_real_Index_from].Table_name == table_from && *real_Index_from != current_real_Index_from){
// 			*real_Index_from = current_real_Index_from
// 			searchInMemTableTo(real_Index_from, real_Index_to, Key_id_to, table_from, result_to)
// 		}
// 	}
// }

func handleRequests(configs *config ) {

	myRouter := mux.NewRouter().StrictSlash(true)
	//myRouter.HandleFunc("/"+ configs.Instance_name + "/", homePage)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_all", get_all)
	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_rows", get_rows)

	myRouter.HandleFunc("/"+ configs.Instance_name + "/get_slices_worker", get_slices_worker)
	

	myRouter.HandleFunc("/"+ configs.Instance_name + "/update_index_manager", update_index_manager)


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


func get_mem_table(){
	configfile, err := os.Open("mem_table.json")
    if err != nil {
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	err = json.Unmarshal(root, &mt)
	if err!= nil{
		log.Fatal(err)
	}

	fmt.Println("MemoryTable:::::::get_mem_table")
	fmt.Println(mt)
	fmt.Println("Pointer:::::::get_mem_table")
	fmt.Println(&mt)
	fmt.Println("JSON:::::::get_mem_table")
	fmt.Println(root)
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
