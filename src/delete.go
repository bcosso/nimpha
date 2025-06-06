package main

import (
	"fmt"
	"slices"
)

func removeIndex(s []mem_row, index int) []mem_row {
	ret := make([]mem_row, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}

// func delete_data_where_worker_contains_rsocket(payload interface{}) interface{} {

// 	payload_content, ok := payload.(map[string]interface{})
// 	if !ok {
// 		fmt.Println("ERROR!")
// 	}
// 	table_name := payload_content["table"].(string)
// 	where_field := payload_content["where_field"].(string)
// 	where_content := payload_content["where_content"].(string)

// 	var rows_result []mem_row
// 	rows_affected := 0

// 	i := len(mt.Rows)
// 	rows_result = mt.Rows
// 	//fmt.Println("----- sizeofI: %d", i)
// 	for i > 0 {
// 		i--
// 		if mt.Rows[i].Table_name == table_name {
// 			if strings.Contains(mt.Rows[i].Parsed_Document[where_field].(string), where_content) {
// 				rows_affected++
// 				//fmt.Println("----- sizeofI: %d", i)
// 				rows_result = remove_index(rows_result, i)
// 			}
// 		}
// 	}

// 	mt.Rows = rows_result
// 	fmt.Println("Rows Affected: " + string(rows_affected))
// 	return "Rows Affected: " + string(rows_affected)
// }

// func deleteWorkerOld(payload interface{}) interface{} {

// 	p, _ := GetParsedDocumentToMemRow(payload)
// 	mt.Rows = slices.DeleteFunc(mt.Rows, func(row mem_row) bool {
// 		allKeys := true

// 		for key, val := range p.Parsed_Document {
// 			valMemTable, exists := row.Parsed_Document[key]
// 			if !exists {
// 				allKeys = false
// 			}
// 			if val != valMemTable {
// 				allKeys = false
// 			}
// 		}
// 		return allKeys
// 	})

// 	// mt.Rows = append(mt.Rows, p)

// 	return "Success"
// }

func (sing *SingletonTable) DeleteWorker(filterLogic *Filter, ctx *map[string]interface{}) interface{} {
	indexRow := 0
	sing.mu.Lock()

	for indexRow < len(sing.mt[filterLogic.TableObject[0].Name]) {
		var mem_table_query mem_table_queries
		mem_table_query.Rows = sing.mt[filterLogic.TableObject[0].Name][indexRow].Parsed_Document
		if evaluateLogic(mem_table_query, filterLogic, ctx) {
			// sing.mu.Lock()
			fmt.Println("----------------------------------------------------------------------------------")
			fmt.Println("Found to delete")
			fmt.Println("----------------------------------------------------------------------------------")

			sing.mt[filterLogic.TableObject[0].Name] = slices.Delete(sing.mt[filterLogic.TableObject[0].Name], indexRow, indexRow+1)
			// sing.mu.Unlock()
			indexRow--
		}
		indexRow++
	}

	sing.mu.Unlock()
	return "Success"
}
