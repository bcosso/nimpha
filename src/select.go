package main

import (
	"bytes"
	"fmt"
	"log"
	"slices"
	"time"

	//"errors"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"

	"github.com/bcosso/rsocket_json_requests"
	"github.com/bcosso/sqlparserproject"
)

// relationship := Relationship{TableNameRight: clauseRight.Prefix, ColumnLeft: clauseLeft.Clause, ColumnRight: clauseRight.Clause, IndexInMemQuery:indexRight }

type Relationship struct {
	TableNameRight  string
	ColumnLeft      string
	ColumnRight     string
	IndexInMemQuery int
	RelatedRow      *mem_table_queries
}

type mem_query_collection struct {
	TableName    string
	TableContent []mem_table_queries
}

type mem_table_queries struct {
	TableName     string
	QueryId       int
	Relationships []Relationship
	Rows          interface{}
}

func selectDataWhereWorkerEquals(payload interface{}) interface{} {
	oldTIme := time.Now()

	payload_content, ok := payload.(map[string]interface{})
	if !ok {
		fmt.Println("ERROR!")
	}

	table_name := payload_content["table"].(string)
	where_field := payload_content["where_field"].(string)
	where_content := payload_content["where_content"].(string)

	// _, existsInIndex := singletonIndex.btreeIndex[filter.TableObject[0].Name][cTree.Clause]
	// if existsInIndex {
	// 	resultBSearch, found := GetBinaryIndex(intValue, 0, len(singletonIndex.btreeIndex[newFilter.TableName][newFilter.ColumnName]), newFilter)
	// 	if found {
	// 		fmt.Println("Found index")
	// 		fmt.Println(resultBSearch)
	// 	}
	// 	if resultBSearch > -1 {
	// 		indexInFilter = append(indexInFilter, resultBSearch)
	// 	} else {
	// 		return false
	// 	}

	// var rows_result []mem_row
	// for _, row := range mt.Rows {
	// 	if row.Table_name == table_name {
	// 		if row.Parsed_Document[where_field] == where_content {
	// 			rows_result = append(rows_result, row)
	// 		}
	// 	}
	// }

	hashIndex, existsInIndex := singletonIndex.hashIndex[table_name][where_field]
	value := fmt.Sprintf("%v", where_content)
	if existsInIndex {
		row := *hashIndex[value]
		newRow := mem_table_queries{TableName: table_name, Rows: row.Parsed_Document}
		// _query[table_name] = append(_query[table_name], newRow)
		fmt.Println(time.Now().Sub(oldTIme))

		return newRow

	}

	return nil
}

// func select_data_where_worker_between_rsocket(payload interface{}) interface{} {

// 	payload_content, ok := payload.(map[string]interface{})
// 	if !ok {
// 		fmt.Println("ERROR!")
// 	}

// 	table_name := payload_content["table"].(string)
// 	where_field_from := payload_content["where_field_from"].(string)
// 	where_content_from := payload_content["where_content_from"].(string)

// 	where_field_to := payload_content["where_field_to"].(string)
// 	where_content_to := payload_content["where_content_to"].(string)

// 	var rows_result []mem_row
// 	for _, row := range mt.Rows {
// 		if row.Table_name == table_name {
// 			if row.Parsed_Document[where_field] == where_content {
// 				rows_result = append(rows_result, row)
// 			}
// 		}
// 	}

// 	return rows_result
// }

// func select_data_where_worker_contains_rsocket(payload interface{}) interface{} {

// 	payload_content, ok := payload.(map[string]interface{})
// 	if !ok {
// 		fmt.Println("ERROR!")
// 	}
// 	table_name := payload_content["table"].(string)
// 	where_field := payload_content["where_field"].(string)
// 	where_content := payload_content["where_content"].(string)

// 	var rows_result []mem_row
// 	for _, row := range mt.Rows {
// 		if row.Table_name == table_name {
// 			if strings.Contains(row.Parsed_Document[where_field].(string), where_content) {
// 				rows_result = append(rows_result, row)
// 			}
// 		}
// 	}

// 	return rows_result
// }

func selectDataRsocket(payload interface{}) interface{} {
	var rows []mem_row
	var result []mem_row
	payload_content, ok := payload.(map[string]interface{})
	if !ok {
		fmt.Println("ERROR!")
	}
	table_name := payload_content["table"].(string)
	where_field := payload_content["where_field"].(string)
	where_content := payload_content["where_content"].(string)
	where_operator := payload_content["where_operator"].(string)

	var jsonStr = `
	{
	"table":"%s",
	"where_field":"%s",
	"where_content":"%s"
	}
	`

	for _, ir := range configs_file.Peers {
		jsonStr = fmt.Sprintf(jsonStr, table_name, where_field, where_content)
		jsonMap := make(map[string]interface{})
		err1 := jsonIterGlobal.Unmarshal([]byte(jsonStr), &jsonMap)
		if err1 != nil {
			fmt.Println(err1)
		}
		url := "/" + ir.Name + "/select_data_where_worker_" + where_operator
		_port, _ := strconv.Atoi(ir.Port)
		rsocket_json_requests.RequestConfigs(ir.Ip, _port)

		CheckConnection(ir)
		response, err := rsocket_json_requests.RequestJSONNew(url, jsonMap, ir.Name)
		if err != nil {
			fmt.Println(err)
		} else {
			if response != nil {
				intermediate_inteface := response.([]interface{})
				json_rows_bytes, _ := jsonIterGlobal.Marshal(intermediate_inteface)

				reader := bytes.NewReader(json_rows_bytes)

				dec := json.NewDecoder(reader)
				dec.DisallowUnknownFields()

				err = dec.Decode(&rows)
				if err != nil {
					log.Fatal(err)
				}

				result = append(result, rows...)
			}
		}
	}

	return result
}

func ParseSqlClauseToStringTables(tables []SqlClause) []string {
	var tablesResult []string
	for _, tb := range tables {
		tablesResult = append(tablesResult, tb.Name)
	}
	return tablesResult
}

// var hashOfTablesToPeers map[string] []Peer
// func GetShardingForTables(tables [] SqlClause) ( []peers, []string){
// 	var peersResult []peers
// 	var tablesOutOfHash [] string
// 	fmt.Println("-----------------------------------------------------")
// 	fmt.Println("Tables:")
// 	fmt.Println(tables)
// 	fmt.Println("-----------------------------------------------------")
// 	for _, tb := range tables {
// 		//create new table with hash (map) for table name containing a peer or a peer name,
// 		// check if exists in peerResult, if yes, add to tablesOutOfHash
// 		fmt.Println("-----------------------------------------------------")
// 		fmt.Println("ShardingStrategies:")
// 		fmt.Println(configs_file.Sharding_strategy)
// 		fmt.Println(tb.Name)
// 		fmt.Println("-----------------------------------------------------")
// 		val, ok := configs_file.Sharding_strategy[tb.Name]
// 		// valAlias, okAlias := configs_file.ShardingGroup[tb.Alias] // That's actually not needed, check again and remove it
// 		if !ok {
// 			// panic("Table not found in shards")
// 			tablesOutOfHash =  append(tablesOutOfHash, tb.Name)
// 			break
// 		}

// 		fmt.Println("-----------------------------------------------------")
// 		fmt.Println(val)
// 		fmt.Println("-----------------------------------------------------")

// 		// if len(val) == 0 { val = valAlias }
// 		if len(peersResult) == 0{

// 			peersResult = append(peersResult, getReplicasFromShardGroup(configs_file.Sharding_groups[val.Sharding_group_id - 1].Replicas)...)
// 			fmt.Println("-----------------------------------------------------")
// 			fmt.Println("Got the PEERs")
// 			fmt.Println(peersResult)
// 			fmt.Println("-----------------------------------------------------")
// 		}else{
// 			for _, v := range getReplicasFromShardGroup(configs_file.Sharding_groups[val.Sharding_group_id - 1].Replicas){
// 				containsPeer := false
// 				for _, p := range peersResult{
// 					if (v.Name == p.Name){
// 						containsPeer = true
// 					}
// 				}
// 				if containsPeer == false{
// 					// peerResult = nil
// 					tablesOutOfHash = append(tablesOutOfHash, v.Name)
// 					// break;
// 				}
// 			}
// 		}
// 		// otherwise add to peerResult
// 		fmt.Println(val)

// 	}
// 	return peersResult, tablesOutOfHash
// }

func GetShardingForTables(tables []string) ([]peers, []string) {
	var peersResult []peers
	var tablesOutOfHash []string
	fmt.Println("-----------------------------------------------------")
	fmt.Println("Tables:")
	fmt.Println(tables)
	fmt.Println("-----------------------------------------------------")
	for _, tb := range tables {
		//create new table with hash (map) for table name containing a peer or a peer name,
		// check if exists in peerResult, if yes, add to tablesOutOfHash
		fmt.Println("-----------------------------------------------------")
		fmt.Println("ShardingStrategies:")
		fmt.Println(configs_file.Sharding_strategy)
		fmt.Println(tb)
		fmt.Println("-----------------------------------------------------")
		val, ok := configs_file.Sharding_strategy[tb]
		// valAlias, okAlias := configs_file.ShardingGroup[tb.Alias] // That's actually not needed, check again and remove it
		if !ok {
			// panic("Table not found in shards")
			tablesOutOfHash = append(tablesOutOfHash, tb)
			break
		}

		fmt.Println("-----------------------------------------------------")
		fmt.Println(val)
		fmt.Println("-----------------------------------------------------")

		// if len(val) == 0 { val = valAlias }
		if len(peersResult) == 0 {

			peersResult = append(peersResult, getReplicasFromShardGroup(configs_file.Sharding_groups[val.Sharding_group_id-1].Replicas)...)
			fmt.Println("-----------------------------------------------------")
			fmt.Println("Got the PEERs")
			fmt.Println(peersResult)
			fmt.Println("-----------------------------------------------------")
		} else {
			for _, v := range getReplicasFromShardGroup(configs_file.Sharding_groups[val.Sharding_group_id-1].Replicas) {
				containsPeer := false
				for _, p := range peersResult {
					if v.Name == p.Name {
						containsPeer = true
					}
				}
				if containsPeer == false {
					// peerResult = nil
					tablesOutOfHash = append(tablesOutOfHash, v.Name)
					// break;
				}
			}
		}
		// otherwise add to peerResult
		fmt.Println(val)

	}
	return peersResult, tablesOutOfHash
}

func getReplicasFromShardGroup(groupSharding []int) []peers {
	var resultPeers []peers
	for _, iShard := range groupSharding {
		resultPeers = append(resultPeers, configs_file.Peers[iShard])
	}
	return resultPeers
}

func GetQueryDataFromShardQuery(peer []peers, query string) []mem_table_queries {
	var result []mem_table_queries
	var rows []mem_table_queries

	fmt.Println("000000000000000000000000000000000000000000000000000000000000000000")
	fmt.Println("Final Contract Object")
	fmt.Println("000000000000000000000000000000000000000000000000000000000000000000")
	// jsonStr := fmt.Sprintf(jsonStr, table.Name , table.Alias ,string(jsonFilter))

	var jsonStr = `
	{
	"query":"%s"
	}
	`

	for _, ir := range peer {
		jsonStr1 := fmt.Sprintf(jsonStr, query)
		fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
		fmt.Println(jsonStr1)
		fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

		jsonMap := make(map[string]interface{})
		errMap := jsonIterGlobal.Unmarshal([]byte(jsonStr1), &jsonMap)
		if errMap != nil {
			fmt.Println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&ERROR")
			fmt.Println(errMap)

		}
		url := "/" + ir.Name + "/execute_query"
		_port, _ := strconv.Atoi(ir.Port)
		rsocket_json_requests.RequestConfigs(ir.Ip, _port)

		fmt.Println(jsonMap)

		CheckConnection(ir)
		response, err := rsocket_json_requests.RequestJSONNew(url, jsonMap, ir.Name)
		if err != nil {
			fmt.Println(err)
		} else {
			if response != nil {
				intermediate_inteface := response.([]interface{})
				json_rows_bytes, _ := jsonIterGlobal.Marshal(intermediate_inteface)

				reader := bytes.NewReader(json_rows_bytes)
				dec := json.NewDecoder(reader)
				dec.DisallowUnknownFields()

				err = dec.Decode(&rows)
				if err != nil {
					log.Fatal(err)
				}

				result = append(result, rows...)
				break
			}
		}
	}

	return result
}

func GetQueryDataFromShard(peer []peers, tables []SqlClause, logic_filters Filter) []mem_table_queries {
	var result []mem_table_queries
	var rows []mem_table_queries
	var innerObjectType TypeContract
	ParseObjectTypeToContract(logic_filters, &innerObjectType)
	jsonFilter, _ := jsonIterGlobal.Marshal(&logic_filters)
	jsonTables, _ := jsonIterGlobal.Marshal(&tables)
	jsonContract, _ := jsonIterGlobal.Marshal(&innerObjectType)

	fmt.Println("000000000000000000000000000000000000000000000000000000000000000000")
	fmt.Println("Final Contract Object")
	fmt.Println("000000000000000000000000000000000000000000000000000000000000000000")
	fmt.Println(innerObjectType)
	// jsonStr := fmt.Sprintf(jsonStr, table.Name , table.Alias ,string(jsonFilter))

	var jsonStr = `
	{
	"tables":%s,
	"filter":%s,
	"contract":%s
	}
	`

	for _, ir := range peer {
		jsonStr1 := fmt.Sprintf(jsonStr, string(jsonTables), string(jsonFilter), string(jsonContract))
		fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
		fmt.Println(jsonStr1)
		fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

		jsonMap := make(map[string]interface{})
		errMap := jsonIterGlobal.Unmarshal([]byte(jsonStr1), &jsonMap)
		if errMap != nil {
			fmt.Println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&ERROR")
			fmt.Println(errMap)

		}
		url := "/" + ir.Name + "/query_data_sharding_rsocket"
		_port, _ := strconv.Atoi(ir.Port)
		rsocket_json_requests.RequestConfigs(ir.Ip, _port)

		fmt.Println(jsonMap)

		CheckConnection(ir)
		response, err := rsocket_json_requests.RequestJSONNew(url, jsonMap, ir.Name)
		if err != nil {
			fmt.Println(err)
		} else {
			if response != nil {
				intermediate_inteface := response.([]interface{})
				json_rows_bytes, _ := jsonIterGlobal.Marshal(intermediate_inteface)

				reader := bytes.NewReader(json_rows_bytes)
				dec := json.NewDecoder(reader)
				dec.DisallowUnknownFields()

				err = dec.Decode(&rows)
				if err != nil {
					log.Fatal(err)
				}

				result = append(result, rows...)
			}
		}
	}

	return result
}

func selectDataWhereWorkerContainsRsocket(payload interface{}, querySql string) interface{} {
	logic_filters := payload.(Filter)
	ctx := make(map[string]interface{})
	var tableWorking []SqlClause
	_query := make(map[string][]mem_table_queries)
	ctx["_query"] = _query
	_analyzedFilterList := make(map[string]int)
	ctx["_analyzedFilterList"] = _analyzedFilterList
	ctx["_querysql"] = querySql

	//Distribute here the call to other instances.
	filteredResult := selectFieldsDecoupled2(logic_filters, logic_filters, 0, tableWorking, "", &ctx)
	// _query_temp_tables = nil

	// _currentQueryId = 0
	fmt.Println(filteredResult)

	return filteredResult
}
func checkIfImInPeers(peersToAnalyze []peers) bool {
	result := false
	for _, currentPeer := range peersToAnalyze {
		if currentPeer.Name == configs_file.Instance_name {
			result = true
			break
		}
	}
	return result
}

func selectFieldsDecoupled2(logic_filters Filter, fullLogicFilters Filter, indexFilter int, tables []SqlClause, aliasSubquery string, ctx *map[string]interface{}) interface{} {
	var tableResult []mem_table_queries
	futureAliasSubquery := ""
	_query := (*ctx)["_query"].((map[string][]mem_table_queries))

	if (len(logic_filters.TableObject) > 0) && logic_filters.TableObject[0].IsSubquery {
		futureAliasSubquery = logic_filters.TableObject[0].Alias
		if indexFilter == 0 {
			aliasSubquery = futureAliasSubquery
		}
	}
	if aliasSubquery != "" && futureAliasSubquery == "" {
		futureAliasSubquery = aliasSubquery
	}
	indexFilter++
	(*ctx)["_indexFilter"] = indexFilter
	for _, filter := range logic_filters.ChildFilters {
		tableResult = selectFieldsDecoupled2(filter, fullLogicFilters, indexFilter, tables, futureAliasSubquery, ctx).([]mem_table_queries)
	}

	//If is a select_to_show clause I need to Check the SelectClause and if there is no table, I create one in the in memory qyery
	if len(logic_filters.ChildFilters) > 0 {
		if len(logic_filters.ChildFilters[0].SelectClause) > 0 {

			tables = lookForRelatedTablesInFilters(logic_filters, indexFilter)
			var tableWorking []string
			// TODO
			// Get the Sharding strategy. After that send to the correspondent nodes via select_data_where_worker_contains_rsocket_sql with the filters
			// and fill tableResult. If data is retrieved from other nodes, and the sharding strategy is TABLE check tables that were used already (remove from tables list)
			//
			fmt.Println("----------------------------------------------------------------------------------")
			fmt.Println("BEFORE Sharding Type = Table")
			fmt.Println(configs_file)
			fmt.Println("----------------------------------------------------------------------------------")

			if strings.ToLower(configs_file.Sharding_type) == "table" {
				fmt.Println("----------------------------------------------------------------------------------")
				fmt.Println("Got in Sharding Type = Table")
				fmt.Println("----------------------------------------------------------------------------------")
				peer, tablesOutOfHash := GetShardingForTables(ParseSqlClauseToStringTables(tables))
				if len(tablesOutOfHash) < 1 && checkIfImInPeers(peer) == false {
					_querySql := (*ctx)["_querysql"].(string)
					tableResult = GetQueryDataFromShardQuery(peer, _querySql)
					return tableResult
				} else {
					//Querying tables out of shard
				}
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			checkForTablesInNodes(tables, logic_filters, ctx)
			fmt.Println("----------------------------------------------------------------------------------")
			fmt.Println("The Tables")
			fmt.Println(tables)
			fmt.Println("----------------------------------------------------------------------------------")

			if len(tables) > 0 {
				for _, table := range tables {
					//Check existance in query_objects
					foundMemTable := isInQueryObject(table, ctx)
					if !foundMemTable {
						//Check existance in (index_ for distributed) mem_table

						if singletonTable.IsInMemTable(table, logic_filters.ChildFilters[0].SelectClause, logic_filters, ctx) == false {
							fmt.Println("----------------------------------------------------------------------------------")
							fmt.Println("Not In MemTable")
							fmt.Println(tables)
							fmt.Println("----------------------------------------------------------------------------------")
							if reflect.TypeOf(table.SelectableObject) == reflect.TypeOf(fullLogicFilters) {
								tableResult = selectFieldsDecoupled2(table.SelectableObject.(Filter), fullLogicFilters, indexFilter, tables, futureAliasSubquery, ctx).([]mem_table_queries)
							} else {
								var columns map[string]interface{}
								for _, column := range logic_filters.SelectClause {
									columns[column.Name] = column.SelectableObject
								}
								newRow := mem_table_queries{TableName: strconv.Itoa(indexFilter), Rows: columns}

								_query[strconv.Itoa(indexFilter)] = append(_query[strconv.Itoa(indexFilter)], newRow)

								tableWorking = append(tableWorking, strconv.Itoa(indexFilter))
								tableResult = append(tableResult, newRow)
							} //I need to return the name of the table I'm working, get it in the mem_query and apply filters on the way back. I just insert on the mem query what is according to Filter
						} else {
							fmt.Println("----------------------------------------------------------------------------------")
							fmt.Println("Yes In MemTable")
							fmt.Println(tables)
							fmt.Println("----------------------------------------------------------------------------------")
							if table.Alias != "" {
								tableWorking = append(tableWorking, table.Alias)
							} else {
								tableWorking = append(tableWorking, table.Name)
							}
							tableResult = GetTableSummarize(tableWorking, logic_filters, logic_filters.ChildFilters[0].SelectClause, aliasSubquery, indexFilter, ctx)
						}
					} else {

						if table.Alias != "" {
							tableWorking = append(tableWorking, table.Alias)
						} else {
							tableWorking = append(tableWorking, table.Name)
						}

						// fmt.Println("----------------------------------------------------------------------------------")
						// fmt.Println("Yes In MemTable")
						// fmt.Println("ctx:")
						// fmt.Println(*ctx)
						// fmt.Println("tableWorking:")
						// fmt.Println(tableWorking)
						// fmt.Println("ChildFilters[0].SelectClause:")
						// fmt.Println(logic_filters.ChildFilters[0].SelectClause)

						// fmt.Println("----------------------------------------------------------------------------------")

						tableResult = GetTableSummarize(tableWorking, logic_filters, logic_filters.ChildFilters[0].SelectClause, aliasSubquery, indexFilter, ctx)
					}
					//if it does not exist anywhere, throw an error
				}

			}
		}
	}
	fmt.Println(tableResult)
	return tableResult
}

// var _query_table_id int = 0
func GetTableSummarize(tables []string, filter Filter, selectObject []SqlClause, aliasSubquery string, indexFilter int, ctx *map[string]interface{}) []mem_table_queries {
	var tableResult []mem_table_queries
	_query := (*ctx)["_query"].(map[string][]mem_table_queries)
	tableWrite := ""
	fmt.Println("----------------------------------------------------------------------------------")
	fmt.Println("GetTableSummarize")

	// ManageQueryIndexes(filter)

	if aliasSubquery != "" {
		tableWrite = aliasSubquery
	} else {
		tableWrite = strconv.Itoa(indexFilter)
	}

	for _, table := range tables {
		index := 0
		for index < len(_query[table]) {
			if evaluateLogic(_query[table][index], &filter, ctx) {
				columns := make(map[string]interface{})
				for _, column := range selectObject {
					if !checkSelectStar(column, &columns, _query[table][index]) {
						columnResult := ProjectColumns(_query[table][index], column, ctx)
						fmt.Println("----------------------------------------------------------------------------------")
						fmt.Println("Not Star")
						fmt.Println(columnResult)
						if column.Alias != "" {
							columnResult.Alias = column.Alias
						} // columnResult.Name = column.Name

						if columnResult.Alias != "" {
							columns[columnResult.Alias] = make(map[string]interface{})
							columns[columnResult.Alias] = columnResult.SelectableObject
						} else {
							columns[columnResult.Name] = make(map[string]interface{})
							columns[columnResult.Name] = columnResult.SelectableObject
						}
					}
				}
				newRow := mem_table_queries{TableName: tableWrite, Rows: columns}
				tableResult = append(tableResult, newRow)
			}

			index++
		}
		_query[tableWrite] = append(_query[tableWrite], tableResult...)
	}

	return tableResult
}

func CheckColumnExistance(row mem_table_queries, clause sqlparserproject.CommandTree) (interface{}, bool) {
	rowColumn := row.Rows.(map[string]interface{})
	actualValue, found := rowColumn[clause.Clause]
	if found == false {
		for _, relationship := range row.Relationships {
			rowColumn = (*relationship.RelatedRow).Rows.(map[string]interface{})
			actualValueRelationship, foundRelationship := rowColumn[clause.Clause]
			if foundRelationship == true {
				actualValue = actualValueRelationship
				found = foundRelationship
			}
		}
	}

	return actualValue, found
}

func ProjectColumns(row mem_table_queries, column SqlClause, ctx *map[string]interface{}) SqlClause {
	var clauseValidation sqlparserproject.CommandTree
	// columns := make(map[string]interface{})
	var conditionValidation Condition
	var columnReturn SqlClause
	var columnManSqlClause SqlClause
	if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(clauseValidation) {
		fmt.Println("*****************************************")
		fmt.Println("Found type of the column - CommandTree")
		clause := column.SelectableObject.(sqlparserproject.CommandTree)
		// columns[clause.Clause] = make(map[string]interface{})
		rowColumn, found := CheckColumnExistance(row, clause)
		if found == false {
			//Change for proper error treatment here.
			fmt.Println(column.SelectableObject)
			panic("Non existent column:" + column.Name + " - " + column.Alias)

		}
		columnReturn.Alias = clause.Clause
		columnReturn.SelectableObject = rowColumn

	} else {
		if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(conditionValidation) {
			fmt.Println("*****************************************")
			fmt.Println("Found type of the column - Condition")
			columnReturn = GetConditionFlow(row, column, ctx)
		} else if reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(columnReturn) || reflect.TypeOf(column.SelectableObject) == reflect.TypeOf(columnManSqlClause) {
			fmt.Println("*****************************************")
			fmt.Println("Found type of the column - SqlClause")
			columnReturn = column.SelectableObject.(SqlClause)
		} else {
			fmt.Println("*****************************************")
			fmt.Println("Not finding type of the column")

			fmt.Println(reflect.TypeOf(column.SelectableObject))
			fmt.Println(reflect.TypeOf(columnReturn))
			fmt.Println(column.SelectableObject)
			columnReturn = column
		}
	}

	return columnReturn
}

func checkSelectStar(columnResult SqlClause, columns *map[string]interface{}, row mem_table_queries) bool {

	found, value := CheckWhichSideContainsColumn(columnResult.SelectableObject, nil)
	if found > -1 {
		if value.Clause == "*" {
			for k, v := range row.Rows.(map[string]interface{}) {
				(*columns)[k] = make(map[string]interface{})
				(*columns)[k] = v
			}
			return true
		}
	}
	return false
}

func checkForTablesInNodes(tables []SqlClause, filter Filter, ctx *map[string]interface{}) {

	_query := (*ctx)["_query"].((map[string][]mem_table_queries))
	_indexFilter := (*ctx)["_indexFilter"].(int)

	fmt.Println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	fmt.Println("IndexFilter")
	fmt.Println(_indexFilter)
	fmt.Println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

	for _, table := range tables {
		//Search in nodes
		var jsonStr = `
		{
		"table":"%s",
		"alias":"%s",
		"filter":%s
		}
		`

		// if configs_file.Sharding_type != "" {
		// check type of sharding and if the table is included somehow, or the table is just a replica. If it's a replica, no need to fetch. If it's a shard, yes.
		// 	return
		// }
		//need to check the safety of doing this in parallel
		if !singletonTable.IsInMemTable(table, filter.ChildFilters[0].SelectClause, filter, ctx) {

			for _, ir := range configs_file.Peers {
				if configs_file.Instance_name != ir.Name {
					var rows []mem_table_queries
					jsonFilter, _ := jsonIterGlobal.Marshal(&filter)
					jsonStr = fmt.Sprintf(jsonStr, table.Name, table.Alias, string(jsonFilter))
					jsonMap := make(map[string]interface{})
					err1 := jsonIterGlobal.Unmarshal([]byte(jsonStr), &jsonMap)
					if err1 != nil {
						fmt.Println(err1)
					}
					//New receiving function
					url := "/" + ir.Name + "/select_table"
					_port, _ := strconv.Atoi(ir.Port)
					rsocket_json_requests.RequestConfigs(ir.Ip, _port)
					CheckConnection(ir)
					response, err := rsocket_json_requests.RequestJSONNew(url, jsonMap, ir.Name)

					if err != nil {
						fmt.Println(err)
					} else {
						if response != nil {
							//add to _query_temp_tables
							intermediate_inteface := response.([]interface{})
							json_rows_bytes, _ := jsonIterGlobal.Marshal(intermediate_inteface)

							reader := bytes.NewReader(json_rows_bytes)
							dec := json.NewDecoder(reader)
							dec.DisallowUnknownFields()

							err = dec.Decode(&rows)
							if err != nil {
								log.Fatal(err)
							}
							tableName := table.Name
							if table.Alias != "" {
								tableName = table.Alias
							}

							_query[tableName] = append(_query[tableName], rows...)
						}
					}
				}
			}
		}
	}
}

func selectTable(payload interface{}) interface{} {

	payload_content, ok := payload.(map[string]interface{})
	if !ok {
		fmt.Println("ERROR!")
	}

	table_name := payload_content["table"].(string)
	alias := payload_content["alias"].(string)

	fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	fmt.Println("Result for distributed Query in Node")
	fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	rows_result := singletonTable.SelectTable(table_name, alias)

	return rows_result
}

func lookForRelatedTablesInFilters(fullLogicFilters Filter, level int) []SqlClause {
	var tables []SqlClause
	if len(fullLogicFilters.TableObject) > 0 {
		tables = append(tables, fullLogicFilters.TableObject...)
	}
	return tables
}

func lookForFinalFiltersInFilters(fullLogicFilters Filter, level int) []SqlClause {
	// check for filters of type numeric or string, to filter in the worker nodes
	var tables []SqlClause
	if len(fullLogicFilters.TableObject) > 0 {
		tables = append(tables, fullLogicFilters.TableObject...)
	}
	return tables
}

func isInQueryObject(selectableObject SqlClause, ctx *map[string]interface{}) bool {
	_query := (*ctx)["_query"].((map[string][]mem_table_queries))
	_, found := _query[selectableObject.Name]
	_, foundALIAS := _query[selectableObject.Alias]

	return found || foundALIAS
}

func isTableInQueryObject(tableName string, ctx *map[string]interface{}) bool {
	_query := (*ctx)["_query"].((map[string][]mem_table_queries))
	_, found := _query[tableName]
	return found
}

func GetValueFromFilter(contentMemRow interface{}, referenceType interface{}) interface{} {
	str := ""
	intVar := 30
	floatVar := 2.321

	var result interface{}

	switch reflect.TypeOf(referenceType) {
	case reflect.TypeOf(str):
		result = strings.Replace(contentMemRow.(string), "'", "", -1)
		break
	case reflect.TypeOf(intVar):
		str := fmt.Sprintf("%v", contentMemRow)
		result, _ = strconv.Atoi(str)
		break
	case reflect.TypeOf(floatVar):
		str := fmt.Sprintf("%v", contentMemRow)
		result, _ = strconv.ParseFloat(str, 64)
		break
	default:
		result = ""
		break

	}

	return result
}

func getBiggerThan(value1 interface{}, value2 interface{}) bool {

	intVar := 30
	floatVar := 2.321

	switch reflect.TypeOf(value1) {

	case reflect.TypeOf(intVar):
		return value1.(int) > value2.(int)
		break
	case reflect.TypeOf(floatVar):
		return value1.(float64) > value2.(float64)
		break
	default:
		return value1.(int) > value2.(int)
		break

	}
	return false
}

func GetComparisonTypeAndCompare(gateName string, leftValue bool, rightValue bool) bool {
	switch strings.ToLower(gateName) {
	case "and":
		return AndCompare(leftValue, rightValue)
		break
	case "or":
		return OrCompare(leftValue, rightValue)
		break
	default:
		return false
		break
	}
	return false
}

func selectDataRsocket_sql(payload interface{}) interface{} {

	var rows []mem_row
	var result []mem_row
	payload_content, ok := payload.(map[string]interface{})
	if !ok {
		fmt.Println("ERROR!")
	}
	table_name := payload_content["table"].(string)
	where_field := payload_content["where_field"].(string)
	where_content := payload_content["where_content"].(string)
	where_operator := payload_content["where_operator"].(string)

	var jsonStr = `
	{
	"table":"%s",
	"where_field":"%s",
	"where_content":"%s"
	}
	`

	for _, ir := range configs_file.Peers {
		jsonStr = fmt.Sprintf(jsonStr, table_name, where_field, where_content)
		jsonMap := make(map[string]interface{})
		jsonIterGlobal.Unmarshal([]byte(jsonStr), &jsonMap)
		url := "/" + ir.Name + "/select_data_where_worker_" + where_operator
		_port, _ := strconv.Atoi(ir.Port)
		rsocket_json_requests.RequestConfigs(ir.Ip, _port)

		CheckConnection(ir)
		response, err := rsocket_json_requests.RequestJSONNew(url, jsonMap, ir.Name)
		if err != nil {
			fmt.Println(err)
		} else {
			if response != nil {
				intermediate_inteface := response.([]interface{})
				json_rows_bytes, _ := jsonIterGlobal.Marshal(intermediate_inteface)

				reader := bytes.NewReader(json_rows_bytes)
				dec := json.NewDecoder(reader)
				dec.DisallowUnknownFields()

				err = dec.Decode(&rows)
				if err != nil {
					log.Fatal(err)
				}

				result = append(result, rows...)
			}
		}
	}

	return result
}

// var _analyzedFilterList map[string]int
func evaluateLogic(current_row mem_table_queries, logicObject2 *Filter, ctx *map[string]interface{}) bool {
	result := false
	previousResult := false
	previousGate := ""
	var treeReference sqlparserproject.CommandTree
	logicObject := *logicObject2
	_analyzedFilterList := (*ctx)["_analyzedFilterList"].(map[string]int)

	for _, filter := range logicObject.ChildFilters {

		if reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(filter) && reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(filter) {
			// Still work in progress, not working

			commandFilterLeft := filter.CommandLeft.(Filter)
			commandFilterRight := filter.CommandRight.(Filter)
			if len(commandFilterLeft.ChildFilters) > 0 {
				result = evaluateLogic(current_row, &commandFilterLeft, ctx)
			} else if len(commandFilterRight.ChildFilters) > 0 {
				//These two scenarios above have to be detailed.
			} else {
				//This should be the most common scenario "table1.Id = table2.Id"
				//Need to execute the join comparisson for the entire table only once
				//After that, need a method ONLY TO CHECK
				if filter.AlreadyConsumed == false {
					// GetJoinAndJoin(filter.Operation, filter.CommandLeft, filter.CommandRight)
					filter.AlreadyConsumed = true
				}

				// Does this relationship exist?
				// current_row.Relationships

				result = CheckRelationshipExistance(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}

		} else if reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(treeReference) && reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(treeReference) {

			//After that, need a method ONLY TO CHECK
			operation := filter.Operation
			clauseLeft := filter.CommandLeft.(sqlparserproject.CommandTree)
			clauseRight := filter.CommandRight.(sqlparserproject.CommandTree)
			_, found := _analyzedFilterList[operation+"_"+clauseLeft.Clause+"_"+clauseRight.Clause]
			if found == false {

				if isTableInQueryObject(clauseLeft.Prefix, ctx) && isTableInQueryObject(clauseRight.Prefix, ctx) {
					GetJoinAndJoin(filter.Operation, filter.CommandLeft, filter.CommandRight, &current_row, ctx)

					if _analyzedFilterList == nil {
						newMap := make(map[string]int)
						newMap[operation+"_"+clauseLeft.Clause+"_"+clauseRight.Clause] = 1
						_analyzedFilterList = newMap
					} else {
						_analyzedFilterList[operation+"_"+clauseLeft.Clause+"_"+clauseRight.Clause] = 1
					}

					_analyzedFilterList[operation+"_"+clauseLeft.Clause+"_"+clauseRight.Clause] = 1

				}
			} else { //Unoptimized
				// GetJoinAndJoin(filter.Operation, filter.CommandLeft, filter.CommandRight)
			}

			result = CheckRelationshipExistance(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)

		} else if reflect.TypeOf(filter.CommandLeft) == reflect.TypeOf(filter) {
			commandFilterLeft := filter.CommandLeft.(Filter)
			if len(commandFilterLeft.ChildFilters) > 0 {
				result = evaluateLogic(current_row, &commandFilterLeft, ctx)
			} else {
				result = GetFilterAndFilter2(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		} else if reflect.TypeOf(filter.CommandRight) == reflect.TypeOf(filter) {
			commandFilterRight := filter.CommandRight.(Filter)
			if len(commandFilterRight.ChildFilters) > 0 {
				result = evaluateLogic(current_row, &commandFilterRight, ctx)
			} else {
				result = GetFilterAndFilter2(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		} else {
			// the above statements should cover cases like "where field IN ()" or some complex subquery logic in where.

			if filter.CommandLeft == nil {
				if len(filter.ChildFilters) > 0 {
					result = evaluateLogic(current_row, &filter, ctx)
				}
			} else {
				//if current row has prefix, we need to check in this method if the prefix corresponds to the table name/alias of the commandleft or command right
				//if they are both CommandTrees, means it's a join. Check which side of the operation it is then scan mem_query for the other side prefix
				result = GetFilterAndFilter2(filter.Operation, filter.CommandLeft, filter.CommandRight, current_row)
			}
		}

		if previousGate != "" {
			result = GetComparisonTypeAndCompare(previousGate, result, previousResult)
		}
		previousGate = filter.Gate
		previousResult = result
	}
	return result
}

func CheckWhichSideContainsColumn(leftValue interface{}, rightValue interface{}) (int, sqlparserproject.CommandTree) {
	var clause sqlparserproject.CommandTree
	var filterReference sqlparserproject.CommandTree
	var filterReferencePointer *sqlparserproject.CommandTree

	if reflect.TypeOf(leftValue) == reflect.TypeOf(filterReference) && reflect.TypeOf(rightValue) == reflect.TypeOf(filterReference) {
		//It's a join
		clause = leftValue.(sqlparserproject.CommandTree)
		return 0, clause
	} else if (reflect.TypeOf(rightValue) == reflect.TypeOf(filterReference)) || (reflect.TypeOf(rightValue) == reflect.TypeOf(filterReferencePointer)) {
		clause = GetClauseFromValue(rightValue)
		return 1, clause
	} else if (reflect.TypeOf(leftValue) == reflect.TypeOf(filterReference)) || (reflect.TypeOf(leftValue) == reflect.TypeOf(filterReferencePointer)) {
		clause = GetClauseFromValue(leftValue)
		return 2, clause
	}

	return -1, clause
}

func GetFilterAndFilter2(operator string, leftValue interface{}, rightValue interface{}, row mem_table_queries) bool {

	var newLeftValue, newRightValue interface{}
	belongsToTable := false
	resultComparison := false
	mapRow := row.Rows.(map[string]interface{})

	side, clause := CheckWhichSideContainsColumn(leftValue, rightValue)
	//side = 0 - both, side = 1 - right, side = 2 - left, side = -1 - none

	if side == 0 {
		//It's a join
		clause = leftValue.(sqlparserproject.CommandTree)
		newLeftValue = GetValueFromFilter(mapRow[clause.Clause].(string), mapRow[clause.Clause].(string))

		//TODO for with Table to join on field
	} else if side == 1 {
		if clause.Clause == "table_name" {
			newRightValue = GetValueFromFilter(row.TableName, leftValue) //Not passing here anymore, since table logic is being handled elsewhere.
		} else if mapRow[clause.Clause] != nil {
			newRightValue = GetValueFromFilter(mapRow[clause.Clause], leftValue)
		}
		newLeftValue = leftValue
	} else if side == 2 {

		if clause.Clause == "table_name" {
			newLeftValue = GetValueFromFilter(row.TableName, rightValue) //Not passing here anymore, since table logic is being handled elsewhere.
		} else if mapRow[clause.Clause] != nil {
			newLeftValue = GetValueFromFilter(mapRow[clause.Clause], rightValue)
		}

		newRightValue = rightValue
	} else {
		belongsToTable = true
		newLeftValue = leftValue
		newRightValue = rightValue
	}

	belongsToTable = CheckBelongsToTable(row, clause)

	switch strings.ToLower(operator) {
	case "equals":
		resultComparison = (newLeftValue == newRightValue)
		break
	case "bigger_than":
		resultComparison = getBiggerThan(newLeftValue, newRightValue)
		break
	case "smaller_than":
		resultComparison = getBiggerThan(newRightValue, newLeftValue)
		break
	default:
		resultComparison = false
		break
	}
	return resultComparison && belongsToTable
}

func CheckBelongsToTable(row mem_table_queries, clause sqlparserproject.CommandTree) bool {
	if clause.Prefix != "" {
		if clause.Prefix != row.TableName {
			return false
		}
	}

	return true
}

func GetJoinAndJoin(operator string, leftValue interface{}, rightValue interface{}, current_row *mem_table_queries, ctx *map[string]interface{}) {
	_query := (*ctx)["_query"].((map[string][]mem_table_queries))
	mapRow := (*current_row).Rows.(map[string]interface{})
	indexLeft := 0

	clauseRight := GetClauseFromValue(rightValue)
	clauseLeft := GetClauseFromValue(leftValue)

	for indexLeft < len(_query[clauseLeft.Prefix]) {

		mapRowLeft := _query[clauseLeft.Prefix][indexLeft].Rows.(map[string]interface{})
		indexRight := 0
		for indexRight < len(_query[clauseRight.Prefix]) {

			mapRowRight := _query[clauseRight.Prefix][indexRight].Rows.(map[string]interface{})

			if mapRowLeft[clauseLeft.Clause] == mapRowRight[clauseRight.Clause] {
				// Could do it with a hash. First doing it with a complex object, unoptimized
				relationship := Relationship{TableNameRight: clauseRight.Prefix, ColumnLeft: clauseLeft.Clause, ColumnRight: clauseRight.Clause, IndexInMemQuery: indexRight, RelatedRow: &(_query[clauseRight.Prefix][indexRight])}

				_query[clauseLeft.Prefix][indexLeft].Relationships = append(_query[clauseLeft.Prefix][indexLeft].Relationships, relationship)

				if ((*current_row).TableName == _query[clauseLeft.Prefix][indexLeft].TableName) && mapRow[clauseLeft.Clause] == mapRowLeft[clauseLeft.Clause] {
					*current_row = _query[clauseLeft.Prefix][indexLeft]
				}
				//Add index (or pointer) to join list, so I can find the respective columns of this table in project/summarize. Will create a relationship list on _query_temp_tables. SHould also contain the name of the relationship table. Can be a map
			}

			indexRight++
		}

		indexLeft++
	}
}

func CheckRelationshipExistance(operator string, leftValue interface{}, rightValue interface{}, row mem_table_queries) bool {

	clauseRight := GetClauseFromValue(rightValue)
	clauseLeft := GetClauseFromValue(leftValue)
	mapRow := row.Rows.(map[string]interface{})

	for _, relation := range row.Relationships {
		if relation.TableNameRight == clauseRight.Prefix {
			mapRowRight := (*relation.RelatedRow).Rows.(map[string]interface{})
			if mapRow[clauseLeft.Clause] == mapRowRight[clauseRight.Clause] {
				return true
			}
		}
	}

	return false
}

func GetClauseFromValue(interfaceValue interface{}) sqlparserproject.CommandTree {

	var filterReferencePointer *sqlparserproject.CommandTree
	var clause sqlparserproject.CommandTree

	if reflect.TypeOf(interfaceValue) == reflect.TypeOf(filterReferencePointer) {
		clause = *interfaceValue.(*sqlparserproject.CommandTree)
	} else {
		clause = interfaceValue.(sqlparserproject.CommandTree)
	}
	return clause

}

// /////////////////////////////////////Mutex///////////////////////////////////////////////////////////
func (sing *SingletonTable) SelectTable(table_name string, alias string) []mem_table_queries {
	if table_name == "" {
		table_name = alias
	}
	sing.mu.RLock()
	defer sing.mu.RUnlock()
	fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	fmt.Println("Result for distributed Query in Node")
	fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	var rows_result []mem_table_queries
	for _, row := range sing.mt[table_name] {
		if row.Table_name == table_name {

			if alias != "" {
				table_name = alias
			}
			rowMemQuery := mem_table_queries{TableName: table_name, Rows: row.Parsed_Document}
			rows_result = append(rows_result, rowMemQuery)
		}
	}
	fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
	fmt.Println("Result for distributed Query in Node")
	fmt.Println(rows_result)
	fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

	return rows_result
}

func (sing *SingletonTable) IsInMemTable(tableObject SqlClause, selectObject []SqlClause, filter Filter, ctx *map[string]interface{}) bool {
	result := false
	_query := (*ctx)["_query"].((map[string][]mem_table_queries))
	// var clauseValidation sqlparserproject.CommandTree
	tableName := tableObject.Name

	//Place to call the Index verification. I need the filtter here
	if ManageQueryIndexes(filter, ctx) {
		return true
	}

	sing.mu.RLock()
	defer sing.mu.RUnlock()

	for _, row := range sing.mt[tableName] {
		if (row.Table_name == tableObject.Name) || (row.Table_name == tableObject.Alias) {
			name := ""
			if (row.Table_name == tableObject.Name) && (tableObject.Alias == "") {
				name = tableObject.Name
			} else {
				name = tableObject.Alias
			}

			newRow := mem_table_queries{TableName: name, Rows: row.Parsed_Document}
			_query[name] = append(_query[name], newRow)
			result = true
		}
	}

	return result
}

func ManageQueryIndexes(filter Filter, ctx *map[string]interface{}) bool {
	fmt.Println("000000000000000000000000000000000000000000000000000000000000000000000000000000000")
	fmt.Println("Filter")
	fmt.Println(filter.ChildFilters[2])
	fmt.Println(filter.ChildFilters[2].ChildFilters[0].CommandLeft)
	fmt.Println(reflect.TypeOf(filter.ChildFilters[2].ChildFilters[0].CommandLeft))
	fmt.Println(len(filter.ChildFilters[2].ChildFilters))

	_query := (*ctx)["_query"].((map[string][]mem_table_queries))
	// var checkCommandTree sqlparserproject.CommandTree
	//Does the table have indexes?
	_, exists := configs_file.Index[filter.TableObject[0].Name]
	var indexInFilter []int
	indexAppears := 0
	var filtersIndex []SimplifiedFilter

	if exists {
		name := filter.TableObject[0].Name
		for _, index := range configs_file.Index[filter.TableObject[0].Name] {
			for _, child := range filter.ChildFilters[2].ChildFilters {
				rightValue := child.CommandRight
				leftValue := child.CommandLeft
				if strings.ToLower(child.Gate) != "and" && child.Gate != "" {
					return false
				}

				if index.IndexType == "HASH" {

					//add row to result as mem_table_queries if there is no more filters for now
					side, cTree := CheckWhichSideContainsColumn(leftValue, rightValue)
					var value string
					//side = 0 - both, side = 1 - right, side = 2 - left, side = -1 - none

					switch side {
					case 1:
						hashIndex, existsInIndex := singletonIndex.hashIndex[filter.TableObject[0].Name][cTree.Clause]
						value = fmt.Sprintf("%v", leftValue)
						if existsInIndex {
							row := *hashIndex[value]
							newRow := mem_table_queries{TableName: name, Rows: row.Parsed_Document}
							_query[name] = append(_query[name], newRow)
							return true

						}
						break
					case 2:
						hashIndex, existsInIndex := singletonIndex.hashIndex[filter.TableObject[0].Name][cTree.Clause]
						value = fmt.Sprintf("%v", rightValue)
						if existsInIndex {
							row := *hashIndex[value]
							newRow := mem_table_queries{TableName: name, Rows: row.Parsed_Document}
							_query[name] = append(_query[name], newRow)
							return true

						}
						break
					}

					// GetValueFromFilter
					// cTree := child.CommandLeft.(sqlparserproject.CommandTree)

				} else if index.IndexType == "BTREE" {
					side, cTree := CheckWhichSideContainsColumn(leftValue, rightValue)
					var intValue int
					switch side {
					case 1:
						str := fmt.Sprintf("%v", leftValue)
						intValue, _ = strconv.Atoi(str)
						break
					case 2:
						str := fmt.Sprintf("%v", rightValue)

						intValue, _ = strconv.Atoi(str)
						break
					default:
						return false
					}
					fmt.Println("VALUE::")
					fmt.Println(intValue)

					newFilter := SimplifiedFilter{ColumnName: cTree.Clause, TableName: name, Operator: child.Operation}
					fmt.Println("FILTER::")
					fmt.Println(newFilter)
					_, existsInIndex := singletonIndex.btreeIndex[filter.TableObject[0].Name][cTree.Clause]
					if existsInIndex {
						resultBSearch, found := GetBinaryIndex(intValue, 0, len(singletonIndex.btreeIndex[newFilter.TableName][newFilter.ColumnName]), newFilter)
						if found {
							fmt.Println("Found index")
							fmt.Println(resultBSearch)
						}
						if resultBSearch > -1 {
							indexInFilter = append(indexInFilter, resultBSearch)
						} else {
							return false
						}

						if len(filtersIndex) > 0 {
							if filtersIndex[0].ColumnName == cTree.Clause {
								if resultBSearch < filtersIndex[0].Index && child.Operation == "bigger_than" {
									// newFilter := SimplifiedFilter{Index: resultBSearch, ColumnName: cTree.Clause}
									newFilter.Index = resultBSearch
									filtersIndex = slices.Insert(filtersIndex, 0, newFilter)
								} else if resultBSearch > filtersIndex[0].Index && child.Operation == "smaller_than" {
									// newFilter := SimplifiedFilter{Index: resultBSearch, ColumnName: cTree.Clause}
									newFilter.Index = resultBSearch
									filtersIndex = append(filtersIndex, newFilter)
								} else {
									return false
								}

								//check if range and sign are bigger or smaller then place it before or after the previous

							} else {
								return false // Only supporting 1 index per time for now
							}
						} else {
							newFilter.Index = resultBSearch
							filtersIndex = append(filtersIndex, newFilter)
						}
						// if any of the gates is OR we leave the function

						indexAppears++
					}

				}

			}

			// check indexType
			// if index == Hash AND it is part of the query AND there is no other condition
			// if index == Btree AND it is part of the query
		}

		if indexAppears > 0 {
			counterCondition := 0
			if len(filtersIndex) > 0 {
				fmt.Println("counterCOndition")
				fmt.Println(counterCondition)
				fmt.Println(filtersIndex)
				if filtersIndex[0].Operator == "smaller_than" && len(filtersIndex) < 2 {
					counterCondition = len(singletonIndex.btreeIndex[filtersIndex[0].TableName][filtersIndex[0].ColumnName]) - 1
				}

				for indexCondition(&counterCondition, filtersIndex) {
					row := (*singletonIndex.btreeIndex[filtersIndex[0].TableName][filtersIndex[0].ColumnName][counterCondition])
					newRow := mem_table_queries{TableName: name, Rows: row.Parsed_Document}
					_query[name] = append(_query[name], newRow)
				}

				return true

			}
		}

	}

	//check what columns are part
	// configs_file.Index[filter.TableObject[0].Name].
	//if filter.TableObject[0].Name //Check how this is going to wok for subqueries
	//if column.Alias is in singletonTable.hashIndex
	//

	fmt.Println("000000000000000000000000000000000000000000000000000000000000000000000000000000000")

	return false
}

type SimplifiedFilter struct {
	Index        int
	Operator     string
	IndexInTable string
	ColumnName   string
	TableName    string
}

func indexCondition(counterCondition *int, filter []SimplifiedFilter) bool {
	operation := 1
	targetIndex := filter[0].Index
	// previosuCondition := true

	if len(filter) > 1 {
		targetIndex = filter[1].Index
		if *counterCondition == 0 {
			*counterCondition = filter[0].Index - 1
		}

	} else if filter[0].Operator == "bigger_than" {
		targetIndex = len(singletonIndex.btreeIndex[filter[0].TableName][filter[0].ColumnName]) - 1
	} else if filter[0].Operator == ">=" {
		targetIndex = len(singletonIndex.btreeIndex[filter[0].TableName][filter[0].ColumnName]) - 1
	} else if filter[0].Operator == "equals" {
		targetIndex = filter[0].Index
		operation = filter[0].Index

	} else {

		targetIndex = 0
		operation *= -1

	}

	if *counterCondition != targetIndex {
		*counterCondition += operation
	} else {
		return false
	}

	return true
}

func GetBinaryIndex(value int, low int, high int, filter SimplifiedFilter) (int, bool) {

	division := (low + high) / 2
	// if division >= len(singletonIndex.btreeIndex[filter.TableName][filter.ColumnName]) {
	// 	division = len(singletonIndex.btreeIndex[filter.TableName][filter.ColumnName]) - 1
	// }
	index := *singletonIndex.btreeIndex[filter.TableName][filter.ColumnName][division]

	str := fmt.Sprintf("%v", index.Parsed_Document[filter.ColumnName])

	intValue, _ := strconv.Atoi(str)

	fmt.Println("VALUE::Binary")
	fmt.Println(intValue)

	if intValue > value {
		if division > 0 {
			indexPrevious := *singletonIndex.btreeIndex[filter.TableName][filter.ColumnName][division-1]
			strPrev := fmt.Sprintf("%v", indexPrevious.Parsed_Document[filter.ColumnName])

			prevIntValue, _ := strconv.Atoi(strPrev)
			if prevIntValue < value {
				if filter.Operator == "bigger_than" || filter.Operator == ">=" {
					return division, false
				} else if filter.Operator == "smaller_than" || filter.Operator == "<=" {
					return division - 1, false
				} else if filter.Operator == "equals" {
					return -1, false
				}
			}
		} else {
			if filter.Operator == "bigger_than" || filter.Operator == ">=" {
				return division, false
			} else {
				return -1, false
			}
		}

		return GetBinaryIndex(value, low, division, filter)
	} else if intValue < value {

		if division < len(singletonIndex.btreeIndex[filter.TableName][filter.ColumnName])-1 {
			indexPrevious := *singletonIndex.btreeIndex[filter.TableName][filter.ColumnName][division+1]
			strPrev := fmt.Sprintf("%v", indexPrevious.Parsed_Document[filter.ColumnName])

			prevIntValue, _ := strconv.Atoi(strPrev)
			if prevIntValue > value {
				if filter.Operator == "smaller_than" || filter.Operator == "<=" {
					return division, false
				} else if filter.Operator == "bigger_than" || filter.Operator == ">=" {
					return division + 1, false
				} else if filter.Operator == "equals" {
					return -1, false
				}
			}
		} else {
			if filter.Operator == "smaller_than" || filter.Operator == "<=" {
				return division, false
			} else {
				return -1, false
			}
		}

		return GetBinaryIndex(value, division, high, filter)
	} else if intValue == value {
		return division, true
	}

	return -1, false
}
