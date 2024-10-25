package client

import (
	"encoding/csv"
	"fmt"
	"math"
	"net"
	"os"
	"postgres_tpch_go_lib/src/proto"
	"postgres_tpch_go_lib/src/tpch"
	"strconv"
	"strings"
	"time"
)

//TODO: Check if when we give a slice with wrong size to hold the result, if bun allocates a new one with the right size or not.

/*type Query interface {
	DoQuery()
	GetQueryStats()
}*/

//getReadsFuncs                 []func(QueryClient, []crdt.ReadObjectParams, []crdt.ReadObjectParams, []int, int) int

type BenchmarkResults struct {
	Id                  int64
	Total               MixStatistics
	IntermediateResults []MixStatistics
}

type RoutineUpdData struct {
	Orders, Items [][]string
	Deletes       []int32
	LineSizes     []int
	Region        int
}

var (
	STOP_QUERIES      bool
	collectQueryStats []bool //One entry per client
)

func prepareQueryClientsBenchmark(baseClient PgQueryClient, queries []int, startTime int64, updateDataChan chan UpdateData) {
	clients := make([]*PgQueryClient, N_CLIENTS)
	collectQueryStats = make([]bool, N_CLIENTS)
	baseSeed := time.Now().UnixNano()
	for i := int64(0); i < N_CLIENTS; i++ {
		if IS_REDIRECT {
			newConn, _ := net.Dial("tcp", IP_DSN)
			clients[i] = CreatePGQueryClient(DBInfo{conn: newConn}, baseClient.SQLTables, baseClient.Sf, baseSeed+i, i, queries)
		} else {
			clients[i] = CreatePGQueryClient(baseClient.DBInfo, baseClient.SQLTables, baseClient.Sf, baseSeed+i, i, queries)
		}
		collectQueryStats[i] = false
	}
	STOP_QUERIES = false
	resultChan := make(chan *BenchmarkResults, N_CLIENTS)

	//TODO: With N_UPD_CLIENTS > 0, we should not split the updates between goroutines.
	if DOES_UPDATES && UPDATE_RATE > 0 {
		fmt.Println("Waiting for updateDataChan")
		updData := <-updateDataChan
		fmt.Println("Received updateDataChan")
		for i, client := range clients {
			client.UpdData = RoutineUpdData{Orders: updData.RoutineOrders[i], Items: updData.RoutineItems[i],
				Deletes: updData.RoutineDeletes[i], LineSizes: updData.RoutineLineSizes[i], Region: updData.RegionPerClient[i]}
			for j := 0; j < QUERIES_PER_CYCLE; j++ {
				client.LineF += client.UpdData.LineSizes[j]
			}
		}
	}

	currTime := time.Now()
	sleepTime := QUERY_WAIT - time.Duration(currTime.UnixNano()-startTime)
	fmt.Println("Sleeping at", currTime.String(), "for", int64(sleepTime/time.Millisecond), "ms.")
	time.Sleep(sleepTime)
	fmt.Println("Benchmark started at", time.Now().String())
	go doMixedStatsInterval()

	if UPDATE_RATE > 0 && N_UPD_CLIENTS > 0 && DOES_UPDATES && N_CLIENTS > int64(N_UPD_CLIENTS) {
		for _, client := range clients[:N_UPD_CLIENTS] {
			go client.doBench(resultChan)
		}
		for _, client := range clients[N_UPD_CLIENTS:] {
			go client.doQueryBench(resultChan)
		}
	} else if DOES_UPDATES && UPDATE_RATE > 0 {
		for _, client := range clients {
			go client.doBench(resultChan)
		}
	} else {
		for _, client := range clients {
			go client.doQueryBench(resultChan)
		}
	}
	fmt.Printf("Test duration: %dms. Will sleep until the test is over. Sleep start: %s\n", int64(TEST_DURATION/time.Millisecond), time.Now().String())
	time.Sleep(TEST_DURATION)
	STOP_QUERIES = true
	stopTime := time.Now().UnixNano()
	fmt.Println()
	fmt.Println("Test time is over.")
	results := make([]BenchmarkResults, len(clients))

	go func() {
		var currResult BenchmarkResults
		for range clients {
			currResult = *<-resultChan
			results[currResult.Id] = currResult
		}
		fmt.Printf("Time (ms) from test end until all clients replied: %d (ms)\n", (time.Now().UnixNano()-stopTime)/1000000)
		fmt.Println("All clients have finished.")
		var totalQueries, totalReads, totalRows, totalFields, avgDuration, avgLatency /*, currDuration*/ float64
		var totalNewOrders, totalNewItems, totalDelOrders, totalDelItems, totalUpdTxns, avgUpdLatency float64
		sToNs, msToNs := 1000000000.0, int64(1000000)
		for _, result := range results {
			//currDuration = float64(result.Total.duration)
			/*fmt.Printf("%d: Queries: %d. Query/s: %f. Reads: %d. Reads/s: %f. Rows: %d. Rows/query: %f. Rows/s: %f. Fields: %d. Fields/query: %f. Fields/s: %f. Avg latency: %f.\n",
			result.Id, result.Total.nQueries, (float64(result.Total.nQueries)/currDuration)*sToNs, result.Total.nReads, (float64(result.Total.nReads)/currDuration)*sToNs,
			result.Total.nRows, float64(result.Total.nRows)/float64(result.Total.nQueries), (float64(result.Total.nRows)/currDuration)*sToNs, result.Total.nResultFields,
			(float64(result.Total.nResultFields)/currDuration)*sToNs, float64(result.Total.nResultFields)/float64(result.Total.nQueries),
			float64(result.Total.queryLatency/msToNs)/float64(result.Total.nQueries))*/
			totalQueries += float64(result.Total.nQueries)
			totalReads += float64(result.Total.nReads)
			totalRows += float64(result.Total.nRows)
			totalFields += float64(result.Total.nResultFields)
			avgDuration += float64(result.Total.duration)
			avgLatency += float64(result.Total.queryLatency)
			totalNewOrders += float64(result.Total.nNewOrders)
			totalNewItems += float64(result.Total.nNewItems)
			totalDelOrders += float64(result.Total.nDelOrders)
			totalDelItems += float64(result.Total.nDelItems)
			totalUpdTxns += float64(result.Total.nUpdTxns)
			avgUpdLatency += float64(result.Total.updLatency)
		}
		avgDuration /= float64(len(results))
		if N_UPD_CLIENTS > 0 && UPDATE_RATE > 0 && DOES_UPDATES {
			avgLatency /= float64(N_CLIENTS - int64(N_UPD_CLIENTS))
			avgUpdLatency /= float64(N_UPD_CLIENTS)
		} else {
			avgLatency /= float64(len(results))
			avgUpdLatency /= float64(len(results))
		}
		totalNewUpds, totalDelUpds := totalNewOrders+totalNewItems, totalDelOrders+totalDelItems
		totalUpds := totalNewUpds + totalDelUpds
		totalNewFields := totalNewOrders*float64(N_ORDER_FIELDS) + totalNewItems*float64(N_ITEM_FIELDS)
		totalDelFields := totalDelOrders*float64(N_ORDER_FIELDS) + totalDelItems*float64(N_ITEM_FIELDS)

		writeStatsFiles(results) //Save statistics before printing the results, ensuring results are the last output.
		/*
				nNewOrders, nNewItems int
			nDelOrders, nDelItems int
			//Rows and fields can be calculated from the above.
			nUpdTxns   int   //new and del are together in one txn.
			updLatency int64 //ns, later converted to ms.
		*/
		/*
				totalNewUpds, totalDelUpds := stats.Total.nNewOrders+stats.Total.nNewItems, stats.Total.nDelOrders+stats.Total.nDelItems
			totalUpds := totalNewUpds + totalDelUpds
			totalNewFields := stats.Total.nNewOrders*N_ORDER_FIELDS + stats.Total.nNewItems*N_ITEM_FIELDS
			totalDelFields := stats.Total.nDelOrders*N_ORDER_FIELDS + stats.Total.nDelItems*N_ITEM_FIELDS

			fmt.Printf("Update client %d finished. Total updates: %d (txns: %d). Total new orders|items: %d|%d. Total del orders|items: %d|%d. Avg items/order (new|del): %f|%f. "+
				"Total new|del fields: %d|%d. Total duration: %dms. Total latency: %dms. Latency/txn: %fms.", client.Id, totalUpds, stats.Total.nUpdTxns, stats.Total.nNewOrders, stats.Total.nNewItems,
				stats.Total.nDelOrders, stats.Total.nDelItems, float64(stats.Total.nNewItems)/float64(stats.Total.nNewOrders), float64(stats.Total.nDelItems)/float64(stats.Total.nDelOrders),
				totalNewFields, totalDelFields, stats.Total.duration/int(time.Millisecond), stats.Total.updLatency/int64(time.Millisecond), float64(stats.Total.updLatency)/float64(stats.Total.nUpdTxns*int(time.Millisecond)))

		*/
		fmt.Printf("Totals: Queries: %f. Query/s: %f. Reads: %f. Reads/s: %f. Rows: %f. Rows/query: %f. Rows/s: %f. Fields: %f. Fields/query: %f. Fields/s: %f. Avg query latency: %f.\n",
			totalQueries, (totalQueries/avgDuration)*sToNs, totalReads, (totalReads/avgDuration)*sToNs, totalRows, totalRows/totalQueries, (totalRows/avgDuration)*sToNs,
			totalFields, (totalFields / totalQueries), (totalFields/avgDuration)*sToNs, (avgLatency/float64(msToNs))/totalQueries)
		fmt.Printf("Total upds|txns: %f|%f. Total new orders|items: %f|%f. Total del orders|items: %f|%f. Avg items/order (new|del): %f|%f. "+
			"Total new|del fields: %f|%f. Avg update latency: %f.\n", totalUpds, totalUpdTxns, totalNewOrders, totalNewItems, totalDelOrders, totalDelItems,
			totalNewItems/totalNewOrders, totalDelItems/totalDelOrders, totalNewFields, totalDelFields, (avgUpdLatency/float64(msToNs))/totalUpdTxns)

		os.Exit(0)
	}()

	time.Sleep(10 * time.Second)
	for i, result := range results {
		if result.Total.nQueries == 0 {
			fmt.Printf("Client %d has not yet finished!\n", i)
		}
	}
}

func (client *PgQueryClient) doBench(resultChan chan *BenchmarkResults) {
	startTime := time.Now().UnixNano()
	lastTime := startTime
	stats := &BenchmarkResults{Id: client.Id, IntermediateResults: make([]MixStatistics, 0, (TEST_DURATION)/(STATISTICS_INTERVAL)+1)}
	currQi, nQueries := 0, len(client.QueryFuncs)
	fmt.Println("Client", client.Id, "started. QueryFuncs:", client.QueryFuncs)
	var currTime int64
	var random float64
	if QUERIES_PER_CYCLE == 1 {
		for !STOP_QUERIES {
			if collectQueryStats[client.Id] {
				//fmt.Println("Client", client.Id, "collecting stats.")
				currTime = time.Now().UnixNano()
				client.updateBenchmarkStats(stats, startTime, lastTime, currTime)
				lastTime = currTime
			}
			random = client.Rng.Float64()
			if random < UPDATE_RATE {
				client.singleUpdateHelper()
			} else {
				currQi = client.singleQueryHelper(currQi, nQueries)
			}
		}
	} else {
		for !STOP_QUERIES {
			if collectQueryStats[client.Id] {
				//fmt.Println("Client", client.Id, "collecting stats.")
				currTime = time.Now().UnixNano()
				client.updateBenchmarkStats(stats, startTime, lastTime, currTime)
				lastTime = currTime
			}
			random = client.Rng.Float64()
			if random < UPDATE_RATE {
				client.multiUpdateHelper()
			} else {
				currQi = client.multiQueryHelper(currQi, nQueries)
			}
		}
	}
	endTime := time.Now().UnixNano()
	client.updateBenchmarkStats(stats, startTime, lastTime, endTime)
	fmt.Printf("Mix client %d finished.\n", client.Id)
	//TODO: Include update statistics on print
	/*fmt.Println("Query client", client.Id, "finished. Total queries:", stats.Total.nQueries, "Total query txns:", stats.Total.nQueryTxns, "Total reads:",
	stats.Total.nReads, "Total rows:", stats.Total.nRows, "Total read fields:", stats.Total.nReadFields, "Total result fields:", stats.Total.nResultFields,
	"Total duration:", stats.Total.duration, "Total latency:", stats.Total.queryLatency)*/
	resultChan <- stats
}

func (client *PgQueryClient) doUpdateBench(resultChan chan *BenchmarkResults) {
	startTime := time.Now().UnixNano()
	lastTime := startTime
	stats := &BenchmarkResults{Id: client.Id, IntermediateResults: make([]MixStatistics, 0, (TEST_DURATION)/(STATISTICS_INTERVAL)+1)}
	fmt.Println("UpdateClient", client.Id, "started.")
	var currTime int64
	if QUERIES_PER_CYCLE == 1 {
		for !STOP_QUERIES {
			if collectQueryStats[client.Id] {
				//fmt.Println("Client", client.Id, "collecting stats.")
				currTime = time.Now().UnixNano()
				client.updateBenchmarkStats(stats, startTime, lastTime, currTime)
				lastTime = currTime
			}
			client.singleUpdateHelper()
		}
	} else {
		for !STOP_QUERIES {
			if collectQueryStats[client.Id] {
				//fmt.Println("Client", client.Id, "collecting stats.")
				currTime = time.Now().UnixNano()
				client.updateBenchmarkStats(stats, startTime, lastTime, currTime)
				lastTime = currTime
			}
			client.multiUpdateHelper()
		}
	}
	endTime := time.Now().UnixNano()
	client.updateBenchmarkStats(stats, startTime, lastTime, endTime)
	/*totalNewUpds, totalDelUpds := stats.Total.nNewOrders+stats.Total.nNewItems, stats.Total.nDelOrders+stats.Total.nDelItems
	totalUpds := totalNewUpds + totalDelUpds
	totalNewFields := stats.Total.nNewOrders*N_ORDER_FIELDS + stats.Total.nNewItems*N_ITEM_FIELDS
	totalDelFields := stats.Total.nDelOrders*N_ORDER_FIELDS + stats.Total.nDelItems*N_ITEM_FIELDS*/

	fmt.Printf("Update client %d finished.\n", client.Id)
	/*fmt.Printf("Update client %d finished. Total updates: %d (txns: %d). Total new orders|items: %d|%d. Total del orders|items: %d|%d. Avg items/order (new|del): %f|%f. "+
	"Total new|del fields: %d|%d. Total duration: %dms. Total latency: %dms. Latency/txn: %fms.", client.Id, totalUpds, stats.Total.nUpdTxns, stats.Total.nNewOrders, stats.Total.nNewItems,
	stats.Total.nDelOrders, stats.Total.nDelItems, float64(stats.Total.nNewItems)/float64(stats.Total.nNewOrders), float64(stats.Total.nDelItems)/float64(stats.Total.nDelOrders),
	totalNewFields, totalDelFields, stats.Total.duration/int(time.Millisecond), stats.Total.updLatency/int64(time.Millisecond), float64(stats.Total.updLatency)/float64(stats.Total.nUpdTxns*int(time.Millisecond)))*/
	resultChan <- stats
}

func (client *PgQueryClient) doQueryBench(resultChan chan *BenchmarkResults) {
	startTime := time.Now().UnixNano()
	lastTime := startTime
	stats := &BenchmarkResults{Id: client.Id, IntermediateResults: make([]MixStatistics, 0, (TEST_DURATION)/(STATISTICS_INTERVAL)+1)}
	currQi, nQueries := 0, len(client.QueryFuncs)
	fmt.Println("QueryClient", client.Id, "started. QueryFuncs:", client.QueryFuncs)
	var currTime int64
	if QUERIES_PER_CYCLE == 1 {
		for !STOP_QUERIES {
			if collectQueryStats[client.Id] {
				//fmt.Println("Client", client.Id, "collecting stats.")
				currTime = time.Now().UnixNano()
				client.updateBenchmarkStats(stats, startTime, lastTime, currTime)
				lastTime = currTime
			}
			currQi = client.singleQueryHelper(currQi, nQueries)
		}
	} else {
		for !STOP_QUERIES {
			if collectQueryStats[client.Id] {
				//fmt.Println("Client", client.Id, "collecting stats.")
				currTime = time.Now().UnixNano()
				client.updateBenchmarkStats(stats, startTime, lastTime, currTime)
				lastTime = currTime
			}
			currQi = client.multiQueryHelper(currQi, nQueries)
		}
	}
	endTime := time.Now().UnixNano()
	client.updateBenchmarkStats(stats, startTime, lastTime, endTime)
	fmt.Printf("Query client %d finished.\n", client.Id)
	/*fmt.Println("Query client", client.Id, "finished. Total queries:", stats.Total.nQueries, "Total query txns:", stats.Total.nQueryTxns, "Total reads:",
	stats.Total.nReads, "Total rows:", stats.Total.nRows, "Total read fields:", stats.Total.nReadFields, "Total result fields:", stats.Total.nResultFields,
	"Total duration:", stats.Total.duration/int(time.Millisecond), "Total latency:", stats.Total.queryLatency/int64(time.Millisecond), "Avg latency/txn:",
	float64(stats.Total.queryLatency)/float64(stats.Total.nQueryTxns*int(time.Millisecond)))*/
	resultChan <- stats
}

func (client *PgQueryClient) singleQueryHelper(currQi, nQueries int) (newQi int) {
	qStart := recordStartLatency()
	//fmt.Printf("[PGQ%d]Started query at %s.\n", client.Id, time.Now().String())
	client.QueryFuncs[currQi]()
	timeTaken := recordFinishLatency(qStart)
	client.queryLatency += timeTaken
	//fmt.Printf("[PGQ%d]Finished query at %s. Time taken: %dms.\n", client.Id, time.Now().String(), timeTaken/int64(time.Millisecond))
	client.nQueryTxns++
	return (currQi + 1) % nQueries
}

func (client *PgQueryClient) multiQueryHelper(currQi, nQueries int) (newQi int) {
	qStart := recordStartLatency()
	client.QProtoI = 0
	for i := 0; i < QUERIES_PER_CYCLE; i++ {
		client.QueryFuncs[currQi]()
		currQi = (currQi + 1) % nQueries
	}
	tpch.SendProto(tpch.PB_MULTI_QUERY, client.CurrQProto, client.DBInfo.conn)
	_, pb, _ := tpch.ReceiveProto(client.conn)
	client.queryLatency += recordFinishLatency(qStart)
	client.nQueryTxns++
	client.multiQueryCountStats(pb.(*proto.MultiQueryResp))
	return currQi
}

func (client *PgQueryClient) multiQueryCountStats(respPb *proto.MultiQueryResp) {
	for i, query := range client.CurrQProto.GetQueries() {
		switch query.GetQueryId() {
		case 1:
			client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+2, client.nRows+8
			client.nReadFields, client.nResultFields = client.nReadFields+80, client.nResultFields+80
		case 3:
			client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+10
			client.nReadFields, client.nResultFields = client.nReadFields+40, client.nResultFields+40
		case 5:
			client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+5
			client.nReadFields, client.nResultFields = client.nReadFields+10, client.nResultFields+10
		case 6:
			client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
			client.nReadFields, client.nResultFields = client.nReadFields+1, client.nResultFields+1
		case 11:
			client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+2, client.nRows+101
			client.nReadFields, client.nResultFields = client.nReadFields+202, client.nResultFields+200
		case 14:
			client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
			client.nReadFields, client.nResultFields = client.nReadFields+1, client.nResultFields+1
		case 15:
			client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
			client.nReadFields, client.nResultFields = client.nReadFields+5, client.nResultFields+5
		case 18:
			results := respPb.GetResults()[i].GetResults() //6 columns per row
			nRows, nFields := len(results)/6, len(results)
			client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+nRows
			client.nReadFields, client.nResultFields = client.nReadFields+nFields, client.nResultFields+nFields
		}
	}
}

func (client *PgQueryClient) multiUpdateHelper() {
	orderF := client.OrderI + QUERIES_PER_CYCLE
	newOrdersS, remOrderKeys, newLineSizes := client.UpdData.Orders[client.OrderI:orderF], client.UpdData.Deletes[client.OrderI:orderF], client.UpdData.LineSizes[client.OrderI:orderF]
	nNewItems, nRemItems := 0, 0
	for _, nItems := range newLineSizes {
		nNewItems += int(nItems)
	}
	for _, remOrderId := range remOrderKeys {
		nRemItems += int(client.SQLTables.ItemsPerOrder[client.SQLTables.GetOrderIndex(remOrderId)])
	}
	newItemsS := make([][]string, nNewItems)
	uStart := recordStartLatency()
	client.MultiUpdFunc(newOrdersS, newItemsS, remOrderKeys, nRemItems)
	client.updLatency += recordFinishLatency(uStart)
	client.nUpdTxns++
	client.OrderI += QUERIES_PER_CYCLE
	if client.OrderI+QUERIES_PER_CYCLE >= len(client.UpdData.Orders) {
		client.OrderI, client.LineI, client.LineF = 0, 0, 0
	} else {
		client.LineI = client.LineF
	}
	for i := client.OrderI; i < client.OrderI+QUERIES_PER_CYCLE; i++ {
		client.LineF += client.UpdData.LineSizes[i]
	}
}

func (client *PgQueryClient) singleUpdateHelper() {
	nDelItems := int(client.SQLTables.ItemsPerOrder[client.SQLTables.GetOrderIndex(client.UpdData.Deletes[client.OrderI])])
	newOrder := client.SQLTables.CreateOrder(client.UpdData.Orders[client.OrderI])
	newItems := client.SQLTables.CreateLineitemsOfOrder(client.UpdData.Items[client.LineI:client.LineF])
	uStart := recordStartLatency()
	fmt.Printf("[PGQ%d]Started update at %s.\n", client.Id, time.Now().String())
	client.UpdateFunc(newOrder, newItems, client.UpdData.Deletes[client.OrderI], nDelItems)
	timeTaken := recordFinishLatency(uStart)
	client.updLatency += timeTaken
	fmt.Printf("[PGQ%d]Finished update at %s. Time taken: %dms.\n", client.Id, time.Now().String(), timeTaken/int64(time.Millisecond))
	client.nUpdTxns++
	client.OrderI++
	if client.OrderI == len(client.UpdData.Orders) {
		client.OrderI, client.LineI, client.LineF = 0, 0, client.UpdData.LineSizes[0]
	} else {
		client.LineI = client.LineF
		client.LineF += client.UpdData.LineSizes[client.OrderI]
	}
}

// Time: ms
func (client *PgQueryClient) updateBenchmarkStats(stats *BenchmarkResults, startTime, lastTime, currTime int64) {
	diffT := currTime - lastTime
	if diffT < int64(STATISTICS_INTERVAL/8) && len(stats.IntermediateResults) > 0 {
		//Replace
		lastStat := stats.IntermediateResults[len(stats.IntermediateResults)-1]
		lastStat.duration += int(diffT)
		//QueryStatistics
		lastStat.nQueries += client.nQueries
		lastStat.nQueryTxns += client.nQueryTxns
		lastStat.nReads += client.nReads
		lastStat.nRows += client.nRows
		lastStat.nReadFields += client.nReadFields
		lastStat.nResultFields += client.nResultFields
		lastStat.queryLatency += client.queryLatency
		//UpdateStatistics
		lastStat.nNewOrders += client.nNewOrders
		lastStat.nNewItems += client.nNewItems
		lastStat.nDelOrders += client.nDelOrders
		lastStat.nDelItems += client.nDelItems
		lastStat.nUpdTxns += client.nUpdTxns
		lastStat.updLatency += client.updLatency
		stats.IntermediateResults[len(stats.IntermediateResults)-1] = lastStat
	} else {
		client.QueryStatistics.duration += int(diffT)
		stats.IntermediateResults = append(stats.IntermediateResults, client.MixStatistics)
	}
	if len(stats.IntermediateResults) > 1 { //Ignore first one as warmup
		stats.Total.duration += client.QueryStatistics.duration
		//QueryStatistics
		stats.Total.nQueries += client.QueryStatistics.nQueries
		stats.Total.nQueryTxns += client.QueryStatistics.nQueryTxns
		stats.Total.nReads += client.QueryStatistics.nReads
		stats.Total.nRows += client.QueryStatistics.nRows
		stats.Total.nResultFields += client.QueryStatistics.nResultFields
		stats.Total.nReadFields += client.QueryStatistics.nReadFields
		stats.Total.queryLatency += client.QueryStatistics.queryLatency
		//UpdateStatistics
		stats.Total.nNewOrders += client.MixStatistics.nNewOrders
		stats.Total.nNewItems += client.MixStatistics.nNewItems
		stats.Total.nDelOrders += client.MixStatistics.nDelOrders
		stats.Total.nDelItems += client.MixStatistics.nDelItems
		stats.Total.nUpdTxns += client.MixStatistics.nUpdTxns
		stats.Total.updLatency += client.MixStatistics.updLatency
	}

	client.MixStatistics = MixStatistics{}
	collectQueryStats[client.Id] = false
}

func doMixedStatsInterval() {
	i := 1
	for {
		newBoolSlice := make([]bool, N_CLIENTS)
		for i := range newBoolSlice {
			newBoolSlice[i] = true
		}
		time.Sleep(STATISTICS_INTERVAL)
		collectQueryStats = newBoolSlice
		fmt.Println("Time elapsed (aproximately):", time.Duration(i)*STATISTICS_INTERVAL, "ms.")
		i++
	}
}

func recordStartLatency() int64 {
	if RECORD_LATENCY {
		return time.Now().UnixNano()
	}
	return 0
}

func recordFinishLatency(startTime int64) int64 {
	if RECORD_LATENCY {
		return time.Now().UnixNano() - startTime
	}
	return 0
}

// Returns the stats in format [time][nclients] and prepare headers.
func writeStatsFiles(stats []BenchmarkResults) {
	totalStat, statPerTime, header := statsFileHelper(stats)
	writeTotalStatsFile(totalStat, statPerTime, header)
}

/*
[]string{"Total time", "Section time", "Queries cycles", "Query txns", "Queries", "Reads", "Rows", "Entries",
"Query cycles/s", "Query txn/s", "Query/s", "Read/s", "Row/s", "Entries/s",
"Update cycles", "Update txns", "Total Updates", "Total Orders", "Total Items", "Total Entries", "New Upds", "New Orders", "New Items", "New Entries",
"Del Upds", "Del Orders", "Del Items", "Del Entries",
"Upd txn/s", "Upd/s", "Upd entries/s", "New upds/s", "New orders/s", "New entries/s", "Del upds/s", "Del orders/s", "Del entries/s",
"AvgQ latency", "AvgU latency", "AvgAll latency (ms)", "Avg latency (ms)(AO)"}
*/
func writeTotalStatsFile(totalStat MixStatistics, statPerTime [][]MixStatistics, header []string) {
	totalData := make([][]string, len(statPerTime)+1) //space for final data as well

	totalTime, partQueries, partQTxns, partReads, partRows, partReadFields, partResultFields, partTime, partQTime := 0, 0, 0, 0, 0, 0, 0, 0, int64(0)
	partNewOrders, partNewItems, partDelOrders, partDelItems, partUpdTxns, partUTime := 0, 0, 0, 0, 0, int64(0)

	sumStats := make([]MixStatistics, len(statPerTime)+1)

	//fmt.Println("[PGQuery]WriteTotalStatsFile")
	//fmt.Println("StatPerTime len:", len(statPerTime), "TotalData len:", len(totalData))
	for i, partStats := range statPerTime {
		for _, clientStat := range partStats {
			partQueries += clientStat.nQueries
			partQTxns += clientStat.nQueryTxns
			partReads += clientStat.nReads
			partRows += clientStat.nRows
			partReadFields += clientStat.nReadFields
			partResultFields += clientStat.nResultFields
			partTime += clientStat.duration
			partQTime += clientStat.queryLatency
			partNewOrders += clientStat.nNewOrders
			partNewItems += clientStat.nNewItems
			partDelOrders += clientStat.nDelOrders
			partDelItems += clientStat.nDelItems
			partUpdTxns += clientStat.nUpdTxns
			partUTime += clientStat.updLatency
		}
		partTime /= int(N_CLIENTS)
		//partTime /= 1000000
		//partQTime /= 1000000
		totalTime += partTime
		/*fmt.Println("Time:", totalTime, "PartTime:", partTime, "PartQTime:", partQTime, "PartQueries:", partQueries,
		"PartQTxns:", partQTxns, "PartReads:", partReads, "PartRows:", partRows, "PartReadFields:", partReadFields,
		"PartResultFields:", partResultFields, "PartNewOrders:", partNewOrders, "PartNewItems:", partNewItems, "PartDelOrders:", partDelOrders,
		"PartDelItems:", partDelItems, "PartUpdTxns:", partUpdTxns, "PartTime:", partTime, "PartQTime:", partQTime, "PartUTime:", partUTime)*/
		sumStats[i] = MixStatistics{QueryStatistics: QueryStatistics{nQueries: partQueries, nQueryTxns: partQTxns, nReads: partReads, nRows: partRows,
			nReadFields: partReadFields, nResultFields: partResultFields, duration: partTime, queryLatency: partQTime},
			UpdateStatistics: UpdateStatistics{nNewOrders: partNewOrders, nNewItems: partNewItems, nDelOrders: partDelOrders, nDelItems: partDelItems, nUpdTxns: partUpdTxns, updLatency: partUTime}}
		totalData[i] = calculateStats(partQueries, partQTxns, partReads, partRows, partReadFields, partResultFields,
			partNewOrders, partNewItems, partDelOrders, partDelItems, partUpdTxns, totalTime, partTime, int(partQTime), int(partUTime))
		/*fmt.Println("SumStats:", sumStats[i])
		fmt.Println("TotalData:", totalData[i])
		fmt.Println()*/

		partQueries, partQTxns, partReads, partRows, partReadFields, partResultFields, partTime, partQTime = 0, 0, 0, 0, 0, 0, 0, 0
		partNewOrders, partNewItems, partDelOrders, partDelItems, partUpdTxns, partUTime = 0, 0, 0, 0, 0, 0
	}
	fmt.Println()

	fmt.Println("TotalStat:", totalStat)
	totalData[len(totalData)-1] = calculateStats(totalStat.nQueries, totalStat.nQueryTxns, totalStat.nReads, totalStat.nRows,
		totalStat.nReadFields, totalStat.nResultFields, totalStat.nNewOrders, totalStat.nNewItems, totalStat.nDelOrders, totalStat.nDelItems,
		totalStat.nUpdTxns, totalStat.duration/int(N_CLIENTS), totalStat.duration/int(N_CLIENTS), int(totalStat.queryLatency), int(totalStat.updLatency))
	fmt.Println("TotalData:", totalData[len(totalData)-1])

	writeDataToFile("mixStats", header, totalData)

}

func calculateStats(nQueries, nQueryTxns, nReads, nRows, nReadFields, nResultFields, nNewOrders, nNewItems, nDelOrders, nDelItems, nUpdTxns, totalTime, partTime, partQTime, partUTime int) (data []string) {
	SEC, MS := 1000000000.0, 1000000.0 //seconds, milliseconds
	queryCycles := nQueries / QUERIES_PER_CYCLE
	queryCycleS, queryS := (float64(queryCycles)/float64(partTime))*SEC, (float64(nQueries)/float64(partTime))*SEC
	readS, qTxnsS := (float64(nReads)/float64(partTime))*SEC, (float64(nQueryTxns)/float64(partTime))*SEC
	rowS, entriesS := (float64(nRows)/float64(partTime))*SEC, (float64(nReadFields)/float64(partTime))*SEC

	totalOrders, totalItems, newUpds, delUpds := nNewOrders+nDelOrders, nNewItems+nDelItems, nNewOrders+nNewItems, nDelOrders+nDelItems
	totalUpds, totalEntries := totalOrders+totalItems, totalOrders*N_ORDER_FIELDS+totalItems*N_ITEM_FIELDS
	newEntries, delEntries := nNewOrders*N_ORDER_FIELDS+nNewItems*N_ITEM_FIELDS, nDelOrders*N_ORDER_FIELDS+nDelItems*N_ITEM_FIELDS
	updTxnsS, updS, updEntriesS := (float64(nUpdTxns)/float64(partTime))*SEC, (float64(totalUpds)/float64(partTime))*SEC, (float64(totalEntries)/float64(partTime))*SEC
	newUpdsS, newOrdersS, newItemsS, newEntriesS := (float64(newUpds)/float64(partTime))*SEC, (float64(nNewOrders)/float64(partTime))*SEC, (float64(nNewItems)/float64(partTime))*SEC, (float64(newEntries)/float64(partTime))*SEC
	delUpdsS, delOrdersS, delItemsS, delEntriesS := (float64(delUpds)/float64(partTime))*SEC, (float64(nDelOrders)/float64(partTime))*SEC, (float64(nDelItems)/float64(partTime))*SEC, (float64(delEntries)/float64(partTime))*SEC
	updCycles := totalOrders / QUERIES_PER_CYCLE

	var latencyPerOp, latencyPerQTxn, latencyPerUTxn float64
	latencyPerOp = float64(partQTime+partUTime) / (float64(nQueryTxns+nUpdTxns) * MS)
	latencyPerQTxn = float64(partQTime) / (float64(nQueryTxns) * MS)
	latencyPerUTxn = float64(partUTime) / (float64(nUpdTxns) * MS)
	/*if N_UPD_CLIENTS > 0 && UPDATE_RATE > 0 && DOES_UPDATES {
		//latencyPerOp = float64(partQTime*int(N_CLIENTS-int64(N_UPD_CLIENTS))) / (float64(nQueries) * MS)
		latencyPerQTxn = float64(partQTime*int(N_CLIENTS-int64(N_UPD_CLIENTS))) / (float64(nQueryTxns) * MS)
		latencyPerUTxn = float64(partUTime*N_UPD_CLIENTS) / (float64(nUpdTxns) * MS)
	} else {
		//latencyPerOp = float64(partQTime*int(N_CLIENTS)) / (float64(nQueries) * MS)
		latencyPerQTxn = float64(partQTime*int(N_CLIENTS)) / (float64(nQueryTxns) * MS)
		latencyPerUTxn = float64(partUTime*int(N_CLIENTS)) / (float64(nUpdTxns) * MS)
	}*/

	data = []string{strconv.Itoa(totalTime / int(MS)), strconv.Itoa(partTime / int(MS)), strconv.Itoa(queryCycles),
		strconv.Itoa(nQueryTxns), strconv.Itoa(nQueries), strconv.Itoa(nReads), strconv.Itoa(nRows),
		strconv.Itoa(nReadFields), strconv.FormatFloat(queryCycleS, 'f', 10, 64), strconv.FormatFloat(qTxnsS, 'f', 10, 64),
		strconv.FormatFloat(queryS, 'f', 10, 64), strconv.FormatFloat(readS, 'f', 10, 64), strconv.FormatFloat(rowS, 'f', 10, 64),
		strconv.FormatFloat(entriesS, 'f', 10, 64),
		strconv.Itoa(updCycles), strconv.Itoa(nUpdTxns), strconv.Itoa(totalUpds), strconv.Itoa(totalOrders), strconv.Itoa(totalItems), strconv.Itoa(totalEntries),
		strconv.Itoa(newUpds), strconv.Itoa(nNewOrders), strconv.Itoa(nNewItems), strconv.Itoa(newEntries),
		strconv.Itoa(delUpds), strconv.Itoa(nDelOrders), strconv.Itoa(nDelItems), strconv.Itoa(delEntries),
		strconv.FormatFloat(updTxnsS, 'f', 10, 64), strconv.FormatFloat(updS, 'f', 10, 64), strconv.FormatFloat(updEntriesS, 'f', 10, 64),
		strconv.FormatFloat(newUpdsS, 'f', 10, 64), strconv.FormatFloat(newOrdersS, 'f', 10, 64), strconv.FormatFloat(newEntriesS, 'f', 10, 64),
		strconv.FormatFloat(delUpdsS, 'f', 10, 64), strconv.FormatFloat(delOrdersS, 'f', 10, 64), strconv.FormatFloat(delEntriesS, 'f', 10, 64),
		strconv.FormatFloat(latencyPerQTxn, 'f', 10, 64), strconv.FormatFloat(latencyPerUTxn, 'f', 10, 64), strconv.FormatFloat(latencyPerOp, 'f', 10, 64)}

	ignore(newItemsS)
	ignore(delItemsS)
	return
}

/*
"Total time", "Section time", "Queries cycles", "Query txns", "Queries", "Reads", "Rows", "Entries",
"Query cycles/s", "Query txn/s", "Query/s", "Read/s", "Row/s", "Entries/s",
"Update cycles", "Update txns", "Total Updates", "Total Orders", "Total Items", "Total Entries", "New Upds", "New Orders", "New Items", "New Entries",
"Del Upds", "Del Orders", "Del Items", "Del Entries",
"Upd txn/s", "Upd/s", "Upd entries/s", "New upds/s", "New orders/s", "New entries/s", "Del upds/s", "Del orders/s", "Del entries/s",
"AvgQ latency", "AvgU latency", "AvgAll latency (ms)", "Avg latency (ms)(AO)"}
*/

func statsFileHelper(stats []BenchmarkResults) (totalStat MixStatistics, statPerTime [][]MixStatistics, header []string) {
	totalStat, statPerTime = convertStats(stats)
	header = []string{"Total time", "Section time", "Queries cycles", "Query txns", "Queries", "Reads", "Rows", "Entries",
		"Query cycles/s", "Query txn/s", "Query/s", "Read/s", "Row/s", "Entries/s",
		"Update cycles", "Update txns", "Total Updates", "Total Orders", "Total Items", "Total Entries", "New Upds", "New Orders", "New Items", "New Entries",
		"Del Upds", "Del Orders", "Del Items", "Del Entries",
		"Upd txn/s", "Upd/s", "Upd entries/s", "New upds/s", "New orders/s", "New entries/s", "Del upds/s", "Del orders/s", "Del entries/s",
		"AvgQ latency", "AvgU latency", "AvgAll latency (ms)"} //, "Avg latency (ms)(AO)"}
	/*header = []string{"Total time", "Section time", "Queries cycles", "Queries", "Reads", "Query cycles/s", "Query/s", "Read/s",
	"Query txns", "Query txns/s", "Updates", "Updates", "New upds", "Del upds", "Index upds", "Update blocks", "Update blocks/s",
	"New+del upds", "New+del upd/s", "Update txns", "Update txns/s", "Ops_all", "Ops_all/s", "Ops", "Ops/s", "Txns", "Txn/s",
	"AvgQ latency", "AvgU latency", "AvgAll latency (ms)", "Avg latency (ms)(AO)", "Average latency (ms)(AOw/I)"}*/
	return
}

func convertStats(stats []BenchmarkResults) (totalStat MixStatistics, statsPerTime [][]MixStatistics) {
	sizeToUse := int(math.MaxInt32)
	for _, currStats := range stats {
		if len(currStats.IntermediateResults) < sizeToUse {
			sizeToUse = len(currStats.IntermediateResults)
		}
	}
	sizeToUse-- //Ignore warmup

	//fmt.Println()
	//fmt.Println("Stats:", stats, "TotalStats:", totalStat, "Len of stats (nClients):", len(stats))
	statsPerTime = make([][]MixStatistics, sizeToUse)
	//fmt.Println("Size of statsPerTime:", sizeToUse, "StatsPerTime:", statsPerTime, "SizeToUse:", sizeToUse)
	var currStatTime []MixStatistics
	for i := range statsPerTime {
		//fmt.Printf("Iterating %d. Inner size: %d\n", i, len(stats))
		currStatTime = make([]MixStatistics, len(stats))
		for j, stat := range stats {
			//fmt.Printf("Iterating %d.%d.\n", i, j)
			currStatTime[j] = stat.IntermediateResults[i+1] //Accounting for warmup
		}
		statsPerTime[i] = currStatTime
	}
	totalStat = MixStatistics{}
	//fmt.Println("Stats per time:", statsPerTime, "Len:", len(statsPerTime))
	//fmt.Println("Starting to iterate totalStat.")
	for _, stat := range stats {
		//fmt.Printf("Iterating totalStat. Stats: %v\n", stat)
		totalStat.nQueries += stat.Total.nQueries
		totalStat.nQueryTxns += stat.Total.nQueryTxns
		totalStat.nReads += stat.Total.nReads
		totalStat.nRows += stat.Total.nRows
		totalStat.nReadFields += stat.Total.nReadFields
		totalStat.nResultFields += stat.Total.nResultFields
		totalStat.queryLatency += stat.Total.queryLatency
		totalStat.duration += stat.Total.duration
		totalStat.nNewOrders += stat.Total.nNewOrders
		totalStat.nNewItems += stat.Total.nNewItems
		totalStat.nDelOrders += stat.Total.nDelOrders
		totalStat.nDelItems += stat.Total.nDelItems
		totalStat.nUpdTxns += stat.Total.nUpdTxns
		totalStat.updLatency += stat.Total.updLatency
	}
	//fmt.Println()
	return
}

func writeDataToFile(filename string, header []string, data [][]string) {
	file := getStatsFileToWrite(filename)
	if file == nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	writer.Write(header)
	for _, line := range data {
		writer.Write(line)
	}
	fmt.Println("Mix statistics saved successfully to " + file.Name())
}

func getStatsFileToWrite(filename string) (file *os.File) {
	if STATISTICS_INTERVAL == -1 {
		fmt.Println("Not writing stats file as statisticsInterval is -1.")
		return
	}
	//os.Mkdir(statsSaveLocation, os.ModeDir)
	filenameParts := strings.Split(filename, "/") //filename may also have folders
	fullLoc := STATS_SAVE_LOCATION
	for i := 0; i < len(filenameParts)-1; i++ {
		fullLoc += filenameParts[i] + "/"
	}
	filename = filenameParts[len(filenameParts)-1]

	os.MkdirAll(fullLoc, 0777)
	fileCreated := false
	//for i := int64(0); !fileCreated; i++ {
	for !fileCreated {
		_, err := os.Stat(fullLoc + filename + ID + ".csv")
		if err != nil {
			fileCreated = true
			file, err = os.Create(fullLoc + filename + ID + ".csv")
			if err != nil {
				fmt.Println("[DATASAVE][ERROR]Failed to create stats file with name "+filename+ID+".csv. Error:", err)
				return
			}
		} else {
			ID = string(ID[0]) + ID
		}
	}
	return
}

/*
type BenchmarkResults struct {
	Id                  int64
	Total               QueryStatistics
	IntermediateResults []QueryStatistics
}

type QueryStatistics struct {
	nQueries      int
	nReads        int
	nRows         int
	nResultFields int   //Number of fields shown to the user
	nReadFields   int   //Number of actual fields downloaded
	duration      int   //ms
	latency       int64 //ns, later converted to ms.
}
*/
