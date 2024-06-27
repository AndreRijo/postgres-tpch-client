package client

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
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
	Total               QueryStatistics
	IntermediateResults []QueryStatistics
}

var (
	STOP_QUERIES      bool
	collectQueryStats []bool //One entry per client
)

func prepareQueryClientsBenchmark(baseClient PgQueryClient, queries []int, startTime int64) {
	clients := make([]*PgQueryClient, N_CLIENTS)
	collectQueryStats = make([]bool, N_CLIENTS)
	baseSeed := time.Now().UnixNano()
	for i := int64(0); i < N_CLIENTS; i++ {
		clients[i] = CreatePGQueryClient(baseClient.DBInfo, baseClient.SQLTables, baseClient.Sf, baseSeed+i, i, queries)
		collectQueryStats[i] = false
	}
	STOP_QUERIES = false
	resultChan := make(chan *BenchmarkResults, N_CLIENTS)

	currTime := time.Now()
	sleepTime := QUERY_WAIT - time.Duration(currTime.UnixNano()-startTime)*time.Nanosecond
	fmt.Println("Sleeping at", currTime.String(), "for", int64(sleepTime/time.Millisecond), "ms.")
	time.Sleep(sleepTime)
	fmt.Println("Benchmark started at", time.Now().String())
	go doMixedStatsInterval()

	for _, client := range clients {
		go client.doBench(resultChan)
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
		var totalQueries, totalReads, totalRows, totalFields, avgDuration, avgLatency, currDuration float64
		for _, result := range results {
			currDuration = float64(result.Total.duration)
			fmt.Printf("%d: Queries: %d. Query/s: %f. Reads: %d. Reads/s: %f. Rows: %d. Rows/query: %f. Rows/s: %f. Fields: %d. Fields/query: %f. Fields/s: %f. Avg latency: %f.\n",
				result.Id, result.Total.nQueries, (float64(result.Total.nQueries)/currDuration)*1000, result.Total.nReads, (float64(result.Total.nReads)/currDuration)*1000,
				result.Total.nRows, float64(result.Total.nRows)/float64(result.Total.nQueries), (float64(result.Total.nRows)/currDuration)*1000, result.Total.nResultFields,
				(float64(result.Total.nResultFields)/currDuration)*1000, float64(result.Total.nResultFields)/float64(result.Total.nQueries),
				float64(result.Total.latency/1000000)/float64(result.Total.nQueries))
			totalQueries += float64(result.Total.nQueries)
			totalReads += float64(result.Total.nReads)
			totalRows += float64(result.Total.nRows)
			totalFields += float64(result.Total.nResultFields)
			avgDuration += float64(result.Total.duration)
			avgLatency += float64(result.Total.latency)
		}
		avgDuration /= float64(len(results))
		avgLatency /= float64(len(results))
		fmt.Printf("Totals: Queries: %f. Query/s: %f. Reads: %f. Reads/s: %f. Rows: %f. Rows/query: %f. Rows/s: %f. Fields: %f. Fields/query: %f. Fields/s: %f. Avg latency: %f.\n",
			totalQueries, (totalQueries/avgDuration)*1000, totalReads, (totalReads/avgDuration)*1000, totalRows, totalRows/totalQueries, (totalRows/avgDuration)*1000,
			totalFields, (totalFields / totalQueries), (totalFields/avgDuration)*1000, (avgLatency/1000000)/totalQueries)

		writeStatsFiles(results)

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
	stats := &BenchmarkResults{Id: client.Id, IntermediateResults: make([]QueryStatistics, 0, (TEST_DURATION)/(STATISTICS_INTERVAL)+1)}
	currQi := 0
	nQueries := len(client.QueryFuncs)
	fmt.Println("Client", client.Id, "started. QueryFuncs:", client.QueryFuncs)
	var qStart, currTime int64
	for !STOP_QUERIES {
		if collectQueryStats[client.Id] {
			//fmt.Println("Client", client.Id, "collecting stats.")
			currTime = time.Now().UnixNano()
			client.updateBenchmarkStats(stats, startTime, lastTime, currTime)
			lastTime = currTime
			collectQueryStats[client.Id] = false
		}
		qStart = recordStartLatency()
		client.QueryFuncs[currQi]()
		client.latency += recordFinishLatency(qStart)
		client.nQueryTxns++
		currQi = (currQi + 1) % nQueries
	}
	endTime := time.Now().UnixNano()
	client.updateBenchmarkStats(stats, startTime, lastTime, endTime)
	fmt.Println("Client", client.Id, "finished. Total queries:", stats.Total.nQueries, "Total query txns:", stats.Total.nQueryTxns, "Total reads:",
		stats.Total.nReads, "Total rows:", stats.Total.nRows, "Total read fields:", stats.Total.nReadFields, "Total result fields:", stats.Total.nResultFields,
		"Total duration:", stats.Total.duration, "Total latency:", stats.Total.latency)
	resultChan <- stats
}

// Time: ms
func (client *PgQueryClient) updateBenchmarkStats(stats *BenchmarkResults, startTime, lastTime, currTime int64) {
	diffT := currTime - lastTime
	if diffT < int64(STATISTICS_INTERVAL/8) && len(stats.IntermediateResults) > 0 {
		//Replace
		lastStat := stats.IntermediateResults[len(stats.IntermediateResults)-1]
		lastStat.duration += int(diffT)
		lastStat.nQueries += client.nQueries
		lastStat.nQueryTxns += client.nQueryTxns
		lastStat.nReads += client.nReads
		lastStat.nRows += client.nRows
		lastStat.nReadFields += client.nReadFields
		lastStat.nResultFields += client.nResultFields
		lastStat.latency += client.latency
		stats.IntermediateResults[len(stats.IntermediateResults)-1] = lastStat
	} else {
		client.QueryStatistics.duration += int(diffT)
		stats.IntermediateResults = append(stats.IntermediateResults, client.QueryStatistics)
	}
	if len(stats.IntermediateResults) > 1 { //Ignore first one as warmup
		stats.Total.duration += client.QueryStatistics.duration
		stats.Total.nQueries += client.QueryStatistics.nQueries
		stats.Total.nQueryTxns += client.QueryStatistics.nQueryTxns
		stats.Total.nReads += client.QueryStatistics.nReads
		stats.Total.nRows += client.QueryStatistics.nRows
		stats.Total.nResultFields += client.QueryStatistics.nResultFields
		stats.Total.nReadFields += client.QueryStatistics.nReadFields
		stats.Total.latency += client.QueryStatistics.latency
	}

	client.QueryStatistics = QueryStatistics{}
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

func writeTotalStatsFile(totalStat QueryStatistics, statPerTime [][]QueryStatistics, header []string) {
	totalData := make([][]string, len(statPerTime)+1) //space for final data as well

	totalTime, partQueries, partQTxns, partReads, partRows, partReadFields, partResultFields, partTime, partQTime := 0, 0, 0, 0, 0, 0, 0, 0, int64(0)

	sumStats := make([]QueryStatistics, len(statPerTime)+1)

	fmt.Println("[PGQuery]WriteTotalStatsFile")
	fmt.Println("StatPerTime len:", len(statPerTime), "TotalData len:", len(totalData))
	for i, partStats := range statPerTime {
		for _, clientStat := range partStats {
			partQueries += clientStat.nQueries
			partQTxns += clientStat.nQueryTxns
			partReads += clientStat.nReads
			partRows += clientStat.nRows
			partReadFields += clientStat.nReadFields
			partResultFields += clientStat.nResultFields
			partTime += clientStat.duration
			partQTime += clientStat.latency
		}
		partTime /= int(N_CLIENTS)
		//partTime /= 1000000
		//partQTime /= 1000000
		totalTime += partTime
		fmt.Println("Time:", totalTime, "PartTime:", partTime, "PartQTime:", partQTime, "PartQueries:", partQueries,
			"PartQTxns:", partQTxns, "PartReads:", partReads, "PartRows:", partRows, "PartReadFields:", partReadFields,
			"PartResultFields:", partResultFields, "PartTime:", partTime, "PartQTime:", partQTime)
		sumStats[i] = QueryStatistics{nQueries: partQueries, nQueryTxns: partQTxns, nReads: partReads, nRows: partRows,
			nReadFields: partReadFields, nResultFields: partResultFields, duration: partTime, latency: partQTime}
		totalData[i] = calculateStats(partQueries, partQTxns, partReads, partRows, partReadFields, partResultFields,
			totalTime, partTime, int(partQTime))
		fmt.Println("SumStats:", sumStats[i])
		fmt.Println("TotalData:", totalData[i])
		fmt.Println()

		partQueries, partQTxns, partReads, partRows, partReadFields, partResultFields, partTime, partQTime = 0, 0, 0, 0, 0, 0, 0, 0
	}
	fmt.Println()

	fmt.Println("TotalStat:", totalStat)
	totalData[len(totalData)-1] = calculateStats(totalStat.nQueries, totalStat.nQueryTxns, totalStat.nReads, totalStat.nRows,
		totalStat.nReadFields, totalStat.nResultFields, totalStat.duration/int(N_CLIENTS), totalStat.duration/int(N_CLIENTS), int(totalStat.latency))
	fmt.Println("TotalData:", totalData[len(totalData)-1])

	writeDataToFile("mixStats", header, totalData)

}

func calculateStats(nQueries, nQueryTxns, nReads, nRows, nReadFields, nResultFields, totalTime, partTime, partQTime int) (data []string) {
	queryCycles := nQueries / QUERIES_PER_CYCLE
	queryCycleS, queryS := (float64(queryCycles)/float64(partTime))*1000000000, (float64(nQueries)/float64(partTime))*1000000000
	readS, qTxnsS := (float64(nReads)/float64(partTime))*1000000000, (float64(nQueryTxns)/float64(partTime))*1000000000
	rowS, entriesS := (float64(nRows)/float64(partTime))*1000000000, (float64(nReadFields)/float64(partTime))*1000000000
	latencyPerOp := float64(partQTime*int(N_CLIENTS)) / (float64(nQueries) * 1000000)
	latencyPerTxn := float64(partQTime*int(N_CLIENTS)) / (float64(nQueryTxns) * 1000000)

	data = []string{strconv.Itoa(totalTime / 1000000), strconv.Itoa(partTime / 1000000), strconv.Itoa(queryCycles),
		strconv.Itoa(nQueryTxns), strconv.Itoa(nQueries), strconv.Itoa(nReads), strconv.Itoa(nRows),
		strconv.Itoa(nReadFields), strconv.FormatFloat(queryCycleS, 'f', 10, 64), strconv.FormatFloat(qTxnsS, 'f', 10, 64),
		strconv.FormatFloat(queryS, 'f', 10, 64), strconv.FormatFloat(readS, 'f', 10, 64), strconv.FormatFloat(rowS, 'f', 10, 64),
		strconv.FormatFloat(entriesS, 'f', 10, 64), strconv.FormatFloat(latencyPerTxn, 'f', 10, 64),
		strconv.FormatFloat(latencyPerTxn, 'f', 10, 64), strconv.FormatFloat(latencyPerOp, 'f', 10, 64)}

	return
}

//"Total time", "Section time", "Queries cycles", "Query txns", "Queries", "Reads", "Rows", "Entries",
//"Query cycles/s", "Query txn/s", "Query/s", "Read/s", "Row/s", "Entries/s",
//"AvgQ latency", "AvgAll latency (ms)", "Avg latency (ms)(AO)"}

func statsFileHelper(stats []BenchmarkResults) (totalStat QueryStatistics, statPerTime [][]QueryStatistics, header []string) {
	totalStat, statPerTime = convertStats(stats)
	header = []string{"Total time", "Section time", "Queries cycles", "Query txns", "Queries", "Reads", "Rows", "Entries",
		"Query cycles/s", "Query txn/s", "Query/s", "Read/s", "Row/s", "Entries/s",
		"AvgQ latency", "AvgAll latency (ms)", "Avg latency (ms)(AO)"}
	/*header = []string{"Total time", "Section time", "Queries cycles", "Queries", "Reads", "Query cycles/s", "Query/s", "Read/s",
	"Query txns", "Query txns/s", "Updates", "Updates", "New upds", "Del upds", "Index upds", "Update blocks", "Update blocks/s",
	"New+del upds", "New+del upd/s", "Update txns", "Update txns/s", "Ops_all", "Ops_all/s", "Ops", "Ops/s", "Txns", "Txn/s",
	"AvgQ latency", "AvgU latency", "AvgAll latency (ms)", "Avg latency (ms)(AO)", "Average latency (ms)(AOw/I)"}*/
	return
}

func convertStats(stats []BenchmarkResults) (totalStat QueryStatistics, statsPerTime [][]QueryStatistics) {
	sizeToUse := int(math.MaxInt32)
	for _, currStats := range stats {
		if len(currStats.IntermediateResults) < sizeToUse {
			sizeToUse = len(currStats.IntermediateResults)
		}
	}
	sizeToUse-- //Ignore warmup

	fmt.Println()
	fmt.Println("Stats:", stats, "TotalStats:", totalStat, "Len of stats (nClients):", len(stats))
	statsPerTime = make([][]QueryStatistics, sizeToUse)
	fmt.Println("Size of statsPerTime:", sizeToUse, "StatsPerTime:", statsPerTime, "SizeToUse:", sizeToUse)
	var currStatTime []QueryStatistics
	for i := range statsPerTime {
		//fmt.Printf("Iterating %d. Inner size: %d\n", i, len(stats))
		currStatTime = make([]QueryStatistics, len(stats))
		for j, stat := range stats {
			//fmt.Printf("Iterating %d.%d.\n", i, j)
			currStatTime[j] = stat.IntermediateResults[i+1] //Accounting for warmup
		}
		statsPerTime[i] = currStatTime
	}
	totalStat = QueryStatistics{}
	fmt.Println("Stats per time:", statsPerTime, "Len:", len(statsPerTime))
	fmt.Println("Starting to iterate totalStat.")
	for _, stat := range stats {
		fmt.Printf("Iterating totalStat. Stats: %v\n", stat)
		totalStat.nQueries += stat.Total.nQueries
		totalStat.nQueryTxns += stat.Total.nQueryTxns
		totalStat.nReads += stat.Total.nReads
		totalStat.nRows += stat.Total.nRows
		totalStat.nReadFields += stat.Total.nReadFields
		totalStat.nResultFields += stat.Total.nResultFields
		totalStat.latency += stat.Total.latency
		totalStat.duration += stat.Total.duration
	}
	fmt.Println()
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
