package client

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/AndreRijo/go-tools/src/tools"
)

//Contains all configurations that comes from either program parameters or config files.

const (
	SINGLE, CYCLE                = 0, 1    //BATCH_MODE
	AVG_OP, AVG_BATCH, PER_BATCH = 0, 1, 2 //LATENCY_MODE
)

var (
	//GENERAL
	DOES_QUERIES, DOES_UPDATES, DOES_DATALOAD              = true, false, true
	PRINT_QUERY, IS_REDIRECT, DOES_VIEWLOAD, USES_VIEWS    = false, false, true, true
	QUERY_FUNCS_INT                                        = []int{ALL_QUERIES}
	STATS_SAVE_LOCATION                                    string
	IP_DSN                                                 = "postgres://postgres:@localhost:5432/test?sslmode=disable"
	POSTGRES_USER                                          = "postgres"
	ID                                                     string //id for statistics files
	RESET_ONLY, UPDATE_SPECIFIC_INDEX_ONLY, DOES_INDEXLOAD        = false, false, false
	SF                                                            = 1.0

	//QUERIES
	QUERY_WAIT, TEST_DURATION, STATISTICS_INTERVAL time.Duration = 60000 * time.Millisecond, 35000 * time.Millisecond, 5000 * time.Millisecond
	N_CLIENTS, QUERIES_PER_CYCLE, RECORD_LATENCY                 = int64(10), 1, true
	//Note: QUERIES_PER_CYCLE, RECORD_LATENCY are also unused for now

	//UPDATES
	UPDATE_RATE, N_UPD_CLIENTS      = 0.0, 0 //N_UPD_CLIENTS: when <= 0, all clients will issue both queries and updates if UPD_RATE > 0 && DOES_UPDATES
	START_UPD_FILE, FINISH_UPD_FILE = 1, 1000

	//UNUSED YET
	BATCH_MODE, LATENCY_MODE int
)

// Loads configurations from file first, if any; then loads the flags
func loadConfigs() (configs *tools.ConfigLoader) {
	configs = loadFlags()
	loadConfigsFile(configs)
	return configs
}

/*
postgresTPCHClient/src/main/main --config=$CONFIG --data_folder=$DATA_FOLDER --query_clients=$QUERY_CLIENTS --test_name=$TEST_NAME --upd_rate=$UPD_RATE --reset=$RESET --id=$ID --n_reads_txn=$N_READS_TXN --batch_mode=$BATCH_MODE --latency_mode=$LATENCY_MODE --ip=$IP --scale=$SF --update_specific_index=$UPDATE_SPECIFIC_INDEX --queryNumbers=$QUERIES --queryDuration=$QUERY_DURATION --queryWait=$QUERY_WAIT;
*/

func loadFlags() (configs *tools.ConfigLoader) {
	fmt.Println("[PGC]All flags:", os.Args)
	configFolder := flag.String("config", "none", "sub-folder in configs folder that contains the configuration files to be used.")
	dataFolder := flag.String("data_folder", "none", "folder containing the TPC-H dataset, updates and headers.")
	scaleS := flag.String("scale", "none", "scale (SF) of the tpch test")
	doesDataload := flag.String("do_dataload", "none", "if this client does the initial loading of base data to PostgreSQL or not.")
	doesViewload := flag.String("do_viewload", "none", "if this client does the loading of views to PostgreSQL or not.")
	doesIndexload := flag.String("do_indexload", "none", "if this client creates indexes for queries in PostgreSQL or not. If doesViewload is true, indexes will not be loaded regardless."+
		"Default: false. Note: query performance without views may be unreasonable even when using indexes.")
	doesQueries := flag.String("do_queries", "none", "if this client does queries or not.")
	doesUpdates := flag.String("do_updates", "none", "if this client does updates or not.")
	usesViews := flag.String("uses_views", "none", "if this client uses views or not when querying. Default: true. Performance may be unreasonable when set to false.")
	isRedirect := flag.String("is_redirect", "none", "if this client is a redirect client or not.")
	serverIP := flag.String("ip", "none", "Url (DSN) of the PostgreSQL server.")
	postgresUser := flag.String("user", "none", "User to use for the PostgreSQL server.")
	idString := flag.String("id", "none", "id to use for statistics file. Should be set when multiple clent instances are running on the same disk.")
	queryPrintString := flag.String("queryPrint", "none", "if query results and information should be printed or not (true/false). Default: false")
	queryNumbersString := flag.String("queryNumbers", "none", "list of TPC-H queries to create views for (e.g: [3,5]). By default only views for queries Q3, Q5, Q11, Q14, Q15 and Q18 are loaded.")
	queryDurationS := flag.String("queryDuration", "none", "how long (ms) should the experiment run for.")
	queryWaitS := flag.String("queryWait", "none", "how long (ms) should the client wait for before starting the experiment. Useful to let the servers finish setting up.")
	testRoutinesString := flag.String("query_clients", "none", "number of query client processes (ignored if this isn't a query client)")
	statsSaveLocationString := flag.String("test_name", "none", "name of the test/path to write statistics to.")
	statsInterval := flag.String("stats_frequency", "none", "frequency (ms) with which statistics should be recorded.")
	reset := flag.String("reset", "none", "set this flag to true for resetting the server status. The program will exit afterwards.")
	nReadsTxn := flag.String("n_reads_txn", "none", "number of reads (not queries) per transaction. Works for both mix and query clients.")
	updateSpecificIndex := flag.String("update_specific_index", "none", "if only the indexes correspondent to the queries in the config file should be updated.")
	nUpdClients := flag.String("n_upd_clients", "none", "specifies how many clients should do updates. If 0, and DOES_UPDATES=true, all clients can do updates.")

	//Unsupported settings (for now...)
	batchModeS := flag.String("batch_mode", "none", "how queries are grouped in a transaction - CYCLE, SINGLE.")
	latencyModeS := flag.String("latency_mode", "none", "how is latency measured - AVG_OP, AVG_BATCH, PER_BATCH.")
	updateRate := flag.String("upd_rate", "none", "rate of updates (0-1). Ignored if either DOES_QUERIES or DOES_UPDATES is false.")

	flag.Parse()
	fmt.Println("[PGC]queryWait on flags:", *queryWaitS)
	configs = &tools.ConfigLoader{}
	if isFlagValid(configFolder) {
		fmt.Println("[PGC]Loading valid configs. Using configFolder:", *configFolder)
		configs.LoadConfigs(*configFolder)
	} else {
		fmt.Println("[PGC]Loading empty configs")
		configs.InitEmptyConfig()
	}
	if isFlagValid(reset) {
		RESET_ONLY = true
	}
	if isFlagValid(dataFolder) {
		configs.ReplaceConfig("folder", *dataFolder)
	}
	if isFlagValid(scaleS) {
		configs.ReplaceConfig("scale", *scaleS)
	}
	if isFlagValid(doesDataload) {
		configs.ReplaceConfig("doDataload", *doesDataload)
	}
	if isFlagValid(doesViewload) {
		configs.ReplaceConfig("doViewload", *doesViewload)
	}
	if isFlagValid(doesViewload) {
		configs.ReplaceConfig("doIndexload", *doesIndexload)
	}
	if isFlagValid(doesQueries) {
		configs.ReplaceConfig("doQueries", *doesQueries)
	}
	if isFlagValid(doesUpdates) {
		configs.ReplaceConfig("doUpdates", *doesUpdates)
	}
	if isFlagValid(usesViews) {
		configs.ReplaceConfig("usesViews", *usesViews)
	}
	if isFlagValid(isRedirect) {
		configs.ReplaceConfig("isRedirect", *isRedirect)
	}
	if isFlagValid(serverIP) {
		configs.ReplaceConfig("ip", *serverIP)
	}
	if isFlagValid(postgresUser) {
		configs.ReplaceConfig("user", *postgresUser)
	}
	if isFlagValid(idString) {
		configs.ReplaceConfig("id", *idString)
	}
	if isFlagValid(queryPrintString) {
		configs.ReplaceConfig("queryPrint", *queryPrintString)
	}
	if isFlagValid(queryNumbersString) {
		configs.ReplaceConfig("queries", processListCommandLine(*queryNumbersString))
	}
	if isFlagValid(updateSpecificIndex) {
		configs.ReplaceConfig("updateSpecificIndex", *updateSpecificIndex)
	}
	if isFlagValid(queryDurationS) {
		configs.ReplaceConfig("queryDuration", *queryDurationS)
	}
	if isFlagValid(queryWaitS) {
		fmt.Println("[PGC][FLAG]Setting queryWait in configs to:", *queryWaitS)
		configs.ReplaceConfig("queryWait", *queryWaitS)
	} else {
		fmt.Println("[PGC][FLAG]Query wait flag is not valid!!! Value:", *queryWaitS)
	}
	if isFlagValid(testRoutinesString) {
		configs.ReplaceConfig("queryClients", *testRoutinesString)
	}
	if isFlagValid(statsSaveLocationString) {
		configs.ReplaceConfig("statsLocation", *statsSaveLocationString)
	}
	if isFlagValid(statsInterval) {
		configs.ReplaceConfig("statisticsInterval", *statsInterval)
	}
	if isFlagValid(nReadsTxn) {
		configs.ReplaceConfig("nReadsTxn", *nReadsTxn)
	}
	if isFlagValid(batchModeS) {
		configs.ReplaceConfig("batchMode", *batchModeS)
	}
	if isFlagValid(latencyModeS) {
		configs.ReplaceConfig("latencyMode", *latencyModeS)
	}
	if isFlagValid(updateRate) {
		configs.ReplaceConfig("updRate", *updateRate)
	}
	if isFlagValid(nUpdClients) {
		configs.ReplaceConfig("nUpdClients", *nUpdClients)
	}
	return
}

func processListCommandLine(origString string) string {
	if origString[0] == '[' {
		origString = strings.Replace(origString[1:len(origString)-1], ",", " ", -1)
	}
	return origString
}

func loadConfigsFile(configs *tools.ConfigLoader) {
	//folder and scale are already read in pgClient.go
	DOES_DATALOAD, UPDATE_SPECIFIC_INDEX_ONLY = configs.GetBoolConfig("doDataload", DOES_DATALOAD), configs.GetBoolConfig("updateSpecificIndex", false)
	DOES_QUERIES, DOES_UPDATES = configs.GetBoolConfig("doQueries", DOES_QUERIES), configs.GetBoolConfig("doUpdates", DOES_UPDATES)
	IP_DSN, POSTGRES_USER, ID = configs.GetOrDefault("ip", IP_DSN), configs.GetOrDefault("user", POSTGRES_USER), configs.GetOrDefault("id", ID)
	PRINT_QUERY, N_CLIENTS = configs.GetBoolConfig("queryPrint", PRINT_QUERY), int64(configs.GetIntConfig("queryClients", int(N_CLIENTS)))
	QUERIES_PER_CYCLE, UPDATE_RATE = configs.GetIntConfig("nReadTxns", QUERIES_PER_CYCLE), float64(configs.GetFloatConfig("updRate", UPDATE_RATE))
	STATISTICS_INTERVAL, STATS_SAVE_LOCATION = time.Duration(configs.GetIntConfig("statisticsInterval", int(STATISTICS_INTERVAL))), configs.GetOrDefault("statsLocation", STATS_SAVE_LOCATION)
	queryFuncsString := strings.Split(configs.GetOrDefault("queries", "3 5 11 14 15 18"), " ")
	BATCH_MODE, LATENCY_MODE = batchModeStringToInt(configs.GetOrDefault("batchMode", "SINGLE")), latencyModeStringToInt(configs.GetOrDefault("latencyMode", "AVG_OP"))
	IS_REDIRECT, DOES_VIEWLOAD = configs.GetBoolConfig("isRedirect", IS_REDIRECT), configs.GetBoolConfig("doViewload", DOES_VIEWLOAD)
	START_UPD_FILE, FINISH_UPD_FILE = configs.GetIntConfig("startUpdFile", START_UPD_FILE), configs.GetIntConfig("finishUpdFile", FINISH_UPD_FILE)
	SF, N_UPD_CLIENTS, USES_VIEWS = configs.GetFloatConfig("scale", SF), configs.GetIntConfig("nUpdClients", N_UPD_CLIENTS), configs.GetBoolConfig("usesViews", true)
	DOES_INDEXLOAD = configs.GetBoolConfig("doIndexload", true)

	testDurationInt, queryWaitInt := configs.GetIntConfig("queryDuration", -1), configs.GetIntConfig("queryWait", -1)
	fmt.Println("[PGC]QUERY_WAIT, queryWaitInt:", QUERY_WAIT, queryWaitInt)
	if testDurationInt != -1 {
		TEST_DURATION = time.Duration(testDurationInt) * time.Millisecond
	} //else: leave the default unchanged
	if queryWaitInt != -1 {
		fmt.Println("[PGC]Setting QUERY_WAIT to:", time.Duration(queryWaitInt)*time.Millisecond, "as queryWaitInt != -1.")
		QUERY_WAIT = time.Duration(queryWaitInt) * time.Millisecond
	} //else: leave the default unchanged
	if STATISTICS_INTERVAL < time.Millisecond {
		STATISTICS_INTERVAL *= time.Millisecond
	}
	QUERY_FUNCS_INT = make([]int, len(queryFuncsString))
	if len(queryFuncsString) == 1 && queryFuncsString[0] == "*" {
		QUERY_FUNCS_INT[0] = ALL_QUERIES
	} else {
		for i, qString := range queryFuncsString {
			QUERY_FUNCS_INT[i], _ = strconv.Atoi(qString)
		}
	}
	fmt.Println("StatisticsInterval:", STATISTICS_INTERVAL)
	fmt.Println("Queries lists:")
	fmt.Println(queryFuncsString)
	fmt.Println(QUERY_FUNCS_INT)
	fmt.Printf("DATALOAD: %v. VIEWLOAD: %v. QUERIES: %v. UPDATES: %v\n", DOES_DATALOAD, DOES_VIEWLOAD, DOES_QUERIES, DOES_UPDATES)
	fmt.Printf("NClients: %d. nReadTxns: %d. Scale: %fSF. UpdRate: %f. Query duration:wait: %d:%d.\n",
		N_CLIENTS, QUERIES_PER_CYCLE, configs.GetFloatConfig("scale", 1.0), UPDATE_RATE, TEST_DURATION, QUERY_WAIT)

}

/*
DOES_QUERIES, DOES_UPDATES, DOES_DATALOAD = true, false, true
	PRINT_QUERY                               = false
	QUERY_FUNCS_STRING                        = []string{"*"}
	STATS_SAVE_LOCATION                       string
	IP_DSN                                    = "postgres://postgres:@localhost:5432/test?sslmode=disable"
	ID                                        string //id for statistics files

	//QUERIES
	QUERY_WAIT, TEST_DURATION, STATISTICS_INTERVAL time.Duration = 60000 * time.Millisecond, 35000 * time.Millisecond, 5000 * time.Millisecond
	N_CLIENTS, QUERIES_PER_CYCLE, RECORD_LATENCY                 = int64(10), 1, true

	//UPDATES
	UPDATE_RATE = 0.0
*/

func isFlagValid(value *string) bool {
	return *value != "none" && *value != "" && *value != " "
}

func batchModeStringToInt(typeS string) int {
	switch strings.ToUpper(typeS) {
	case "SINGLE":
		return SINGLE
	case "CYCLE":
		return CYCLE
	default:
		fmt.Println("[ERROR]Unknown batch mode type. Exitting")
		os.Exit(0)
	}
	return CYCLE
}

func latencyModeStringToInt(typeS string) int {
	switch strings.ToUpper(typeS) {
	case "AVG_OP":
		return AVG_OP
	case "AVG_BATCH":
		return AVG_BATCH
	case "PER_BATCH":
		return PER_BATCH
	default:
		fmt.Println("[ERROR]Unknown batch mode type. Exitting")
		os.Exit(0)
	}
	return AVG_OP
}
