package client

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"time"
	"tpch_data_processor/tpch"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	pgTpch "postgres_tpch_go_lib/src/tpch"
)

type DBInfo struct {
	DB   *bun.DB         //Direct Postgres
	Ctx  context.Context //Direct Postgres
	conn net.Conn        //Redirect Postgres
}

//../../../../potionDB docs/tpch_data/
//0.01
//go run pgClient.go --data_folder="../../../../potionDB docs/tpch_data/" --scale=0.01

func Start() {
	startTime := time.Now().UnixNano()
	configs := loadConfigs()

	seed := time.Now().UnixNano()
	//queryClient.DropViews()
	fmt.Println("[PGC]Flags loaded.")
	var dbInfo DBInfo
	if IS_REDIRECT {
		dbInfo = connectToRedirect()
	} else {
		dbInfo = connectToPostgres()
		dbInfo.DB.SetMaxIdleConns(200)
	}
	if RESET_ONLY {
		if IS_REDIRECT {
			SendDropTablesRedirect(dbInfo)
		} else {
			SendDropTables(dbInfo)
		}
		os.Exit(0)
	}
	tpchConfigs := tpch.TpchConfigs{Sf: SF, DataLoc: configs.GetConfig("folder")}
	queryClient := CreatePGQueryClient(dbInfo, nil, tpchConfigs.Sf, seed, 0, QUERY_FUNCS_INT)

	completeChan, updateDataChan := make(chan bool, 1), make(chan UpdateData, 1)
	var tables *pgTpch.SQLTables
	if DOES_DATALOAD {
		fmt.Println("[PGC]Starting to load base data. Will send data to server too.")
		tables = LoadAndSendBaseData(tpchConfigs, dbInfo, completeChan, updateDataChan)
	} else {
		fmt.Println("[PGC]Starting to load base data. Will NOT send data to server.")
		tables = LoadBaseData(tpchConfigs, dbInfo, completeChan, updateDataChan)
	}
	queryClient.SetTables(tables)
	<-completeChan
	if DOES_DATALOAD || DOES_VIEWLOAD || DOES_INDEXLOAD {
		if DOES_DATALOAD && DOES_VIEWLOAD {
			fmt.Println("[PGC]Finished loading and uploading base data. Creating views.")
		} else if DOES_DATALOAD && DOES_INDEXLOAD {
			fmt.Println("[PGC]Finished loading and uploading base data. Creating indexes.")
		} else if DOES_VIEWLOAD {
			fmt.Println("[PGC]Finished loading base data. Creating views.")
		} else if DOES_INDEXLOAD {
			fmt.Println("[PGC]Finished loading base data. Creating indexes.")
		} else {
			fmt.Println("[PGC]Finished loading and uploading base data. Not creating views nor indexes.")
		}
		if DOES_VIEWLOAD {
			queryClient.CreateViews()
		} else if DOES_INDEXLOAD {
			queryClient.CreateIndexes()
		}
		//queryClient.QueryViews()
	} else {
		fmt.Println("[PGC]Finished loading base data. Not creating views nor indexes.")
	}
	if DOES_QUERIES || DOES_UPDATES {
		prepareQueryClientsBenchmark(*queryClient, QUERY_FUNCS_INT, startTime, updateDataChan)
	}
	//testQuery(dbInfo)

	ignore(queryClient)
	//select {}
}

func connectToRedirect() (dbInfo DBInfo) {
	fmt.Printf("[PGC]Connecting to Redirect PostgresSQL on ip %s.\n", IP_DSN)
	conn, err := net.Dial("tcp", IP_DSN)
	if err != nil {
		fmt.Println("[PGC]Error on connecting to Redirect PostgresSQL:", err)
		os.Exit(1)
	}
	fmt.Println("[PGC]Connected to Redirect PostgresSQL.")

	return DBInfo{conn: conn}
}

func connectToPostgres() (dbInfo DBInfo) {
	ctx := context.Background()

	// Open a PostgreSQL database.
	fmt.Printf("[PGC]Connecting to PostgresSQL on ip %s, with user %s, on database test.\n", IP_DSN, POSTGRES_USER)
	pgconn := pgdriver.NewConnector(pgdriver.WithNetwork("tcp"),
		pgdriver.WithAddr(IP_DSN),
		pgdriver.WithTLSConfig(nil),
		pgdriver.WithUser(POSTGRES_USER),
		pgdriver.WithDatabase("test"),
		pgdriver.WithTimeout(180*time.Second),
	)
	pgdb := sql.OpenDB(pgconn)

	fmt.Println("[PGC]Pinging PostgresSQL.")
	err := pgdb.Ping()
	if err != nil {
		fmt.Println("[PGC]Error on pinging PostgresSQL:", err)
		os.Exit(1)
	}
	fmt.Println("[PGC]Connected to PostgresSQL.")

	// Create a Bun db on top of it.
	db := bun.NewDB(pgdb, pgdialect.New())
	fmt.Println("[PGC]Created DB item.")

	_, err = db.NewRaw("CREATE EXTENSION IF NOT EXISTS pg_ivm").Exec(ctx)
	if err != nil {
		fmt.Println("Failed to load pg_ivm extension on Postgres:", err)
		os.Exit(1)
	} else {
		fmt.Println("[PGC]Successfully loaded pg_ivm extension on Postgres.")
	}

	return DBInfo{DB: db, Ctx: ctx}
}

func testQuery(dbInfo DBInfo) {
	orderIDWanted := []pgTpch.Orders{{O_ORDERKEY: 2048}}
	err := dbInfo.DB.NewSelect().Model(&orderIDWanted).WherePK().Scan(dbInfo.Ctx, &orderIDWanted)
	if err != nil {
		fmt.Println("[PGC]Error on test query on orders:", err)
	}
	fmt.Printf("[PGC]Test query result: %+v.\n", orderIDWanted)
}

func ignore(any interface{}) {

}
