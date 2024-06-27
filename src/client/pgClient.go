package client

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"
	"tpch_data_processor/tpch"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

type DBInfo struct {
	DB  *bun.DB
	Ctx context.Context
}

//../../../../potionDB docs/tpch_data/
//0.01
//go run pgClient.go --data_folder="../../../../potionDB docs/tpch_data/" --scale=0.01

func Start() {

	startTime := time.Now().UnixNano()
	configs := loadConfigs()
	ctx := context.Background()

	fmt.Println("[PGC]DSN:", IP_DSN)
	// Open a PostgreSQL database.
	fmt.Println("[PGC]Connecting to PostgresSQL.")
	//pgdb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(IP_DSN)))
	pgconn := pgdriver.NewConnector(pgdriver.WithNetwork("tcp"),
		//pgdriver.WithAddr("0.0.0.0:5533"),
		pgdriver.WithAddr(IP_DSN),
		pgdriver.WithTLSConfig(nil),
		pgdriver.WithUser(POSTGRES_USER),
		pgdriver.WithDatabase("test"),
		pgdriver.WithTimeout(180*time.Second),
		//pgdriver.WithPassword("default"),
	)
	pgdb := sql.OpenDB(pgconn)
	/*
			pgconn := pgdriver.NewConnector(
			pgdriver.WithNetwork("tcp"),
			pgdriver.WithAddr("localhost:5437"),
			pgdriver.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}),
			pgdriver.WithUser("test"),
			pgdriver.WithPassword("test"),
			pgdriv2r.WithDatabase("test"),
			pgdriver.WithApplicationName("myapp"),
			pgdriver.WithTimeout(5 * time.Second),
			pgdriver.WithDialTimeout(5 * time.Second),
			pgdriver.WithReadTimeout(5 * time.Second),
			pgdriver.WithWriteTimeout(5 * time.Second),
			pgdriver.WithConnParams(map[string]interface{}{
				"search_path": "my_search_path",
			}),
		)
	*/
	fmt.Println("[PGC]Pinging PostgresSQL.")
	err := pgdb.Ping()
	if err != nil {
		fmt.Println("[PGC]Error on pinging PostgresSQL:", err)
		os.Exit(1)
	}
	fmt.Println("[PGC]Connected to PostgresSQL.")
	fmt.Println(IP_DSN)

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

	dbInfo := DBInfo{DB: db, Ctx: ctx}
	seed := time.Now().UnixNano()
	//queryClient.DropViews()
	fmt.Println("[PGC]Flags loaded.")
	if RESET_ONLY {
		(&SQLTables{}).SendDropTables(dbInfo)
		os.Exit(0)
	}
	tpchConfigs := tpch.TpchConfigs{Sf: configs.GetFloatConfig("scale", 1.0), DataLoc: configs.GetConfig("folder")}
	queryClient := CreatePGQueryClient(dbInfo, nil, tpchConfigs.Sf, seed, 0, QUERY_FUNCS_INT)

	fmt.Println("[PGC]Starting to load base data.")
	completeChan := make(chan bool, 1)
	var tables *SQLTables
	if DOES_DATALOAD {
		tables = LoadAndSendBaseData(tpchConfigs, dbInfo, completeChan)
	} else {
		tables = LoadBaseData(tpchConfigs, dbInfo, completeChan)
	}
	queryClient.SetTables(tables)
	<-completeChan
	if DOES_DATALOAD {
		fmt.Println("[PGC]Finished loading and uploading base data.")
		queryClient.CreateViews()
		//queryClient.QueryViews()
	} else {
		fmt.Println("[PGC]Finished loading base data")
	}
	if DOES_QUERIES {
		prepareQueryClientsBenchmark(*queryClient, QUERY_FUNCS_INT, startTime)
	}
	//testQuery(dbInfo)

	ignore(queryClient)
	//select {}
}

func testQuery(dbInfo DBInfo) {
	orderIDWanted := []Orders{{O_ORDERKEY: 2048}}
	err := dbInfo.DB.NewSelect().Model(&orderIDWanted).WherePK().Scan(dbInfo.Ctx, &orderIDWanted)
	if err != nil {
		fmt.Println("[PGC]Error on test query on orders:", err)
	}
	fmt.Printf("[PGC]Test query result: %+v.\n", orderIDWanted)
}

func ignore(any interface{}) {

}
