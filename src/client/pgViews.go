package client

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	"postgres_tpch_go_lib/src/proto"
	"postgres_tpch_go_lib/src/tpch"

	"github.com/uptrace/bun"
)

//TODO: Try to do both views for Q1 in a single step.

const ALL_QUERIES = -1

var (
	TIMEZONE = time.FixedZone("Portugal", 0)
)

type PgQueryClient struct {
	DBInfo
	*tpch.SQLTables
	Rng *rand.Rand
	Sf  float64
	Id  int64
	MixStatistics
	QueryFuncs   []func()
	UpdateFunc   func(*tpch.Orders, []*tpch.LineItem, int32, int)
	MultiUpdFunc func([][]string, [][]string, []int32, int)
	//Helper variables for benchmarking
	UpdData              RoutineUpdData
	OrderI, LineI, LineF int               //Helper variables used when benchmarking updates
	CurrQProto           *proto.MultiQuery //Helper variable for when executing multiple queries per txn with redirection.
	QProtoI              int
	/*q3NoViewBaseQuery    *bun.SelectQuery
	q5NoViewBaseQuery    *bun.SelectQuery
	q11NoViewBaseQuery   *bun.SelectQuery
	q14NoViewBaseQuery   *bun.SelectQuery
	q15NoViewBaseQuery   *bun.SelectQuery
	q18NoViewBaseQuery   *bun.SelectQuery*/
}

type QueryStatistics struct {
	nQueries      int
	nQueryTxns    int
	nReads        int
	nRows         int
	nResultFields int   //Number of fields shown to the user
	nReadFields   int   //Number of actual fields downloaded
	duration      int   //ms
	queryLatency  int64 //ns, later converted to ms.
}

type UpdateStatistics struct {
	nNewOrders, nNewItems int
	nDelOrders, nDelItems int
	//Rows and fields can be calculated from the above.
	nUpdTxns   int   //new and del are together in one txn.
	updLatency int64 //ns, later converted to ms.
}

type MixStatistics struct {
	QueryStatistics
	UpdateStatistics
}

func CreatePGQueryClient(dbInfo DBInfo, tables *tpch.SQLTables, sf float64, seed, id int64, queries []int) *PgQueryClient {
	client := &PgQueryClient{DBInfo: dbInfo, SQLTables: tables, Sf: sf, Id: id, Rng: rand.New(rand.NewSource(seed))}
	if dbInfo.conn != nil { //Prepare functions for redirect client
		if QUERIES_PER_CYCLE == 1 {
			client.prepareRedirectQueryFuncs(queries)
		} else {
			client.CurrQProto, client.QProtoI = &proto.MultiQuery{Queries: make([]*proto.Query, QUERIES_PER_CYCLE)}, 0
			client.prepareRedirectMultiQueryFuncs(queries)
		}
		client.UpdateFunc, client.MultiUpdFunc = client.DoSingleUpdateRedirect, client.DoUpdateRedirect
	} else if USES_VIEWS {
		client.preparePGQueryFuncs(queries)
		client.UpdateFunc, client.MultiUpdFunc = client.DoSingleUpdate, client.DoUpdate
	} else {
		//client.prepareBaseNoViewQueryStatements()
		client.preparePGNoViewQueryFuncs(queries)
		client.UpdateFunc, client.MultiUpdFunc = client.DoSingleUpdate, client.DoUpdate
	}
	return client
}

/*func (client *PgQueryClient) prepareBaseNoViewQueryStatements() {
	client.q3NoViewBaseQuery = prepareQ3NoViewBaseQueryStatement(client.DB)
	client.q5NoViewBaseQuery = prepareQ5NoViewBaseQueryStatement(client.DB)
	client.q11NoViewBaseQuery = prepareQ11NoViewBaseQueryStatement(client.DB)
	client.q14NoViewBaseQuery = prepareQ14NoViewBaseQueryStatement(client.DB)
	client.q15NoViewBaseQuery = prepareQ15NoViewBaseQueryStatement(client.DB)
	client.q18NoViewBaseQuery = prepareQ18NoViewBaseQueryStatement(client.DB)
}*/

func CreatePGQueryClientWithoutFuncs(dbInfo DBInfo, tables *tpch.SQLTables, sf float64, seed, id int64) *PgQueryClient {
	return &PgQueryClient{DBInfo: dbInfo, SQLTables: tables, Sf: sf, Id: id, Rng: rand.New(rand.NewSource(seed))}
}

func (client *PgQueryClient) prepareRedirectQueryFuncs(queries []int) {
	fmt.Println("Preparing client with redirect queries", queries)
	if queries[0] == ALL_QUERIES {
		client.QueryFuncs = []func(){client.DoQ1QueryRedirect, client.DoQ3QueryRedirect, client.DoQ5QueryRedirect, client.DoQ6QueryRedirect,
			client.DoQ11QueryRedirect, client.DoQ14QueryRedirect, client.DoQ15QueryRedirect, client.DoQ18QueryRedirect}
	} else {
		client.QueryFuncs = make([]func(), len(queries))
		for i, query := range queries {
			switch query {
			case 1:
				client.QueryFuncs[i] = client.DoQ1QueryRedirect
			case 3:
				client.QueryFuncs[i] = client.DoQ3QueryRedirect
			case 5:
				client.QueryFuncs[i] = client.DoQ5QueryRedirect
			case 6:
				client.QueryFuncs[i] = client.DoQ6QueryRedirect
			case 11:
				client.QueryFuncs[i] = client.DoQ11QueryRedirect
			case 14:
				client.QueryFuncs[i] = client.DoQ14QueryRedirect
			case 15:
				client.QueryFuncs[i] = client.DoQ15QueryRedirect
			case 18:
				client.QueryFuncs[i] = client.DoQ18QueryRedirect
			}
		}
	}
}

func (client *PgQueryClient) prepareRedirectMultiQueryFuncs(queries []int) {
	fmt.Println("Preparing client with multi redirect queries", queries)
	client.QueryFuncs = make([]func(), len(queries))
	for i, query := range queries {
		switch query {
		case 1:
			client.QueryFuncs[i] = client.PrepareQ1QueryRedirect
		case 3:
			client.QueryFuncs[i] = client.PrepareQ3QueryRedirect
		case 5:
			client.QueryFuncs[i] = client.PrepareQ5QueryRedirect
		case 6:
			client.QueryFuncs[i] = client.PrepareQ6QueryRedirect
		case 11:
			client.QueryFuncs[i] = client.PrepareQ11QueryRedirect
		case 14:
			client.QueryFuncs[i] = client.PrepareQ14QueryRedirect
		case 15:
			client.QueryFuncs[i] = client.PrepareQ15QueryRedirect
		case 18:
			client.QueryFuncs[i] = client.PrepareQ18QueryRedirect
		}
	}
}

func (client *PgQueryClient) preparePGNoViewQueryFuncs(queries []int) {
	fmt.Println("Preparing client with PG queries WITHOUT views", queries)
	if queries[0] == ALL_QUERIES {
		client.QueryFuncs = []func(){client.DoQ3QueryNoView, client.DoQ5QueryNoView, client.DoQ11QueryNoView, client.DoQ14QueryNoView,
			client.DoQ15QueryNoView, client.DoQ18QueryNoView}
	} else {
		client.QueryFuncs = make([]func(), len(queries))
		for i, query := range queries {
			switch query {
			case 3:
				client.QueryFuncs[i] = client.DoQ3QueryNoView
			case 5:
				client.QueryFuncs[i] = client.DoQ5QueryNoView
			case 11:
				client.QueryFuncs[i] = client.DoQ11QueryNoView
			case 14:
				client.QueryFuncs[i] = client.DoQ14QueryNoView
			case 15:
				client.QueryFuncs[i] = client.DoQ15QueryNoView
			case 18:
				client.QueryFuncs[i] = client.DoQ18QueryNoView
			default:
				fmt.Printf("Query number %d is not supported for PG query with no views.\n", query)
			}
		}
	}
}

func (client *PgQueryClient) preparePGQueryFuncs(queries []int) {
	fmt.Println("Preparing client with PG queries", queries)
	if queries[0] == ALL_QUERIES {
		client.QueryFuncs = []func(){client.DoQ1Query, client.DoQ3Query, client.DoQ5Query, client.DoQ6Query, client.DoQ11Query,
			client.DoQ14Query, client.DoQ15Query, client.DoQ18Query}
	} else {
		client.QueryFuncs = make([]func(), len(queries))
		for i, query := range queries {
			switch query {
			case 1:
				client.QueryFuncs[i] = client.DoQ1Query
			case 3:
				client.QueryFuncs[i] = client.DoQ3Query
			case 5:
				client.QueryFuncs[i] = client.DoQ5Query
			case 6:
				client.QueryFuncs[i] = client.DoQ6Query
			case 11:
				client.QueryFuncs[i] = client.DoQ11Query
			case 14:
				client.QueryFuncs[i] = client.DoQ14Query
			case 15:
				client.QueryFuncs[i] = client.DoQ15Query
			case 18:
				client.QueryFuncs[i] = client.DoQ18Query
			}
		}
	}
}

func (client *PgQueryClient) SetTables(tables *tpch.SQLTables) {
	client.SQLTables = tables
}

// https://stackoverflow.com/questions/20820839/does-the-go-compiler-concatenate-strings-separated-by-a-plus-sign
// The compiler optimizes this into a single string, thus it is efficient.
// "ORDER BY l_returnflag, l_linestatus" + "')"
// 1998-12-01 - 60 days = 1998-10-02
func (client *PgQueryClient) GetQ1ViewStatementBase() string {
	return "SELECT create_immv('q160', '" +
		"SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price," +
		"sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge," +
		"avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate <= ''1998-10-02''\n" +
		"GROUP BY l_returnflag, l_linestatus" + "')"
}

func (client *PgQueryClient) GetQ1ViewStatementDay() string {
	return "SELECT create_immv('q1?', '" +
		"SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price," +
		"sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge," +
		"avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate > ''1998-10-02'' AND l_shipdate <= ''?''\n" +
		"GROUP BY l_returnflag, l_linestatus" + "')"
}

func (client *PgQueryClient) GetQ1ViewStatementBaseRedirect() string {
	return "SELECT create_immv('q160', '" +
		"SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price," +
		"sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge," +
		"avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate <= ''1998-10-02''\n" +
		"GROUP BY l_returnflag, l_linestatus" + "')"
}

func (client *PgQueryClient) GetQ1ViewStatementDayRedirect() string {
	return "SELECT create_immv('q1%d', '" +
		"SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price," +
		"sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge," +
		"avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate > ''1998-10-02'' AND l_shipdate <= ''%s''\n" +
		"GROUP BY l_returnflag, l_linestatus" + "')"
}

func (client *PgQueryClient) MakeQ1View() {
	result, err := client.DB.NewRaw(client.GetQ1ViewStatementBase()).Exec(client.Ctx)
	q1Day := client.GetQ1ViewStatementDay()
	if err != nil {
		fmt.Printf("[PGViews]Start of MakeQ1View. Error: %v. Result: %+v\n", err, result)
	}
	date := time.Date(1998, 10, 2, 0, 0, 0, 0, TIMEZONE)
	for i := 61; i <= 120; i++ {
		date = date.AddDate(0, 0, 1)
		result, err = client.DB.NewRaw(q1Day, i, bun.Ident(date.Format(tpch.TIME_PARSE_LAYOUT))).Exec(client.Ctx)
		//result, err = client.DB.NewRaw(q1Day, i, i).Exec(client.Ctx)
		if err != nil {
			fmt.Printf("[PGViews]MakeQ1View. Error: %v. Result: %+v\n", err, result)
		}
	}
	fmt.Printf("[PGViews]MakeQ1View - success.\n")
}

func (client *PgQueryClient) MakeQ1ViewRedirect() {
	statements := make([]string, 61)
	statements[0] = client.GetQ1ViewStatementBaseRedirect()
	q1Day := client.GetQ1ViewStatementDayRedirect()
	date := time.Date(1998, 10, 2, 0, 0, 0, 0, TIMEZONE)
	for i := 61; i <= 120; i++ {
		date = date.AddDate(0, 0, 1)
		statements[i-60] = fmt.Sprintf(q1Day, i, date.Format(tpch.TIME_PARSE_LAYOUT))
	}
	tpch.SendProto(tpch.PB_CREATE_VIEW, &proto.CreateView{Statement: statements}, client.conn)
	_, replyProto, _ := tpch.ReceiveProto(client.conn)
	viewResp := replyProto.(*proto.CreateViewResp)
	if viewResp.GetErrorMsg() != "" {
		fmt.Println(viewResp.GetErrorMsg())
	} else {
		fmt.Printf("[PGViews]MakeQ1View - success.\n")
	}
}

func (client *PgQueryClient) GetQ1QueryArgs() tpch.Q1Args {
	rndDay := client.Rng.Intn(60) + 61
	return tpch.Q1Args{FirstFrom: "q160", SecondFrom: "q1" + strconv.Itoa(rndDay), FirstOrderByOne: "l_returnflag", FirstOrderByTwo: "l_linestatus",
		SecondOrderByOne: "l_returnflag", SecondOrderByTwo: "l_linestatus"}
}

func (client *PgQueryClient) DoQ1QueryRedirect() {
	tpch.SendProto(tpch.PB_QUERY, client.GetQ1QueryArgs().ToProtobuf(), client.conn)
	_, pb, _ := tpch.ReceiveProto(client.conn)
	results := pb.(*proto.QueryResp).GetResults()
	ignore(results)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+2, client.nRows+8
	client.nReadFields, client.nResultFields = client.nReadFields+80, client.nResultFields+80
}

func (client *PgQueryClient) PrepareQ1QueryRedirect() {
	client.CurrQProto.Queries[client.QProtoI] = client.GetQ1QueryArgs().ToProtobuf()
}

func (client *PgQueryClient) DoQ1Query() {
	//Column("l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order")
	q1BaseResult, q1DayResult, randomDay := make([]tpch.Q1Result, 4), make([]tpch.Q1Result, 4), client.Rng.Intn(60)+61
	err := client.DB.NewSelect().ModelTableExpr("q160").Order("l_returnflag", "l_linestatus").Scan(client.Ctx, &q1BaseResult)
	if err != nil {
		fmt.Println("[PGViews]Q1Query. Error on q160:", err)
	}
	err = client.DB.NewSelect().ModelTableExpr("q1"+strconv.Itoa(randomDay)).Order("l_returnflag", "l_linestatus").Scan(client.Ctx, &q1DayResult)
	if err != nil {
		fmt.Printf("[PGViews]Q1Query. Error on q1%d: %v\n", randomDay, err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q1Query for day %d. Size of replies: %d (base), %d (day)\n", randomDay, len(q1BaseResult), len(q1DayResult))
	}
	j := 0
	for i, baseR := range q1BaseResult {
		dayR := q1DayResult[j]
		if dayR.L_linestatus != baseR.L_linestatus || dayR.L_returnflag != baseR.L_returnflag {
			continue //Note that the "day" result does not contain all combinations. However, on both slices, the combinations are sorted. Thus, safe to skip.
		}
		totalOrders := float64(baseR.Count_order + dayR.Count_order)
		//baseR.Avg_disc = (baseR.Sum_base_price - baseR.Sum_disc_price + dayR.Sum_base_price - dayR.Sum_disc_price) / totalOrders
		baseR.Avg_disc = 1 - ((baseR.Sum_disc_price + dayR.Sum_disc_price) / (baseR.Sum_base_price + dayR.Sum_base_price))
		baseR.Avg_price = (baseR.Sum_base_price + dayR.Sum_base_price) / totalOrders
		baseR.Avg_qty = float64(baseR.Sum_qty+dayR.Sum_qty) / totalOrders
		baseR.Sum_base_price += dayR.Sum_base_price
		baseR.Sum_charge += dayR.Sum_charge
		baseR.Sum_disc_price += baseR.Sum_disc_price
		baseR.Sum_qty += baseR.Sum_qty
		q1BaseResult[i] = baseR
		j++
		if j == len(q1DayResult) {
			break //Nothing left to merge
		}
	}
	if PRINT_QUERY {
		for _, baseR := range q1BaseResult {
			fmt.Printf("%+v\n", baseR)
		}
		fmt.Println()
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+2, client.nRows+8
	client.nReadFields, client.nResultFields = client.nReadFields+80, client.nResultFields+80
}

/*
select o_orderpriority, count(*) as order_count
from orders
where o_orderdate >= date '[DATE]' and o_orderdate < date '[DATE]' + interval '3' month and
exists (
	select *
	from lineitem
	where l_orderkey = o_orderkey and l_commitdate < l_receiptdate
)
group by o_orderpriority
order by o_orderpriority;

*/
//Note: Could also be implemented with a count instead of exists. Maybe more efficient?
//TODO: ORDER BY o_orderpriority
func (client *PgQueryClient) GetQ2ViewStatement() string {
	return "SELECT create_immv('q2?', '" +
		"SELECT o_orderpriority, COUNT(*) as order_count\n" +
		"FROM orders\n" +
		"WHERE o_orderdate >= ''?'' AND o_orderdate < ''?'' AND EXISTS(\n" +
		"SELECT * FROM line_items\n" +
		"WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)\n" +
		"GROUP BY o_orderpriority\n" +
		"')"
}

func (client *PgQueryClient) GetQ2ViewStatementRedirect() string {
	return "SELECT create_immv('q2%s', '" +
		"SELECT o_orderpriority, COUNT(*) as order_count\n" +
		"FROM orders\n" +
		"WHERE o_orderdate >= ''%s'' AND o_orderdate < ''%s'' AND EXISTS(\n" +
		"SELECT * FROM line_items\n" +
		"WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)\n" +
		"GROUP BY o_orderpriority\n" +
		"')"
}

func (client *PgQueryClient) MakeQ2View() {

}

func (client *PgQueryClient) DoQ2Query() {

}

// Split by segment and orderdate.
// Order by renevue desc, o_orderdate. Limit 10.
func (client *PgQueryClient) GetQ3ViewStatement() string {
	return "SELECT create_immv('q3?', '" +
		"SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority\n" +
		"FROM customers, orders, line_items\n" +
		"WHERE c_mktsegment = ''?'' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < ''?'' and l_shipdate > ''?''\n" +
		"GROUP BY l_orderkey, o_orderdate, o_shippriority" + "')"
}

func (client *PgQueryClient) GetQ3ViewStatementRedirect() string {
	return "SELECT create_immv('q3%s', '" +
		"SELECT l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority\n" +
		"FROM customers, orders, line_items\n" +
		"WHERE c_mktsegment = ''%s'' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < ''%s'' and l_shipdate > ''%s''\n" +
		"GROUP BY l_orderkey, o_orderdate, o_shippriority" + "')"
}

func prepareQ3NoViewBaseQueryStatement(db *bun.DB) *bun.SelectQuery {
	return db.NewSelect().Column("l_orderkey", "o_orderdate", "o_shippriority").ColumnExpr("sum(l_extendedprice*(1-l_discount)) as revenue").
		Table("customers", "orders", "line_items").Group("l_orderkey", "o_orderdate", "o_shippriority").OrderExpr("revenue desc, o_orderdate").Limit(10)
}

// DATE [1995-03-01 .. 1995-03-31].
func (client *PgQueryClient) MakeQ3View() {
	success := true
	date := time.Date(1995, 3, 1, 0, 0, 0, 0, TIMEZONE)
	var q3DateKey, q3Date string
	notifyChan, nWait := make(chan bool, 30), 30
	for i := 0; i <= 30; i++ {
		q3DateKey = strconv.Itoa(date.Day())
		q3Date = date.Format(tpch.TIME_PARSE_LAYOUT)
		go func(q3Date string, q3DateKey string) {
			for _, segment := range tpch.SEGMENTS {
				_, err := client.DB.NewRaw(client.GetQ3ViewStatement(), bun.Safe(q3DateKey+segment), bun.Safe(segment), bun.Safe(q3Date), bun.Safe(q3Date)).Exec(client.Ctx)
				if err != nil {
					fmt.Printf("[PGViews]MakeQ3View. Error for segment %s, date 1995-03-%s. Error: %v\n", segment, q3DateKey, err)
					success = false
				}
			}
			notifyChan <- true
		}(q3Date, q3DateKey)
		date = date.AddDate(0, 0, 1)
	}
	for i := 0; i < nWait; i++ {
		<-notifyChan
	}
	if success {
		fmt.Printf("[PGViews]MakeQ3View - success.\n")
	}
}

func (client *PgQueryClient) MakeQ3ViewRedirect() {
	nSegments := len(tpch.SEGMENTS)
	statement, statements := client.GetQ3ViewStatementRedirect(), make([]string, 31*nSegments)
	date := time.Date(1995, 3, 1, 0, 0, 0, 0, TIMEZONE)
	var q3DateKey, q3Date string
	for i := 0; i <= 30; i++ {
		q3DateKey = strconv.Itoa(date.Day())
		q3Date = date.Format(tpch.TIME_PARSE_LAYOUT)
		for j, segment := range tpch.SEGMENTS {
			statements[i*nSegments+j] = fmt.Sprintf(statement, q3DateKey+segment, segment, q3Date, q3Date)
		}
		date = date.AddDate(0, 0, 1)
	}
	tpch.SendProto(tpch.PB_CREATE_VIEW, &proto.CreateView{Statement: statements}, client.conn)
	_, replyProto, _ := tpch.ReceiveProto(client.conn)
	viewResp := replyProto.(*proto.CreateViewResp)
	if viewResp.GetErrorMsg() == "" {
		fmt.Printf("[PGViews]MakeQ3View - success.\n")
	} else {
		fmt.Println(viewResp.GetErrorMsg())
	}
}

func (client *PgQueryClient) GetQ3QueryArgs() tpch.Q3Args {
	rndDay, rndSegment := client.Rng.Intn(31)+1, client.SQLTables.Segments[client.Rng.Intn(len(client.SQLTables.Segments))]
	return tpch.Q3Args{From: "q3" + strconv.Itoa(rndDay) + rndSegment, FirstOrderBy: "revenue desc", SecondOrderBy: "o_orderdate", Limit: 10}
}

func (client *PgQueryClient) DoQ3QueryRedirect() {
	tpch.SendProto(tpch.PB_QUERY, client.GetQ3QueryArgs().ToProtobuf(), client.conn)
	_, pb, _ := tpch.ReceiveProto(client.conn)
	results := pb.(*proto.QueryResp).GetResults()
	ignore(results)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+10
	client.nReadFields, client.nResultFields = client.nReadFields+40, client.nResultFields+40
}

func (client *PgQueryClient) PrepareQ3QueryRedirect() {
	client.CurrQProto.Queries[client.QProtoI] = client.GetQ3QueryArgs().ToProtobuf()
}

func (client *PgQueryClient) DoQ3Query() {
	rndDay, rndSegment, q3Result := client.Rng.Intn(31)+1, client.SQLTables.Segments[client.Rng.Intn(len(client.SQLTables.Segments))], make([]tpch.Q3Result, 10)
	key := "q3" + strconv.Itoa(rndDay) + rndSegment
	err := client.DB.NewSelect().ModelTableExpr(key).Order("revenue desc", "o_orderdate").Limit(10).Scan(client.Ctx, &q3Result)
	if err != nil {
		fmt.Printf("[PGViews]Q3Query. Error on view %s: %s\n", key, err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q3Query. Success: %v\n", q3Result)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+10
	client.nReadFields, client.nResultFields = client.nReadFields+40, client.nResultFields+40
}

func (client *PgQueryClient) DoQ3QueryNoView() {
	rndDay, rndSegment, q3Result := client.Rng.Intn(31)+1, client.SQLTables.Segments[client.Rng.Intn(len(client.SQLTables.Segments))], make([]tpch.Q3Result, 10)
	date := "1995-03-" + strconv.Itoa(rndDay)
	query := prepareQ3NoViewBaseQueryStatement(client.DB).Where("c_mktsegment = '?' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_shipdate > '?' AND o_orderdate < '?'", bun.Safe(rndSegment), bun.Safe(date), bun.Safe(date))
	err := query.Scan(client.Ctx, &q3Result)
	if err != nil {
		fmt.Printf("[PGViews]Q3Query. Error on Q3 (no view): %s\n", err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q3Query. Success: %v\n", q3Result)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+10
	client.nReadFields, client.nResultFields = client.nReadFields+40, client.nResultFields+40
}

func (client *PgQueryClient) GetQ4ViewStatement() string {
	return "SELECT create_immv('q4?', '" + "')"
}

func (client *PgQueryClient) GetQ4ViewStatementRedirect() string {
	return "SELECT create_immv('q4?', '" + "')"
}

func (client *PgQueryClient) MakeQ4View() {

}

func (client *PgQueryClient) DoQ4Query() {

}

// Split by region and year [1993-1997]
// Order by revenue desc
func (client *PgQueryClient) GetQ5ViewStatement() string {
	return "SELECT create_immv('q5?', '" +
		"SELECT n_name, sum(l_extendedprice * (1-l_discount)) as revenue\n" +
		"FROM customers, orders, line_items, suppliers, nations, regions\n" +
		"WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND " +
		"n_regionkey = r_regionkey AND r_name = ''?'' AND o_orderdate >= ''?'' AND o_orderdate < ''?''\n" +
		"GROUP BY n_name" + "')"
}

func (client *PgQueryClient) GetQ5ViewStatementRedirect() string {
	return "SELECT create_immv('q5%d', '" +
		"SELECT n_name, sum(l_extendedprice * (1-l_discount)) as revenue\n" +
		"FROM customers, orders, line_items, suppliers, nations, regions\n" +
		"WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND " +
		"n_regionkey = r_regionkey AND r_name = ''%s'' AND o_orderdate >= ''%s'' AND o_orderdate < ''%s''\n" +
		"GROUP BY n_name" + "')"
}

func prepareQ5NoViewBaseQueryStatement(db *bun.DB) *bun.SelectQuery {
	//return db.NewSelect().Column("n_name").ColumnExpr("sum(l_extendedprice*(1-l_discount)) as revenue").
	return db.NewSelect().ColumnExpr("n_name, sum(l_extendedprice*(1-l_discount)) as revenue").
		Table("customers", "orders", "line_items", "suppliers", "nations", "regions").Group("n_name").OrderExpr("revenue desc")
}

func (client *PgQueryClient) MakeQ5View() {
	success := true
	date := time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	q5S := client.GetQ5ViewStatement()
	firstDateString := date.Format(tpch.TIME_PARSE_LAYOUT)
	for year := 1993; year <= 1997; year++ {
		nextYear := strconv.Itoa(year + 1)
		secondDateString := nextYear + firstDateString[4:]
		go func(year int, firstDateString, secondDateString string) {
			for i, region := range tpch.REGIONS_NAME {
				_, err := client.DB.NewRaw(q5S, year*10+i, bun.Safe(region), bun.Safe(firstDateString), bun.Safe(secondDateString)).Exec(client.Ctx)
				if err != nil {
					success = false
					fmt.Printf("[PGViews]Error creating q5 view. Key: q5%d. Error: %v\n", year*10+i, err)
				}
			}
		}(year, firstDateString, secondDateString)
		firstDateString = secondDateString
	}
	if success {
		fmt.Printf("[PGViews]MakeQ5View - success.\n")
	}
}

func (client *PgQueryClient) MakeQ5ViewRedirect() {
	date := time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	q5S := client.GetQ5ViewStatementRedirect()
	firstDateString := date.Format(tpch.TIME_PARSE_LAYOUT)
	statements := make([]string, 25)
	for year := 1993; year <= 1997; year++ {
		nextYear := strconv.Itoa(year + 1)
		secondDateString := nextYear + firstDateString[4:]
		for i, region := range tpch.REGIONS_NAME {
			statements[(year-1993)*5+i] = fmt.Sprintf(q5S, year*10+i, region, firstDateString, secondDateString)
		}
		firstDateString = secondDateString
	}
	tpch.SendProto(tpch.PB_CREATE_VIEW, &proto.CreateView{Statement: statements}, client.conn)
	_, replyProto, _ := tpch.ReceiveProto(client.conn)
	viewResp := replyProto.(*proto.CreateViewResp)
	if viewResp.GetErrorMsg() != "" {
		fmt.Println(viewResp.GetErrorMsg())
	} else {
		fmt.Printf("[PGViews]MakeQ5View - success.\n")
	}
}

func (client *PgQueryClient) GetQ5QueryArgs() tpch.Q5Args {
	rndYear, rndRegion := client.Rng.Intn(5)+1993, client.Rng.Intn(5)
	return tpch.Q5Args{From: "q5" + strconv.Itoa(rndYear) + strconv.Itoa(rndRegion), OrderBy: "revenue desc"}
}

func (client *PgQueryClient) DoQ5QueryRedirect() {
	tpch.SendProto(tpch.PB_QUERY, client.GetQ5QueryArgs().ToProtobuf(), client.conn)
	_, pb, _ := tpch.ReceiveProto(client.conn)
	results := pb.(*proto.QueryResp).GetResults() //5 rows (nations of one region), 2 columns
	ignore(results)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+5
	client.nReadFields, client.nResultFields = client.nReadFields+10, client.nResultFields+10
}

func (client *PgQueryClient) PrepareQ5QueryRedirect() {
	client.CurrQProto.Queries[client.QProtoI] = client.GetQ5QueryArgs().ToProtobuf()
}

func (client *PgQueryClient) DoQ5Query() {
	q5Result, year, region := make([]tpch.Q5Result, 5), client.Rng.Intn(5)+1993, client.Rng.Intn(5)
	key := "q5" + strconv.Itoa(year) + strconv.Itoa(region)
	err := client.DB.NewSelect().ModelTableExpr(key).Order("revenue desc").Scan(client.Ctx, &q5Result)
	if err != nil {
		fmt.Printf("[PGViews]Q5Query. Error on %s. Error: %v\n", key, err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q5Query. Sucess: %v\n", q5Result)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+5
	client.nReadFields, client.nResultFields = client.nReadFields+10, client.nResultFields+10
}

func (client *PgQueryClient) DoQ5QueryNoView() {
	q5Result, year, region := make([]tpch.Q5Result, 5), client.Rng.Intn(5)+1993, client.Rng.Intn(5)
	query := prepareQ5NoViewBaseQueryStatement(client.DB).Where("c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = '?' AND EXTRACT(year FROM o_orderdate) = ?",
		bun.Safe(client.SQLTables.Regions[region].R_NAME), year)
	err := query.Scan(client.Ctx, &q5Result)
	if err != nil {
		fmt.Printf("[PGViews]Q5Query. Error on Q5 (no view): %s\n", err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q5Query. Success: %v\n", q5Result)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+5
	client.nReadFields, client.nResultFields = client.nReadFields+10, client.nResultFields+10
}

func (client *PgQueryClient) GetQ6ViewStatement() string {
	return "SELECT create_immv('q6?', '" +
		"SELECT sum(l_extendedprice*l_discount) as revenue\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate >= ''?'' AND l_shipdate <= ''?'' AND l_discount >= ? AND l_discount <= ? AND l_quantity < ?')"
}

func (client *PgQueryClient) GetQ6ViewStatementRedirect() string {
	return "SELECT create_immv('q6%s', '" +
		"SELECT sum(l_extendedprice*l_discount) as revenue\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate >= ''%s'' AND l_shipdate <= ''%s'' AND l_discount >= %f AND l_discount <= %f AND l_quantity < %d')"
}

func (client *PgQueryClient) MakeQ6View() {
	success := true
	date := time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	for year := 1993; year <= 1997; year++ {
		endDate := date.AddDate(1, 0, 0)
		dateS, endDateS := date.Format(tpch.TIME_PARSE_LAYOUT), endDate.Format(tpch.TIME_PARSE_LAYOUT)
		key := strconv.Itoa(year)
		for i := 0.01; i <= 0.08; i += 0.01 { //Discount
			keyQ := key + strconv.Itoa(int((i+0.01)*100))
			keyQ24, keyQ25 := keyQ+"24", keyQ+"25"
			//keyQ24, keyQ25 := 1993224, 1993225
			_, err := client.DB.NewRaw(client.GetQ6ViewStatement(), bun.Safe(keyQ24), bun.Safe(dateS), bun.Safe(endDateS), i, i+0.02, 24).Exec(client.Ctx)
			if err != nil {
				fmt.Printf("[PGViews]MakeQ6View. Error for year %d, discount %f, quantity 24. Err: %v\n", year, i, err)
				success = false
			}
			_, err = client.DB.NewRaw(client.GetQ6ViewStatement(), bun.Safe(keyQ25), bun.Safe(dateS), bun.Safe(endDateS), i, i+0.02, 25).Exec(client.Ctx)
			if err != nil {
				fmt.Printf("[PGViews]MakeQ6View. Error for year %d, discount %f, quantity 25. Err: %v\n", year, i, err)
				success = false
			}
		}
		date = endDate
	}
	if success {
		fmt.Printf("[PGviews]MakeQ6View - success.\n")
	}
}

func (client *PgQueryClient) MakeQ6ViewRedirect() {
	statements := make([]string, 5*8*2)
	date := time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	baseStatement := client.GetQ6ViewStatementRedirect()
	j := 0
	for year := 1993; year <= 1997; year++ {
		endDate := date.AddDate(1, 0, 0)
		dateS, endDateS := date.Format(tpch.TIME_PARSE_LAYOUT), endDate.Format(tpch.TIME_PARSE_LAYOUT)
		key := strconv.Itoa(year)
		for i := 0.01; i <= 0.08; i += 0.01 { //Discount
			keyQ := key + strconv.Itoa(int((i+0.01)*100))
			keyQ24, keyQ25 := keyQ+"24", keyQ+"25"
			//keyQ24, keyQ25 := 1993224, 1993225
			statements[j] = fmt.Sprintf(baseStatement, keyQ24, dateS, endDateS, i, i+0.02, 24)
			statements[j+1] = fmt.Sprintf(baseStatement, keyQ25, dateS, endDateS, i, i+0.02, 25)
			j += 2
		}
		date = endDate
	}
	tpch.SendProto(tpch.PB_CREATE_VIEW, &proto.CreateView{Statement: statements}, client.conn)
	_, replyProto, _ := tpch.ReceiveProto(client.conn)
	viewResp := replyProto.(*proto.CreateViewResp)
	if viewResp.GetErrorMsg() != "" {
		fmt.Println(viewResp.GetErrorMsg())
	} else {
		fmt.Printf("[PGViews]MakeQ6View - success.\n")
	}
}

func (client *PgQueryClient) GetQ6QueryArgs() tpch.Q6Args {
	rndYear, rndQuantity, rndAmount := client.Rng.Intn(5)+1993, client.Rng.Intn(8)+2, client.Rng.Intn(2)+24
	return tpch.Q6Args{From: "q6" + strconv.Itoa(rndYear) + strconv.Itoa(rndQuantity) + strconv.Itoa(rndAmount)}
}

func (client *PgQueryClient) DoQ6QueryRedirect() {
	tpch.SendProto(tpch.PB_QUERY, client.GetQ6QueryArgs().ToProtobuf(), client.conn)
	_, pb, _ := tpch.ReceiveProto(client.conn)
	results := pb.(*proto.QueryResp).GetResults()
	ignore(results)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
	client.nReadFields, client.nResultFields = client.nReadFields+1, client.nResultFields+1
}

func (client *PgQueryClient) PrepareQ6QueryRedirect() {
	client.CurrQProto.Queries[client.QProtoI] = client.GetQ6QueryArgs().ToProtobuf()
}

// Unlike in PotionDB, here it is only one download per query (each view contains already the range of discounts).
func (client *PgQueryClient) DoQ6Query() {
	rndYear, rndQuantity, rndAmount, q6Result := client.Rng.Intn(5)+1993, client.Rng.Intn(8)+2, client.Rng.Intn(2)+24, tpch.Q6Result{}
	key := "q6" + strconv.Itoa(rndYear) + strconv.Itoa(rndQuantity) + strconv.Itoa(rndAmount)
	err := client.DB.NewSelect().ModelTableExpr(key).Scan(client.Ctx, &q6Result)
	if err != nil {
		fmt.Printf("[PGViews]Q6Query. Error on view %s: %s\n", key, err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q6Query. Success: %v\n", q6Result)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
	client.nReadFields, client.nResultFields = client.nReadFields+1, client.nResultFields+1
}

func (client *PgQueryClient) GetQ7ViewStatement() string {
	return "SELECT create_immv('q7?', '" + "')"
}

func (client *PgQueryClient) GetQ7ViewStatementRedirect() string {
	return "SELECT create_immv('q7?', '" + "')"
}

func (client *PgQueryClient) MakeQ7View() {

}

func (client *PgQueryClient) DoQ7Query() {

}

func (client *PgQueryClient) GetQ8ViewStatement() string {
	return "SELECT create_immv('q8?', '" + "')"
}

func (client *PgQueryClient) GetQ8ViewStatementRedirect() string {
	return "SELECT create_immv('q8?', '" + "')"
}

func (client *PgQueryClient) MakeQ8View() {

}

func (client *PgQueryClient) DoQ8Query() {

}

func (client *PgQueryClient) GetQ9ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) GetQ9ViewStatementRedirect() string {
	return "SELECT create_immv('q9?', '" + "')"
}

func (client *PgQueryClient) MakeQ9View() {

}

func (client *PgQueryClient) DoQ9Query() {

}

func (client *PgQueryClient) GetQ10ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) GetQ10ViewStatementRedirect() string {
	return "SELECT create_immv('q10?', '" + "')"
}

func (client *PgQueryClient) MakeQ10View() {

}

func (client *PgQueryClient) DoQ10Query() {

}

// Having is not supported. Thus, I have implemented the same way as in PotionDB: with two views per nation: sums per product; sum per nation.
// The later is the one used for filtering which product sums are we actually interested in.
// List of sums
// Split by nation.
// Order by value desc
func (client *PgQueryClient) GetQ11ViewBaseStatement() string {
	return "SELECT create_immv('q11p?', '" +
		"SELECT ps_partkey, sum(ps_supplycost * ps_availqty) as value\n" +
		"FROM part_supps, suppliers, nations\n" +
		"WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationKey AND n_name = ''?''\n" +
		"GROUP BY ps_partkey" + "')"
}

func (client *PgQueryClient) GetQ11ViewNationSumStatement() string {
	return "SELECT create_immv('q11n?', '" +
		"SELECT n_nationkey, sum(ps_supplycost * ps_availqty * 0.0001/?) as factor\n" + //Added n_nationkey as it is a requirement from the iimv extension
		"FROM part_supps, suppliers, nations\n" +
		"WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = ''?''\n" +
		"GROUP BY n_nationkey" + "')"
}

func (client *PgQueryClient) GetQ11ViewBaseStatementRedirect() string {
	return "SELECT create_immv('q11p%d', '" +
		"SELECT ps_partkey, sum(ps_supplycost * ps_availqty) as value\n" +
		"FROM part_supps, suppliers, nations\n" +
		"WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationKey AND n_name = ''%s''\n" +
		"GROUP BY ps_partkey" + "')"
}

func (client *PgQueryClient) GetQ11ViewNationSumStatementRedirect() string {
	return "SELECT create_immv('q11n%d', '" +
		"SELECT n_nationkey, sum(ps_supplycost * ps_availqty * 0.0001/%f) as factor\n" + //Added n_nationkey as it is a requirement from the iimv extension
		"FROM part_supps, suppliers, nations\n" +
		"WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = ''%s''\n" +
		"GROUP BY n_nationkey" + "')"
}

func prepareQ11NoViewBaseQueryStatement(db *bun.DB) *bun.SelectQuery {
	return db.NewSelect().ColumnExpr("ps_partkey, sum(ps_supplycost * ps_availqty) as value").
		Table("part_supps", "suppliers", "nations").OrderExpr("value desc")
	//.GroupExpr("ps_partkey having sum(ps_supplycost * ps_availqty) > (SELECT sum(ps_supplycost * ps_availqty) * 0.0001) FROM part_supps, suppliers, nations WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = ?)")
}

/*
func (client *PgQueryClient) GetQ11ViewNationSumStatement() string {
	return "SELECT create_immv('q11n?', '" +
		"SELECT ps_partkey, sum(ps_supplycost * ps_availqty) * 0.0001/? as factor\n" + //Added n_nationkey as it is a requirement from the iimv extension
		"FROM part_supps, suppliers, nations\n" +
		"WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey and n_name = '?'\n" +
		"GROUP BY ps_partkey" + "')"
}*/

func (client *PgQueryClient) MakeQ11View() {
	success := true
	baseStatement, nationStatement := client.GetQ11ViewBaseStatement(), client.GetQ11ViewNationSumStatement()
	for i, nationName := range tpch.NATIONS_NAME {
		//nationName := client.SQLTables.Nations[i].N_NAME
		_, err := client.DB.NewRaw(baseStatement, i, bun.Safe(nationName)).Exec(client.Ctx)
		if err != nil {
			fmt.Printf("[PGViews]MakeQ11View. Error in making base view for nation %d (%s). Error: %v\n", i, nationName, err)
			success = false
		}
		_, err = client.DB.NewRaw(nationStatement, i, client.Sf, bun.Safe(nationName)).Exec(client.Ctx)
		if err != nil {
			fmt.Printf("[PGViews]MakeQ11View. Error in making nation view for nation %d (%s). Error: %v\n", i, nationName, err)
			success = false
		}
	}
	if success {
		fmt.Printf("[PGViews]MakeQ11View - success.\n")
	}
}

func (client *PgQueryClient) MakeQ11ViewRedirect() {
	statements := make([]string, len(tpch.NATIONS_NAME)*2)
	baseStatement, nationStatement := client.GetQ11ViewBaseStatementRedirect(), client.GetQ11ViewNationSumStatementRedirect()
	for i, nationName := range tpch.NATIONS_NAME {
		//nationName := client.SQLTables.Nations[i].N_NAME
		statements[i*2] = fmt.Sprintf(baseStatement, i, nationName)
		statements[i*2+1] = fmt.Sprintf(nationStatement, i, client.Sf, nationName)
	}
	tpch.SendProto(tpch.PB_CREATE_VIEW, &proto.CreateView{Statement: statements}, client.conn)
	_, replyProto, _ := tpch.ReceiveProto(client.conn)
	viewResp := replyProto.(*proto.CreateViewResp)
	if viewResp.GetErrorMsg() != "" {
		fmt.Println(viewResp.GetErrorMsg())
	} else {
		fmt.Printf("[PGViews]MakeQ11View - success.\n")
	}
}

func (client *PgQueryClient) GetQ11QueryArgs() tpch.Q11Args {
	rndNation := client.Rng.Intn(len(client.SQLTables.Nations))
	return tpch.Q11Args{FirstFrom: "q11p" + strconv.Itoa(rndNation), SecondFrom: "q11n" + strconv.Itoa(rndNation), FirstOrderBy: "value desc", FirstLimit: 100}
}

func (client *PgQueryClient) DoQ11QueryRedirect() {
	tpch.SendProto(tpch.PB_QUERY, client.GetQ11QueryArgs().ToProtobuf(), client.conn)
	_, pb, _ := tpch.ReceiveProto(client.conn)
	results := pb.(*proto.QueryResp).GetResults()
	ignore(results)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+2, client.nRows+101
	client.nReadFields, client.nResultFields = client.nReadFields+202, client.nResultFields+200
}

func (client *PgQueryClient) PrepareQ11QueryRedirect() {
	client.CurrQProto.Queries[client.QProtoI] = client.GetQ11QueryArgs().ToProtobuf()
}

func (client *PgQueryClient) DoQ11Query() {
	rndNation, q11BaseResult, q11NationResult := client.Rng.Intn(len(client.Nations)), make([]tpch.Q11BaseResult, 100), tpch.Q11NationResult{}
	err := client.DB.NewSelect().ModelTableExpr("q11p"+strconv.Itoa(rndNation)).Order("value desc").Limit(100).Scan(client.Ctx, &q11BaseResult)
	if err != nil {
		fmt.Printf("[PGViews]Q11Query. Error on base view q11%d: %s\n", rndNation, err)
	} else {
		//fmt.Printf("[PGViews]Q11Query. Success: %v\n", q11BaseResult)
	}
	err = client.DB.NewSelect().ModelTableExpr("q11n"+strconv.Itoa(rndNation)).Scan(client.Ctx, &q11NationResult)
	if err != nil {
		fmt.Printf("[PGViews]Q11Query. Error on nation view q11n%d: %s\n", rndNation, err)
	} else if PRINT_QUERY {
		for i, res := range q11BaseResult {
			if res.Value <= q11NationResult.Factor {
				q11BaseResult = q11BaseResult[:i] //The current one is not considered
				break
			}
		}
		fmt.Printf("[PGViews]Q11Query. Success. Result: %+v\n (Nation factor: %f)\n", q11BaseResult, q11NationResult.Factor)
		//fmt.Printf("[PGViews]Q11Query. Success (nation view): %v\n", q11NationResult)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+2, client.nRows+101
	client.nReadFields, client.nResultFields = client.nReadFields+202, client.nResultFields+200
}

func (client *PgQueryClient) DoQ11QueryNoView() {
	rndNation, q11Result := client.Rng.Intn(len(client.Nations)), make([]tpch.Q11BaseResult, 100)
	nationS := client.Nations[rndNation]
	query := prepareQ11NoViewBaseQueryStatement(client.DB).Where("ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = '?'", bun.Safe(nationS.N_NAME))
	query = query.GroupExpr("ps_partkey having sum(ps_supplycost * ps_availqty) > (SELECT sum(ps_supplycost * ps_availqty * 0.0001) FROM part_supps, suppliers, nations WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = '?')", bun.Safe(nationS.N_NAME))
	err := query.Scan(client.Ctx, &q11Result)
	if err != nil {
		fmt.Printf("[PGViews]Q11Query. Error on Q11 (no view): %s\n", err)
		//fmt.Printf("%+v\n", *query)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q11Query. Success: %v\n", q11Result)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+101
	client.nReadFields, client.nResultFields = client.nReadFields+101, client.nResultFields+100
}

func (client *PgQueryClient) GetQ12ViewStatement() string {
	return "SELECT create_immv('q12?', '" + "')"
}

func (client *PgQueryClient) GetQ12ViewStatementRedirect() string {
	return "SELECT create_immv('q12?', '" + "')"
}

func (client *PgQueryClient) MakeQ12View() {

}

func (client *PgQueryClient) DoQ12Query() {

}

func (client *PgQueryClient) GetQ13ViewStatement() string {
	return "SELECT create_immv('q13?', '" + "')"
}

func (client *PgQueryClient) GetQ13ViewStatementRedirect() string {
	return "SELECT create_immv('q13?', '" + "')"
}

func (client *PgQueryClient) MakeQ13View() {

}

func (client *PgQueryClient) DoQ13Query() {

}

// Split by year [1993-1997] and month.
// TODO: May need to split this into two views, as with iimvs we can't do mathematical operations of an aggregate with anything else
func (client *PgQueryClient) GetQ14ViewStatement() string {
	return "SELECT create_immv('q14?', '" +
		"SELECT sum((case when p_type like ''PROMO%'' then l_extendedprice*(1-l_discount)*100.00 else 0 end)/(l_extendedprice * (1-l_discount))) as promo_revenue\n" +
		"FROM line_items, parts\n" +
		"WHERE l_partkey = p_partkey AND l_shipdate >= ''?'' AND l_shipdate < ''?''" + "')"
}

func (client *PgQueryClient) GetQ14ViewStatementRedirect() string {
	return "SELECT create_immv('q14%s', '" +
		"SELECT sum((case when p_type like ''PROMO%%'' then l_extendedprice*(1-l_discount)*100.00 else 0 end)/(l_extendedprice * (1-l_discount))) as promo_revenue\n" +
		"FROM line_items, parts\n" +
		"WHERE l_partkey = p_partkey AND l_shipdate >= ''%s'' AND l_shipdate < ''%s''" + "')"
}

func prepareQ14NoViewBaseQueryStatement(db *bun.DB) *bun.SelectQuery {
	return db.NewSelect().ColumnExpr("100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end)/sum(l_extendedprice * (1-l_discount)) as promo_revenue").
		Table("line_items", "parts")
}

/*
return "SELECT create_immv('q14?', '" +
		"SELECT sum(case when p_type like ''PROMO%'' then l_extendedprice*(1-l_discount)*100.00 else 0 end) / sum(l_extendedprice * (1-l_discount)) as promo_revenue\n" +
		"FROM line_items, parts\n" +
		"WHERE l_partkey = p_partkey AND l_shipdate >= ''?'' AND l_shipdate < ''?''" + "')"
*/

func (client *PgQueryClient) MakeQ14View() {
	success, statement, date := true, client.GetQ14ViewStatement(), time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	dateS := date.Format(tpch.TIME_PARSE_LAYOUT)
	notifyChan, nWait := make(chan bool, 5), 5
	for year := 1993; year <= 1997; year++ {
		yearS := strconv.Itoa(year)
		go func(year int, yearS string, date time.Time, dateS string) {
			for month := 1; month <= 12; month++ {
				nextDate := time.Date(year, time.Month(month+1), 1, 0, 0, 0, 0, TIMEZONE)
				nextDateS := nextDate.Format(tpch.TIME_PARSE_LAYOUT)
				_, err := client.DB.NewRaw(statement, bun.Safe(yearS+strconv.Itoa(month)), bun.Safe(dateS), bun.Safe(nextDateS)).Exec(client.Ctx)
				if err != nil {
					fmt.Printf("[PGViews]MakeQ14View. Error for year %d, month %d: %s\n", year, month, err)
					success = false
				}
				date, dateS = nextDate, nextDateS
			}
			notifyChan <- true
		}(year, yearS, date, dateS)
	}
	for i := 0; i < nWait; i++ {
		<-notifyChan
	}
	if success {
		fmt.Printf("[PGViews]MakeQ14View - success.\n")
	}
}

func (client *PgQueryClient) MakeQ14ViewRedirect() {
	statement, date, statements := client.GetQ14ViewStatementRedirect(), time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE), make([]string, 5*12)
	dateS, j := date.Format(tpch.TIME_PARSE_LAYOUT), 0
	for year := 1993; year <= 1997; year++ {
		yearS := strconv.Itoa(year)
		for month := 1; month <= 12; month++ {
			nextDate := time.Date(year, time.Month(month+1), 1, 0, 0, 0, 0, TIMEZONE)
			nextDateS := nextDate.Format(tpch.TIME_PARSE_LAYOUT)
			statements[j] = fmt.Sprintf(statement, yearS+strconv.Itoa(month), dateS, nextDateS)
			j++
			date, dateS = nextDate, nextDateS
		}
	}
	tpch.SendProto(tpch.PB_CREATE_VIEW, &proto.CreateView{Statement: statements}, client.conn)
	_, replyProto, _ := tpch.ReceiveProto(client.conn)
	viewResp := replyProto.(*proto.CreateViewResp)
	if viewResp.GetErrorMsg() != "" {
		fmt.Println(viewResp.GetErrorMsg())
	} else {
		fmt.Printf("[PGViews]MakeQ14View - success.\n")
	}
}

func (client *PgQueryClient) GetQ14QueryArgs() tpch.Q14Args {
	rndYear, rndMonth := client.Rng.Intn(5)+1993, client.Rng.Intn(12)+1
	return tpch.Q14Args{From: "q14" + strconv.Itoa(rndYear) + strconv.Itoa(rndMonth)}
}

func (client *PgQueryClient) DoQ14QueryRedirect() {
	tpch.SendProto(tpch.PB_QUERY, client.GetQ14QueryArgs().ToProtobuf(), client.conn)
	_, pb, _ := tpch.ReceiveProto(client.conn)
	results := pb.(*proto.QueryResp).GetResults() //1 row, 1 column
	ignore(results)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
	client.nReadFields, client.nResultFields = client.nReadFields+1, client.nResultFields+1
}

func (client *PgQueryClient) PrepareQ14QueryRedirect() {
	client.CurrQProto.Queries[client.QProtoI] = client.GetQ14QueryArgs().ToProtobuf()
}

func (client *PgQueryClient) DoQ14Query() {
	rndYear, rndMonth, q14Result := client.Rng.Intn(5)+1993, client.Rng.Intn(12)+1, tpch.Q14Result{}
	key := "q14" + strconv.Itoa(rndYear) + strconv.Itoa(rndMonth)
	err := client.DB.NewSelect().ModelTableExpr(key).Scan(client.Ctx, &q14Result)
	if err != nil {
		fmt.Printf("[PGViews]Q14Query. Error on view %s: %s\n", key, err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q14Query. Success: %v\n", q14Result)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
	client.nReadFields, client.nResultFields = client.nReadFields+1, client.nResultFields+1
}

func (client *PgQueryClient) DoQ14QueryNoView() {
	rndYear, rndMonth, q14Result := client.Rng.Intn(5)+1993, client.Rng.Intn(12)+1, tpch.Q14Result{}
	//query := client.q14NoViewBaseQuery.Where("l_partkey = p_partkey AND EXTRACT(year FROM l_shipdate) = ? AND EXTRACT(month FROM l_shipdate) = ?",
	//rndYear, rndMonth)
	var startDate string
	if rndMonth < 10 {
		startDate = strconv.Itoa(rndYear) + "-0" + strconv.Itoa(rndMonth) + "-01"
	} else {
		startDate = strconv.Itoa(rndYear) + "-" + strconv.Itoa(rndMonth) + "-01"
	}
	var endDate string
	if rndMonth < 12 {
		if rndMonth < 9 {
			endDate = strconv.Itoa(rndYear) + "-0" + strconv.Itoa(rndMonth+1) + "-01"
		} else {
			endDate = strconv.Itoa(rndYear) + "-" + strconv.Itoa(rndMonth+1) + "-01"
		}
	} else {
		endDate = strconv.Itoa(rndYear+1) + "-01-01"
	}
	query := prepareQ14NoViewBaseQueryStatement(client.DB).Where("l_partkey = p_partkey AND l_shipdate >= date '?' AND l_shipdate < date '?'",
		bun.Safe(startDate), bun.Safe(endDate))
	err := query.Scan(client.Ctx, &q14Result)
	if err != nil {
		fmt.Printf("[PGViews]Q14Query. Error on Q14 (no view): %s\n", err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q14Query. Success: %v\n", q14Result)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
	client.nReadFields, client.nResultFields = client.nReadFields+1, client.nResultFields+1
}

// We can't build a view on top of a view.
// So how can we calculate the sum of revenue?
// Maybe just make a view to implement revenue. Then when querying, do the max?
// ORDER BY s_suppkey	(kinda irrelevant since usually only one result per query.)
// Cannot nest aggregate functions, even in normal SQL (i.e., max(sum())) is not allowed.
// And cannot make views out of views.
// Thus, the query itself has to apply the max :(
// Split by quarter of years [1993-1997]
func (client *PgQueryClient) GetQ15ViewStatement() string {
	return "SELECT create_immv('q15?', '" +
		"SELECT l_suppkey, sum(l_extendedprice * (1-l_discount)) as total_revenue\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate >= ''?'' AND l_shipdate < ''?''\n" +
		"GROUP BY l_suppkey')"
}

func (client *PgQueryClient) GetQ15ViewStatementRedirect() string {
	return "SELECT create_immv('q15%s', '" +
		"SELECT l_suppkey, sum(l_extendedprice * (1-l_discount)) as total_revenue\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate >= ''%s'' AND l_shipdate < ''%s''\n" +
		"GROUP BY l_suppkey')"
}

func prepareQ15NoViewBaseQueryStatement(db *bun.DB) *bun.SelectQuery {
	return db.NewSelect().Column("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue").Order("s_suppkey")
}

func (client *PgQueryClient) MakeQ15View() {
	success, statement, date := true, client.GetQ15ViewStatement(), time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	dateS := date.Format(tpch.TIME_PARSE_LAYOUT)
	for year := 1993; year <= 1997; year++ {
		yearS := strconv.Itoa(year)
		for month := 1; month <= 12; month += 3 {
			nextDate := time.Date(year, time.Month(month+3), 1, 0, 0, 0, 0, TIMEZONE)
			nextDateS := nextDate.Format(tpch.TIME_PARSE_LAYOUT)
			_, err := client.DB.NewRaw(statement, bun.Safe(yearS+strconv.Itoa(month)), bun.Ident(dateS), bun.Ident(nextDateS)).Exec(client.Ctx)
			if err != nil {
				fmt.Printf("[PGViews]MakeQ15View. Error for year %d, month %d: %s\n", year, month, err)
				success = false
			}
			date, dateS = nextDate, nextDateS
		}
	}
	if success {
		fmt.Printf("[PGViews]MakeQ15View - success.\n")
	}
}

func (client *PgQueryClient) MakeQ15ViewRedirect() {
	statement, date, statements := client.GetQ15ViewStatementRedirect(), time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE), make([]string, 5*4)
	dateS, j := date.Format(tpch.TIME_PARSE_LAYOUT), 0
	for year := 1993; year <= 1997; year++ {
		yearS := strconv.Itoa(year)
		for month := 1; month <= 12; month += 3 {
			nextDate := time.Date(year, time.Month(month+3), 1, 0, 0, 0, 0, TIMEZONE)
			nextDateS := nextDate.Format(tpch.TIME_PARSE_LAYOUT)
			statements[j] = fmt.Sprintf(statement, yearS+strconv.Itoa(month), dateS, nextDateS)
			j++
			date, dateS = nextDate, nextDateS
		}
	}
	tpch.SendProto(tpch.PB_CREATE_VIEW, &proto.CreateView{Statement: statements}, client.conn)
	_, replyProto, _ := tpch.ReceiveProto(client.conn)
	viewResp := replyProto.(*proto.CreateViewResp)
	if viewResp.GetErrorMsg() != "" {
		fmt.Println(viewResp.GetErrorMsg())
	} else {
		fmt.Printf("[PGViews]MakeQ15View - success.\n")
	}
}

func (client *PgQueryClient) GetQ15QueryArgs() tpch.Q15Args {
	rndYear, rndQuarter := client.Rng.Intn(5)+1993, client.Rng.Intn(4)*3+1
	key := "q15" + strconv.Itoa(rndYear) + strconv.Itoa(rndQuarter)
	columns := []string{"total_revenue", "s_suppkey", "s_name", "s_address", "s_phone"}
	where := "s_suppkey = l_suppkey AND total_revenue = (SELECT max(total_revenue) FROM " + key + ")"
	return tpch.Q15Args{FromOne: key, FromTwo: "suppliers", Columns: columns, Where: where, OrderBy: "s_suppkey"}
}

func (client *PgQueryClient) DoQ15QueryRedirect() {
	tpch.SendProto(tpch.PB_QUERY, client.GetQ15QueryArgs().ToProtobuf(), client.conn)
	_, pb, _ := tpch.ReceiveProto(client.conn)
	results := pb.(*proto.QueryResp).GetResults() //5 columns, 1 row
	ignore(results)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
	client.nReadFields, client.nResultFields = client.nReadFields+5, client.nResultFields+5
}

func (client *PgQueryClient) PrepareQ15QueryRedirect() {
	client.CurrQProto.Queries[client.QProtoI] = client.GetQ15QueryArgs().ToProtobuf()
}

func (client *PgQueryClient) DoQ15Query() {
	rndYear, rndQuarter, q15Result := client.Rng.Intn(5)+1993, client.Rng.Intn(4)*3+1, tpch.Q15Result{}
	key := "q15" + strconv.Itoa(rndYear) + strconv.Itoa(rndQuarter)
	err := client.DB.NewSelect().Column("total_revenue", "s_suppkey", "s_name", "s_address", "s_phone").Table(key, "suppliers").
		Where("s_suppkey = l_suppkey AND total_revenue = (SELECT max(total_revenue) FROM "+key+")").Order("s_suppkey").Scan(client.Ctx, &q15Result)
	if err != nil {
		fmt.Printf("[PGViews]Q15Query. Error on view %s: %s.\n", key, err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q15Query. Success: %v.\n", q15Result)
	}
	//Note: While we only count as one row, internally PostgreSQL may have to fetch many and then apply the max operator
	//However, only one row is sent over the network.
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
	client.nReadFields, client.nResultFields = client.nReadFields+5, client.nResultFields+5
}

func (client *PgQueryClient) DoQ15QueryNoView() {
	rndYear, rndQuarter, q15Result := client.Rng.Intn(5)+1993, client.Rng.Intn(4)*3+1, tpch.Q15Result{}
	viewName := "revenue" + strconv.Itoa(rndYear) + strconv.Itoa(rndQuarter)
	//query := client.q15NoViewBaseQuery.Where("s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM ?))", viewName)
	query := prepareQ15NoViewBaseQueryStatement(client.DB).Where("s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM ?)",
		bun.Safe(viewName)).Table("suppliers", viewName)
	err := query.Scan(client.Ctx, &q15Result)
	if err != nil {
		fmt.Printf("[PGViews]Q15Query. Error on Q15 (no view): %s\n", err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q15Query. Success: %v\n", q15Result)
	}
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+1
	client.nReadFields, client.nResultFields = client.nReadFields+5, client.nResultFields+5
}

func (client *PgQueryClient) GetQ16ViewStatement() string {
	return "SELECT create_immv('q16?', '" + "')"
}

func (client *PgQueryClient) GetQ16ViewStatementRedirect() string {
	return "SELECT create_immv('q16?', '" + "')"
}

func (client *PgQueryClient) MakeQ16View() {

}

func (client *PgQueryClient) DoQ16Query() {

}

func (client *PgQueryClient) GetQ17ViewStatement() string {
	return "SELECT create_immv('q17?', '" + "')"
}

func (client *PgQueryClient) GetQ17ViewStatementRedirect() string {
	return "SELECT create_immv('q17?', '" + "')"
}

func (client *PgQueryClient) MakeQ17View() {

}

func (client *PgQueryClient) DoQ17Query() {

}

// Limit is not supported. Nor is order by. But in practice for SF=1, the limit is not needed (limit=100 but only 10-14 entries actually exist)
// Having is also not supported, and this does affect how the query is written.
// In theory could have all orders in the view and then filter (on the query) those with quantity > the value we want.
// Another option would be to modify orders to include a field with the sum of quantities... or have a trigger fill that in for orders.
// The only reasonable solution for now is to define a trigger on lineitems to update the order with the sum of quantity.
// And with this, the query can be done efficiently.
func (client *PgQueryClient) GetQ18ViewStatement() string {
	return "SELECT create_immv('q18?', '" +
		"SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, o_sumquantity\n" +
		"FROM customers, orders\n" +
		"WHERE c_custkey = o_custkey AND o_sumquantity > ?" + "')"
}

func (client *PgQueryClient) GetQ18ViewStatementRedirect() string {
	return "SELECT create_immv('q18%d', '" +
		"SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, o_sumquantity\n" +
		"FROM customers, orders\n" +
		"WHERE c_custkey = o_custkey AND o_sumquantity > %d" + "')"
}

func prepareQ18NoViewBaseQueryStatement(db *bun.DB) *bun.SelectQuery {
	return db.NewSelect().Column("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice").ColumnExpr("sum(l_quantity)").
		Table("customers", "orders", "line_items").Group("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice").OrderExpr("o_totalprice desc, o_orderdate").Limit(100)
	//.GroupExpr("ps_partkey having sum(ps_supplycost * ps_availqty) > (SELECT sum(ps_supplycost * ps_availqty) * 0.0001) FROM part_supps, suppliers, nations WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = ?)")
}

func (client *PgQueryClient) MakeQ18View() {
	success, statement := true, client.GetQ18ViewStatement()
	for quantity := 312; quantity <= 315; quantity++ {
		_, err := client.DB.NewRaw(statement, quantity, quantity).Exec(client.Ctx)
		if err != nil {
			fmt.Printf("[PGViews]MakeQ18View. Error for quantity %d: %s.\n", quantity, err)
			success = false
		}
	}
	if success {
		fmt.Println("[PGViews]MakeQ18View - success.")
	}
}

func (client *PgQueryClient) MakeQ18ViewRedirect() {
	statement, statements := client.GetQ18ViewStatementRedirect(), make([]string, 4)
	for quantity := 312; quantity <= 315; quantity++ {
		statements[quantity-312] = fmt.Sprintf(statement, quantity, quantity)
	}
	tpch.SendProto(tpch.PB_CREATE_VIEW, &proto.CreateView{Statement: statements}, client.conn)
	_, replyProto, _ := tpch.ReceiveProto(client.conn)
	viewResp := replyProto.(*proto.CreateViewResp)
	if viewResp.GetErrorMsg() != "" {
		fmt.Println(viewResp.GetErrorMsg())
	} else {
		fmt.Println("[PGViews]MakeQ18View - success.")
	}
}

func (client *PgQueryClient) GetQ18QueryArgs() tpch.Q18Args {
	return tpch.Q18Args{From: "q18" + strconv.Itoa(client.Rng.Intn(4)+312), OrderByOne: "o_totalprice desc", OrderByTwo: "o_orderdate", Limit: 100}
}

func (client *PgQueryClient) DoQ18QueryRedirect() {
	tpch.SendProto(tpch.PB_QUERY, client.GetQ18QueryArgs().ToProtobuf(), client.conn)
	_, pb, _ := tpch.ReceiveProto(client.conn)
	results := pb.(*proto.QueryResp).GetResults() //6 columns per row
	nRows, nFields := len(results)/6, len(results)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+nRows
	client.nReadFields, client.nResultFields = client.nReadFields+nFields, client.nResultFields+nFields
}

func (client *PgQueryClient) PrepareQ18QueryRedirect() {
	client.CurrQProto.Queries[client.QProtoI] = client.GetQ18QueryArgs().ToProtobuf()
}

func (client *PgQueryClient) DoQ18Query() {
	quantity, q18Result := client.Rng.Intn(4)+312, make([]tpch.Q18Result, 0, 100)
	key := "q18" + strconv.Itoa(quantity)
	err := client.DB.NewSelect().ModelTableExpr(key).Order("o_totalprice desc", "o_orderdate").Limit(100).Scan(client.Ctx, &q18Result)
	if err != nil {
		fmt.Printf("[PGViews]Q18Query. Error on view %s: %s\n", key, err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q18Query. Success: %v\n", q18Result)
	}
	nRows := len(q18Result)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+nRows
	client.nReadFields, client.nResultFields = client.nReadFields+nRows*6, client.nResultFields+nRows*6
}

func (client *PgQueryClient) DoQ18QueryNoView() {
	quantity, q18Result := client.Rng.Intn(4)+312, make([]tpch.Q18Result, 0, 100)
	query := prepareQ18NoViewBaseQueryStatement(client.DB).Where("o_orderkey in (SELECT l_orderkey FROM line_items GROUP BY l_orderkey having sum(l_quantity) > ?) AND c_custkey = o_custkey AND o_orderkey = l_orderkey", quantity)
	err := query.Scan(client.Ctx, &q18Result)
	if err != nil {
		fmt.Printf("[PGViews]Q18Query. Error on Q18 (no view): %s\n", err)
	} else if PRINT_QUERY {
		fmt.Printf("[PGViews]Q18Query. Success: %v\n", q18Result)
	}
	nRows := len(q18Result)
	client.nQueries, client.nReads, client.nRows = client.nQueries+1, client.nReads+1, client.nRows+nRows
	client.nReadFields, client.nResultFields = client.nReadFields+nRows*6, client.nResultFields+nRows*6
}

func (client *PgQueryClient) GetQ19ViewStatement() string {
	return "SELECT create_immv('q19', '" + "')"
}

func (client *PgQueryClient) GetQ19ViewStatementRedirect() string {
	return "SELECT create_immv('q19?', '" + "')"
}

func (client *PgQueryClient) MakeQ19View() {

}

func (client *PgQueryClient) DoQ19Query() {

}

func (client *PgQueryClient) GetQ20ViewStatement() string {
	return "SELECT create_immv('q20?', '" + "')"
}

func (client *PgQueryClient) GetQ20ViewStatementRedirect() string {
	return "SELECT create_immv('q20?', '" + "')"
}

func (client *PgQueryClient) MakeQ20View() {

}

func (client *PgQueryClient) DoQ20Query() {

}

func (client *PgQueryClient) GetQ21ViewStatement() string {
	return "SELECT create_immv('q21?', '" + "')"
}

func (client *PgQueryClient) GetQ21ViewStatementRedirect() string {
	return "SELECT create_immv('q21?', '" + "')"
}

func (client *PgQueryClient) MakeQ21View() {

}

func (client *PgQueryClient) DoQ21Query() {

}

func (client *PgQueryClient) DoUpdate(newOrdersS [][]string, newItemsS [][]string, remOrderKeys []int32, nRemItems int) {
	newOrders, newItems := make([]*tpch.Orders, len(newOrdersS)), make([]*tpch.LineItem, len(newItemsS))
	for i, orderS := range newOrdersS {
		newOrders[i] = client.SQLTables.CreateOrder(orderS)
	}
	for i, itemS := range newItemsS {
		newItems[i] = client.SQLTables.CreateLineItem(itemS)
	}
	_, err := client.DB.NewInsert().Model(&newOrders).Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on order insert with data %v: %v.\n", newOrders, err)
	}
	_, err = client.DB.NewInsert().Model(&newItems).Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on items insert with data %v: %v.\n", newItems, err)
	}
	remOrders := make([]*tpch.Orders, len(remOrderKeys))
	for i, key := range remOrderKeys {
		remOrders[i] = &tpch.Orders{O_ORDERKEY: key}
	}
	_, err = client.DB.NewDelete().Model(&remOrders).WherePK().Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on order delete with data %v: %v.\n", remOrders, err)
	}
	//Items are deleted on cascade
	client.nNewOrders, client.nDelOrders = client.nNewOrders+len(newOrders), client.nDelOrders+len(remOrders)
	client.nNewItems, client.nDelItems = client.nNewItems+len(newItems), client.nDelItems+nRemItems
}

/*func (client *PgQueryClient) DoUpdate(newOrders []*tpch.Orders, newItems [][]*tpch.LineItem, remOrderKeys []int32, nRemItems []int8) {
	_, err := client.DB.NewInsert().Model(&newOrders).Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on order insert with data %v: %v.\n", newOrders, err)
	}
	_, err = client.DB.NewInsert().Model(&newItems).Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on items insert with data %v: %v.\n", newItems, err)
	}
	remOrders := make([]*tpch.Orders, len(remOrderKeys))
	for i, key := range remOrderKeys {
		remOrders[i] = &tpch.Orders{O_ORDERKEY: key}
	}
	_, err = client.DB.NewDelete().Model(&remOrders).WherePK().Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on order delete with data %v: %v.\n", remOrders, err)
	}
	//Items are deleted on cascade
	countNewItems, countDelItems := 0, 0
	for i, items := range newItems {
		countNewItems, countDelItems = countNewItems+len(items), countDelItems+int(nRemItems[i])
	}
	client.nNewOrders, client.nDelOrders = client.nNewOrders+len(newOrders), client.nDelOrders+len(remOrders)
	client.nNewItems, client.nDelItems = client.nNewItems+countNewItems, client.nDelItems+countDelItems
}*/

func (client *PgQueryClient) DoUpdateRedirect(newOrdersS [][]string, newItemsS [][]string, remOrderKeys []int32, nRemItems int) {
	newOrders, newItems, deletes := make([]*proto.InsertOrder, len(newOrdersS)), make([]*proto.InsertLineItem, len(newItemsS)), make([]string, len(remOrderKeys))
	orderTableName := &(tpch.PG_TABLE_IDS[tpch.ORDERS])
	for i, newOrder := range newOrdersS {
		newOrders[i], deletes[i] = client.SQLTables.CreateOrder(newOrder).ToProto(), strconv.Itoa(int(remOrderKeys[i]))
	}
	err := tpch.SendProto(tpch.PB_MULTI_INSERT_TPCH, &proto.MultiTpchUpdate{Orders: newOrders, Items: newItems, DeleteTable: orderTableName, DeleteIds: deletes}, client.conn)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on order insert with orders %v, items %v, deleteKeys %v: %v.\n", newOrders, newItems, remOrderKeys, err)
	}
	_, _, err = tpch.ReceiveProto(client.conn)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on receiving confirmation with data %v: %v.\n", newOrders, err)
	}
	client.nNewOrders, client.nDelOrders = client.nNewOrders+len(newOrders), client.nDelOrders+len(remOrderKeys)
	client.nNewItems, client.nDelItems = client.nNewItems+len(newItems), client.nDelItems+nRemItems
}

// In multi-update, for delete, we only need the PK.
/*func (client *PgQueryClient) DoUpdateRedirect(newOrders []*tpch.Orders, newItems [][]*tpch.LineItem, remOrderKeys []int32, nRemItems []int8) {
	inserts, deletes := make([]*proto.InsertOrderItems, len(newOrders)), make([]*proto.Delete, len(remOrderKeys))
	orderTableName := &(tpch.PG_TABLE_IDS[tpch.ORDERS])
	countNewItems, countDelItems := 0, 0
	for i, newOrder := range newOrders {
		protoOrder, protoItems, delWhere := newOrder.ToProto(), tpch.FromLineItemsSliceToProto(newItems[i]), strconv.Itoa(int(remOrderKeys[i]))
		inserts[i], deletes[i] = &proto.InsertOrderItems{Order: protoOrder, LineItems: protoItems}, &proto.Delete{Table: orderTableName, Condition: &delWhere}
		countNewItems, countDelItems = countNewItems+len(newItems[i]), countDelItems+int(nRemItems[i])
	}
	err := tpch.SendProto(tpch.PB_INSERT_TPCH, &proto.TpchUpdate{Insert: inserts, Delete: deletes}, client.conn)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on order insert with orders %v, items %v, deleteKeys %v: %v.\n", newOrders, newItems, remOrderKeys, err)
	}
	_, _, err = tpch.ReceiveProto(client.conn)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on receiving confirmation with data %v: %v.\n", newOrders, err)
	}
	client.nNewOrders, client.nDelOrders = client.nNewOrders+len(newOrders), client.nDelOrders+len(remOrderKeys)
	client.nNewItems, client.nDelItems = client.nNewItems+countNewItems, client.nDelItems+countDelItems
}*/

func (client *PgQueryClient) DoSingleUpdate(newOrder *tpch.Orders, newItems []*tpch.LineItem, remOrderKey int32, nRemItems int) {
	_, err := client.DB.NewInsert().Model(newOrder).Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on order insert with data %v: %v.\n", newOrder, err)
	}
	_, err = client.DB.NewInsert().Model(&newItems).Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on items insert with data %v: %v.\n", newItems, err)
	}
	/*delItem := &tpch.LineItem{}
	res, errItems := client.DB.NewDelete().Model(delItem).Where("l_orderkey = " + strconv.Itoa(int(remOrderKey))).Exec(client.Ctx)
	//res, errItems := client.DB.NewDelete().ModelTableExpr("line_items").Where("l_orderkey = " + strconv.Itoa(int(remOrderKey))).Exec(client.Ctx)
	//fmt.Printf("[PGSQuery]Finished deleting items. Result: %+v. Error: %v.\n", res, errItems)
	ignore(res)
	ignore(errItems)
	remOrder := &tpch.Orders{O_ORDERKEY: remOrderKey}
	_, err = client.DB.NewDelete().Model(remOrder).WherePK().Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on order delete with data %v: %v.\n", remOrder, err)
	}
	//Items will be deleted by cascade
	client.nNewOrders, client.nDelOrders = client.nNewOrders+1, client.nDelOrders+1
	client.nNewItems, client.nDelItems = client.nNewItems+len(newItems), client.nDelItems+nRemItems*/

	client.nNewOrders, client.nNewItems = client.nNewOrders+1, client.nNewItems+len(newItems)
}

func (client *PgQueryClient) DoSingleUpdateRedirect(newOrder *tpch.Orders, newItems []*tpch.LineItem, remOrderKey int32, nRemItems int) {
	protoOrder, protoItems, delWhere := newOrder.ToProto(), tpch.FromLineItemsSliceToProto(newItems), "o_orderkey = "+strconv.Itoa(int(remOrderKey))
	err := tpch.SendProto(tpch.PB_INSERT_TPCH, &proto.TpchUpdate{Insert: &proto.InsertOrderItems{Order: protoOrder, LineItems: protoItems},
		Delete: &proto.Delete{Table: &(tpch.PG_TABLE_IDS[tpch.ORDERS]), Condition: &delWhere}}, client.conn)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on order insert with order %v, item %v, delete with condition %s: %v.\n",
			newOrder, newItems, delWhere, err)
	}
	_, _, err = tpch.ReceiveProto(client.conn)
	if err != nil {
		fmt.Printf("[PGSQuery]OrderItemsInsert. Error on receiving confirmation with order %v, item %v, delete with condition %s: %v.\n", newOrder, newItems, delWhere, err)
	}
	client.nNewOrders, client.nDelOrders = client.nNewOrders+1, client.nDelOrders+1
	client.nNewItems, client.nDelItems = client.nNewItems+len(newItems), client.nDelItems+nRemItems
}

func (client *PgQueryClient) CreateViews() {
	var notifyChan chan bool
	start := time.Now().UnixNano()
	var nWait int
	if client.conn == nil {
		if UPDATE_SPECIFIC_INDEX_ONLY {
			viewFuncs := make([]func(), len(client.QueryFuncs))
			notifyChan, nWait = make(chan bool, len(client.QueryFuncs)), len(client.QueryFuncs)
			for i, qId := range QUERY_FUNCS_INT {
				switch qId {
				case 1:
					viewFuncs[i] = client.MakeQ1View
				case 3:
					viewFuncs[i] = client.MakeQ3View
				case 5:
					viewFuncs[i] = client.MakeQ5View
				case 6:
					viewFuncs[i] = client.MakeQ6View
				case 11:
					viewFuncs[i] = client.MakeQ11View
				case 14:
					viewFuncs[i] = client.MakeQ14View
				case 15:
					viewFuncs[i] = client.MakeQ15View
				case 18:
					viewFuncs[i] = client.MakeQ18View
				}
			}
			for _, viewFunc := range viewFuncs {
				go client.ExecuteAndNotify(viewFunc, notifyChan)
			}
		} else {
			notifyChan, nWait = make(chan bool, 6), 6
			//client.MakeQ1View()
			go client.ExecuteAndNotify(client.MakeQ3View, notifyChan)
			go client.ExecuteAndNotify(client.MakeQ5View, notifyChan)
			//client.MakeQ6View()
			go client.ExecuteAndNotify(client.MakeQ11View, notifyChan)
			go client.ExecuteAndNotify(client.MakeQ14View, notifyChan)
			go client.ExecuteAndNotify(client.MakeQ15View, notifyChan)
			go client.ExecuteAndNotify(client.MakeQ18View, notifyChan)
		}
	} else {
		baseSeed, clientId := time.Now().UnixNano(), int64(2^16)
		var viewIds []int
		if UPDATE_SPECIFIC_INDEX_ONLY {
			viewIds = QUERY_FUNCS_INT
		} else {
			//viewIds = []int{1, 3, 5, 6, 11, 14, 15, 18}
			viewIds = []int{3, 5, 11, 14, 15, 18}
		}
		notifyChan, nWait = make(chan bool, len(viewIds)), len(viewIds)
		for _, qId := range viewIds {
			go func(qId int) {
				newConn, _ := net.Dial("tcp", IP_DSN)
				viewClient := CreatePGQueryClientWithoutFuncs(DBInfo{conn: newConn}, client.SQLTables, client.Sf, baseSeed, clientId)
				switch qId {
				case 1:
					viewClient.MakeQ1ViewRedirect()
				case 3:
					viewClient.MakeQ3ViewRedirect()
				case 5:
					viewClient.MakeQ5ViewRedirect()
				case 6:
					viewClient.MakeQ6ViewRedirect()
				case 11:
					viewClient.MakeQ11ViewRedirect()
				case 14:
					viewClient.MakeQ14ViewRedirect()
				case 15:
					viewClient.MakeQ15ViewRedirect()
				case 18:
					viewClient.MakeQ18ViewRedirect()
				}
				notifyChan <- true
				tpch.SendProto(tpch.PB_CLOSE_CONNECTION, &proto.CloseConnection{}, newConn)
				newConn.Close()
			}(qId)
			baseSeed++
			clientId++
		}
	}
	for i := 0; i < nWait; i++ {
		<-notifyChan
	}
	timeTaken := (time.Now().UnixNano() - start) / int64(time.Millisecond)
	fmt.Printf("[PGViews]Finished making all views. Time taken: %dms.\n", timeTaken)
}

func (client *PgQueryClient) CreateIndexes() {
	//Q3, Q14, Q15: l_shipdate
	//Q5: o_orderdate
	//Q11: s_nationkey, ps_suppkey, ps_partkey
	//Q18: nothing :
	_, err := client.DB.NewCreateIndex().Model((*tpch.LineItem)(nil)).Column("l_shipdate").Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGViews]CreateIndexes. Error on line_items (Q3, Q14, Q15) index: %s.\n", err)
	}
	_, err = client.DB.NewCreateIndex().Model((*tpch.Orders)(nil)).ColumnExpr("EXTRACT(year FROM o_orderdate)").Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGViews]CreateIndexes. Error on o_orderdate (Q5) index: %s.\n", err)
	}
	supp := (*tpch.Supplier)(nil)
	_, err = client.DB.NewCreateIndex().Model(supp).Column("s_nationkey").Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGViews]CreateIndexes. Error on s_nationkey (Q11) index: %s.\n", err)
	}
	_, err = client.DB.NewCreateIndex().Model(supp).Column("ps_suppkey").Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGViews]CreateIndexes. Error on ps_suppkey (Q11) index: %s.\n", err)
	}
	_, err = client.DB.NewCreateIndex().Model(supp).Column("ps_partkey").Exec(client.Ctx)
	if err != nil {
		fmt.Printf("[PGViews]CreateIndexes. Error on ps_partkey (Q11) index: %s.\n", err)
	}
}

func (client *PgQueryClient) ExecuteAndNotify(funToRun func(), notifyChan chan bool) {
	funToRun()
	notifyChan <- true
}

func (client *PgQueryClient) QueryViews() {
	//client.DoQ1Query()
	client.DoQ3Query()
	client.DoQ5Query()
	//client.DoQ6Query()
	client.DoQ11Query()
	client.DoQ14Query()
	client.DoQ15Query()
	client.DoQ18Query()
}

func (client *PgQueryClient) QueryViewRedirect() {
	//client.DoQ1QueryRedirect()
	client.DoQ3QueryRedirect()
	client.DoQ5QueryRedirect()
	//client.DoQ6QueryRedirect()
	client.DoQ11QueryRedirect()
	client.DoQ14QueryRedirect()
	client.DoQ15QueryRedirect()
	client.DoQ18QueryRedirect()
}

/*func (client *PgQueryClient) DropViews() {
	client.DropQ1View()
	client.DropQ6View()
}

func (client *PgQueryClient) DropQ1View() {
	for i := 60; i <= 120; i++ {
		client.DB.NewDropTable().ModelTableExpr("q1" + strconv.Itoa(i)).IfExists().Exec(client.Ctx)
	}
}

func (client *PgQueryClient) DropQ6View() {
	for year := 1993; year <= 1997; year++ {
		key := "q6" + strconv.Itoa(year)
		for i := 0.01; i <= 0.08; i += 0.01 { //Discount
			keyQ := key + strconv.Itoa(int((i+0.01)*100))
			keyQ24, keyQ25 := keyQ+"24", keyQ+"25"
			client.DB.NewDropTable().ModelTableExpr(keyQ24).IfExists().Exec(client.Ctx)
			client.DB.NewDropTable().ModelTableExpr(keyQ25).IfExists().Exec(client.Ctx)
		}
	}
}*/

/*
Query:
For making IMMV: db.NewSelect().Column(//create_immv expression).Exec(ctx)
SELECT create_immv('myview', 'SELECT * FROM mytab');

For general queries:
db.NewSelect().Model(&model).Column("col1", "col2", "count(*)").Table("table1", "table2").Join(...).WherePK().Where(...).Group(...).Order(...).Having(...).Limit(100).Offset(100).Exec(ctx)
For more information: https://bun.uptrace.dev/guide/query-select.html#api
*/
