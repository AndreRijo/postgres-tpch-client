package client

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/uptrace/bun"
)

//TODO: Try to do both views for Q1 in a single step.

const ALL_QUERIES = -1

var (
	TIMEZONE = time.FixedZone("Portugal", 0)
)

type PgQueryClient struct {
	DBInfo
	*SQLTables
	Rng *rand.Rand
	Sf  float64
	Id  int64
	QueryStatistics
	QueryFuncs []func()
}

type Q1Result struct {
	bun.BaseModel
	L_returnflag   string
	L_linestatus   string
	Sum_qty        int
	Sum_base_price float64
	Sum_disc_price float64
	Sum_charge     float64
	Avg_qty        float64
	Avg_price      float64
	Avg_disc       float64
	Count_order    int
}

/*
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue,
o_orderdate,
o_shippriority
*/
type Q3Result struct {
	bun.BaseModel
	Revenue        float64
	L_orderkey     int32
	O_orderdate    time.Time
	O_shippriority string
}

type Q5Result struct {
	bun.BaseModel
	N_name  string
	Revenue float64
}

type Q6Result struct {
	bun.BaseModel
	Revenue float64
}

type Q11BaseResult struct {
	bun.BaseModel
	Ps_partkey int32
	Value      float64
}

type Q11NationResult struct {
	bun.BaseModel
	Factor      float64
	N_nationkey int8
}

type Q14Result struct {
	bun.BaseModel
	Promo_revenue float64
}

type Q15Result struct {
	bun.BaseModel
	S_suppkey     string
	S_name        string
	S_address     string
	S_phone       string
	Total_revenue float64
}

type Q18Result struct {
	bun.BaseModel
	C_name        string
	C_custkey     int32
	O_orderkey    int32
	O_orderdate   time.Time
	O_totalprice  float64
	O_sumquantity int
}

type QueryStatistics struct {
	nQueries      int
	nQueryTxns    int
	nReads        int
	nRows         int
	nResultFields int   //Number of fields shown to the user
	nReadFields   int   //Number of actual fields downloaded
	duration      int   //ms
	latency       int64 //ns, later converted to ms.
}

func CreatePGQueryClient(dbInfo DBInfo, tables *SQLTables, sf float64, seed, id int64, queries []int) *PgQueryClient {
	client := &PgQueryClient{DBInfo: dbInfo, SQLTables: tables, Sf: sf, Id: id, Rng: rand.New(rand.NewSource(seed))}
	client.prepareQueryFuncs(queries)
	return client
}

func (client *PgQueryClient) prepareQueryFuncs(queries []int) {
	fmt.Println("Preparing client with queries", queries)
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

func (client *PgQueryClient) SetTables(tables *SQLTables) {
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

func (client *PgQueryClient) MakeQ1View() {
	result, err := client.DB.NewRaw(client.GetQ1ViewStatementBase()).Exec(client.Ctx)
	q1Day := client.GetQ1ViewStatementDay()
	if err != nil {
		fmt.Printf("[PGViews]Start of MakeQ1View. Error: %v. Result: %+v\n", err, result)
	}
	date := time.Date(1998, 10, 2, 0, 0, 0, 0, TIMEZONE)
	for i := 61; i <= 120; i++ {
		date = date.AddDate(0, 0, 1)
		result, err = client.DB.NewRaw(q1Day, i, bun.Ident(date.Format(TIME_PARSE_LAYOUT))).Exec(client.Ctx)
		//result, err = client.DB.NewRaw(q1Day, i, i).Exec(client.Ctx)
		if err != nil {
			fmt.Printf("[PGViews]MakeQ1View. Error: %v. Result: %+v\n", err, result)
		}
	}
	fmt.Printf("[PGViews]MakeQ1View - success.\n")
}

func (client *PgQueryClient) DoQ1Query() {
	//Column("l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc", "count_order")
	q1BaseResult, q1DayResult, randomDay := make([]Q1Result, 4), make([]Q1Result, 4), client.Rng.Intn(60)+61
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

func (client *PgQueryClient) GetQ2ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
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

// DATE [1995-03-01 .. 1995-03-31].
func (client *PgQueryClient) MakeQ3View() {
	success := true
	date := time.Date(1995, 3, 1, 0, 0, 0, 0, TIMEZONE)
	var q3DateKey, q3Date string
	for i := 0; i <= 30; i++ {
		q3DateKey = strconv.Itoa(date.Day())
		q3Date = date.Format(TIME_PARSE_LAYOUT)
		for _, segment := range client.SQLTables.Segments {
			_, err := client.DB.NewRaw(client.GetQ3ViewStatement(), bun.Safe(q3DateKey+segment), bun.Safe(segment), bun.Safe(q3Date), bun.Safe(q3Date)).Exec(client.Ctx)
			if err != nil {
				fmt.Printf("[PGViews]MakeQ3View. Error for segment %s, date 1995-03-%s. Error: %v\n", segment, q3DateKey, err)
				success = false
			}
		}
		date = date.AddDate(0, 0, 1)
	}
	if success {
		fmt.Printf("[PGViews]MakeQ3View - success.\n")
	}
}

func (client *PgQueryClient) DoQ3Query() {
	rndDay, rndSegment, q3Result := client.Rng.Intn(31)+1, client.SQLTables.Segments[client.Rng.Intn(len(client.SQLTables.Segments))], make([]Q3Result, 10)
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

func (client *PgQueryClient) GetQ4ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
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

func (client *PgQueryClient) MakeQ5View() {
	success := true
	date := time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	q5S := client.GetQ5ViewStatement()
	firstDateString := date.Format(TIME_PARSE_LAYOUT)
	for year := 1993; year <= 1997; year++ {
		nextYear := strconv.Itoa(year + 1)
		secondDateString := nextYear + firstDateString[4:]
		for i, region := range client.SQLTables.Regions {
			_, err := client.DB.NewRaw(q5S, year*10+i, bun.Safe(region.R_NAME), bun.Safe(firstDateString), bun.Safe(secondDateString)).Exec(client.Ctx)
			if err != nil {
				success = false
				fmt.Printf("[PGViews]Error creating q5 view. Key: q5%d. Error: %v\n", year*10+i, err)
			}
		}
		firstDateString = secondDateString
	}
	if success {
		fmt.Printf("[PGViews]MakeQ5View - success.\n")
	}
}

func (client *PgQueryClient) DoQ5Query() {
	q5Result, year, region := make([]Q5Result, 5), client.Rng.Intn(5)+1993, client.Rng.Intn(5)
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

func (client *PgQueryClient) GetQ6ViewStatement() string {
	return "SELECT create_immv('q6?', '" +
		"SELECT sum(l_extendedprice*l_discount) as revenue\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate >= ''?'' AND l_shipdate <= ''?'' AND l_discount >= ? AND l_discount <= ? AND l_quantity < ?')"
}

func (client *PgQueryClient) MakeQ6View() {
	success := true
	date := time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	for year := 1993; year <= 1997; year++ {
		endDate := date.AddDate(1, 0, 0)
		dateS, endDateS := date.Format(TIME_PARSE_LAYOUT), endDate.Format(TIME_PARSE_LAYOUT)
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

// Unlike in PotionDB, here it is only one download per query (each view contains already the range of discounts).
func (client *PgQueryClient) DoQ6Query() {
	rndYear, rndQuantity, rndAmount, q6Result := client.Rng.Intn(5)+1993, client.Rng.Intn(8)+2, client.Rng.Intn(2)+24, Q6Result{}
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
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) MakeQ7View() {

}

func (client *PgQueryClient) DoQ7Query() {

}

func (client *PgQueryClient) GetQ8ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) MakeQ8View() {

}

func (client *PgQueryClient) DoQ8Query() {

}

func (client *PgQueryClient) GetQ9ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) MakeQ9View() {

}

func (client *PgQueryClient) DoQ9Query() {

}

func (client *PgQueryClient) GetQ10ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
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
	for i := 0; i < len(client.SQLTables.Nations); i++ {
		nationName := client.SQLTables.Nations[i].N_NAME
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

func (client *PgQueryClient) DoQ11Query() {
	rndNation, q11BaseResult, q11NationResult := client.Rng.Intn(len(client.Nations)), make([]Q11BaseResult, 100), Q11NationResult{}
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

func (client *PgQueryClient) GetQ12ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) MakeQ12View() {

}

func (client *PgQueryClient) DoQ12Query() {

}

func (client *PgQueryClient) GetQ13ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
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

/*
return "SELECT create_immv('q14?', '" +
		"SELECT sum(case when p_type like ''PROMO%'' then l_extendedprice*(1-l_discount)*100.00 else 0 end) / sum(l_extendedprice * (1-l_discount)) as promo_revenue\n" +
		"FROM line_items, parts\n" +
		"WHERE l_partkey = p_partkey AND l_shipdate >= ''?'' AND l_shipdate < ''?''" + "')"
*/

func (client *PgQueryClient) MakeQ14View() {
	success, statement, date := true, client.GetQ14ViewStatement(), time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	dateS := date.Format(TIME_PARSE_LAYOUT)
	for year := 1993; year <= 1997; year++ {
		yearS := strconv.Itoa(year)
		for month := 1; month <= 12; month++ {
			nextDate := time.Date(year, time.Month(month+1), 1, 0, 0, 0, 0, TIMEZONE)
			nextDateS := nextDate.Format(TIME_PARSE_LAYOUT)
			_, err := client.DB.NewRaw(statement, bun.Safe(yearS+strconv.Itoa(month)), bun.Safe(dateS), bun.Safe(nextDateS)).Exec(client.Ctx)
			if err != nil {
				fmt.Printf("[PGViews]MakeQ14View. Error for year %d, month %d: %s\n", year, month, err)
				success = false
			}
			date, dateS = nextDate, nextDateS
		}
	}
	if success {
		fmt.Printf("[PGViews]MakeQ14View - success.\n")
	}
}

func (client *PgQueryClient) DoQ14Query() {
	rndYear, rndMonth, q14Result := client.Rng.Intn(5)+1993, client.Rng.Intn(12)+1, Q14Result{}
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

// We can't build a view on top of a view.
// So how can we calculate the sum of revenue?
// Maybe just make a view to implement revenue. Then when querying, do the max?
// ORDER BY s_suppkey	(kinda irrelevant since usually only one result per query.)
// Cannot nest aggregate functions, even in normal SQL (i.e., max(sum())) is not allowed.
// And cannot make views out of views.
// Thus, the query itself has to apply the max :(
// TODO: Query itself applies the max.
// Split by quarter of years [1993-1997]
func (client *PgQueryClient) GetQ15ViewStatement() string {
	return "SELECT create_immv('q15?', '" +
		"SELECT l_suppkey, sum(l_extendedprice * (1-l_discount)) as total_revenue\n" +
		"FROM line_items\n" +
		"WHERE l_shipdate >= ''?'' AND l_shipdate < ''?''\n" +
		"GROUP BY l_suppkey')"
}

func (client *PgQueryClient) MakeQ15View() {
	success, statement, date := true, client.GetQ15ViewStatement(), time.Date(1993, 1, 1, 0, 0, 0, 0, TIMEZONE)
	dateS := date.Format(TIME_PARSE_LAYOUT)
	for year := 1993; year <= 1997; year++ {
		yearS := strconv.Itoa(year)
		for month := 1; month <= 12; month += 3 {
			nextDate := time.Date(year, time.Month(month+3), 1, 0, 0, 0, 0, TIMEZONE)
			nextDateS := nextDate.Format(TIME_PARSE_LAYOUT)
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

func (client *PgQueryClient) DoQ15Query() {
	rndYear, rndQuarter, q15Result := client.Rng.Intn(5)+1993, client.Rng.Intn(4)*3+1, Q15Result{}
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

func (client *PgQueryClient) GetQ16ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) MakeQ16View() {

}

func (client *PgQueryClient) DoQ16Query() {

}

func (client *PgQueryClient) GetQ17ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
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

func (client *PgQueryClient) DoQ18Query() {
	quantity, q18Result := client.Rng.Intn(4)+312, make([]Q18Result, 0, 100)
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

func (client *PgQueryClient) GetQ19ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) MakeQ19View() {

}

func (client *PgQueryClient) DoQ19Query() {

}

func (client *PgQueryClient) GetQ20ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) MakeQ20View() {

}

func (client *PgQueryClient) DoQ20Query() {

}

func (client *PgQueryClient) GetQ21ViewStatement() string {
	return "SELECT create_immv('q?', '" + "')"
}

func (client *PgQueryClient) MakeQ21View() {

}

func (client *PgQueryClient) DoQ21Query() {

}

func (client *PgQueryClient) CreateViews() {
	if UPDATE_SPECIFIC_INDEX_ONLY {
		viewFuncs := make([]func(), len(client.QueryFuncs))
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
	} else {
		//client.MakeQ1View()
		client.MakeQ3View()
		client.MakeQ5View()
		//client.MakeQ6View()
		client.MakeQ11View()
		client.MakeQ14View()
		client.MakeQ15View()
		client.MakeQ18View()
	}
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
