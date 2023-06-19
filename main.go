package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/pingcap/errors"
)

var SleepMs = 0
var seq = 0

func main() {
	sm := os.Getenv("SLEEP_MS")
	if len(sm) > 0 {
		SleepMs, _ = strconv.Atoi(sm)
	}
	initBenchTabRows()
	host, port, user, pass := get_inner_ip()["inner"], 3306, "root", "supersecret"
	// Listen for connections on localhost port 4000
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("mysql -h%s -P%d -u%s -p%s\n", host, port, user, pass)

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}

		conn, err := server.NewConn(c, user, pass, &testHandler{})
		if err != nil {
			log.Fatal(err)
		}
		seq++
		log.Println(logStr(), "new client", seq)

		go func(conn *server.Conn, seq int) {
			for {
				if err := conn.HandleCommand(); err != nil {
					log.Println(err, seq)
					return
				}
			}
		}(conn, seq)
	}
}

func logStr() string {
	return fmt.Sprintf("[%s]", time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05"))
}

var header = []string{"id", "extra_data", "create_time"}
var rows = make([]string, 0)
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func generateRandomString(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
func initBenchTabRows() {
	maxLen := 10000
	for i := 0; i < maxLen; i++ {
		rows = append(rows, generateRandomString(512-1))
	}
}

func getBenchTabRows(size int, binary bool) (*mysql.Resultset, error) {
	rs := make([][]interface{}, 0)
	for i := 0; i < size; i++ {
		rs = append(rs, []interface{}{
			i + 1,
			rows[i],
			0,
		})
	}
	return mysql.BuildSimpleResultset(header, rs, binary)
}

func getLastNumber(q string) int {
	a := ""
	for i := len(q) - 1; i >= 0; i-- {
		if q[i] == ' ' && len(a) == 0 {
			continue
		}

		if q[i] >= '0' && q[i] <= '9' {
			a = string(q[i]) + a
		} else {
			break
		}
	}
	r, _ := strconv.Atoi(a)
	return r
}

func (h *testHandler) handleQuery(query string, binary bool) (*mysql.Result, error) {
	ss := strings.Split(query, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		var r *mysql.Resultset
		var err error
		if strings.Contains(query, "bench_tab") || strings.Contains(query, "BENCH_TAB") {
			size := getLastNumber(query)
			if size < 0 {
				size = 0
			}
			r, err = getBenchTabRows(size, binary)
			if SleepMs > 0 {
				time.Sleep(time.Duration(SleepMs) * time.Millisecond)
			}
		} else if strings.Contains(strings.ToLower(query), "max_allowed_packet") {
			r, err = mysql.BuildSimpleResultset([]string{"@@max_allowed_packet"}, [][]interface{}{
				{mysql.MaxPayloadLen},
			}, binary)
		} else if strings.Contains(strings.ToLower(query), "sql_auto_is_null") {
			r, err = mysql.BuildSimpleResultset([]string{"@@sql_auto_is_null"}, [][]interface{}{
				{0},
			}, binary)
		} else {
			r, err = mysql.BuildSimpleResultset([]string{"a", "b", "c"}, [][]interface{}{
				{1, "hello world", 0},
			}, binary)
		}

		if err != nil {
			return nil, errors.Trace(err)
		} else {
			return &mysql.Result{
				Status:       0,
				Warnings:     0,
				InsertId:     0,
				AffectedRows: 0,
				Resultset:    r,
			}, nil
		}
	case "rollback", "set":
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid query %s", query)
	}
}

type testHandler struct {
}

func (h *testHandler) UseDB(dbName string) error {
	return nil
}

func (h *testHandler) HandleQuery(query string) (*mysql.Result, error) {
	return h.handleQuery(query, false)
}

func (h *testHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}
func (h *testHandler) HandleStmtPrepare(sql string) (params int, columns int, ctx interface{}, err error) {
	ss := strings.Split(sql, " ")
	switch strings.ToLower(ss[0]) {
	case "select":
		params = 1
		columns = 3
	default:
		err = fmt.Errorf("invalid prepare %s", sql)
	}
	return params, columns, nil, err
}

func (h *testHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *testHandler) HandleStmtExecute(ctx interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return h.handleQuery(query, true)
}

func (h *testHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("command %d is not supported now", cmd))
}

func get_inner_ip() map[string]string {
	addrs, err := net.InterfaceAddrs()
	ret := make(map[string]string)
	ret["loopback"] = "127.0.0.1"
	if err != nil {
		fmt.Println(err)
		return ret
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil && isPrivateIP(ipnet.IP) {
				ret["inner"] = ipnet.IP.String()
				fmt.Println(ipnet.IP.String())
			}
		}
	}
	return ret
}

// 判断是否为内网 IP 地址
func isPrivateIP(ip net.IP) bool {
	privateIPBlocks := []*net.IPNet{
		{IP: net.ParseIP("10.0.0.0"), Mask: net.CIDRMask(8, 32)},
		{IP: net.ParseIP("172.16.0.0"), Mask: net.CIDRMask(12, 32)},
		{IP: net.ParseIP("192.168.0.0"), Mask: net.CIDRMask(16, 32)},
	}

	for _, block := range privateIPBlocks {
		if block.Contains(ip) {
			return true
		}
	}

	return false
}
