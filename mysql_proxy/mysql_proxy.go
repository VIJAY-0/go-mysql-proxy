package mysql_proxy

import (
	"database/sql"
	"strings"

	"fmt"
	"log"
	"net"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	_ "github.com/go-sql-driver/mysql" // Important to use the blank identifier to register the MySQL driver
)

// CustomHandler is your implementation of the server.Handler interface
type CustomHandler struct {
	db *sql.DB
}

func (h *CustomHandler) UseDB(dbName string) error {

	log.Printf("Received UseDB %s", dbName)
	log.Printf("Client switched to database: %s", dbName)
	_, err := h.db.Exec(fmt.Sprintf("USE %s;", dbName))
	if err != nil {
		log.Printf("Query execution failed ... %v", err)
	}
	return nil
}

func ReadQuery(q string) bool {
	q = strings.ToUpper(q)
	return strings.Contains(q, "SELECT") ||
		strings.Contains(q, "SHOW")
}

func (h *CustomHandler) HandleQuery(query string) (*mysql.Result, error) {

	if !ReadQuery(query) {
		result, err := h.db.Exec(query)
		if err != nil {
			log.Printf("Query execution failed ... %v", err)
		}

		lid, err := result.LastInsertId()
		ra, err := result.RowsAffected()

		return &mysql.Result{
			Status:       0,
			Warnings:     0,
			InsertId:     uint64(lid),
			AffectedRows: uint64(ra),
		}, nil
	}

	rows, err := h.db.Query(query)
	if err != nil {
		log.Println("db.Query error")
		return nil, nil
	}

	resSet, err := NewResultsetFromRows(rows)

	result := mysql.Result{
		Status:       uint16(mysql.COM_QUERY),
		Warnings:     0,
		InsertId:     0,
		AffectedRows: 0,
		Resultset:    resSet,
	}

	return &result, nil

}

func (h *CustomHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	log.Printf("Client requested field list for table: %s", table)
	return nil, nil

}

func (h *CustomHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	log.Printf("Client prepared statement: %s\n\n", query)
	return 0, 0, nil, nil
}

func (h *CustomHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	log.Printf("Executing prepared statement: %s", query)
	return nil, nil
}

func (h *CustomHandler) HandleStmtClose(context interface{}) error {
	log.Println("Closing prepared statement")
	return nil
}

func (h *CustomHandler) HandleOtherCommand(cmd byte, data []byte) error {
	log.Printf("Client sent other command: %x", cmd)
	return nil
}

func RunProxyServer(host string, port string) {
	// Listen for connections on localhost port 4000

	masterDB, err := sql.Open("mysql", "root:rootpassword@tcp(localhost:3306)/")
	if err != nil {
		log.Fatalf("Failed to connect to master MySQL server: %v", err)
	}
	defer masterDB.Close()

	log.Printf("MySQL proxy server starting on %s:%s", host, port)
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		log.Println("Proxy server fatal error")
		log.Fatal(err)
	}
	defer l.Close()
	log.Printf("MySQL proxy server listening on %s:%s", host, port)

	// Accept new connections
	for {
		c, err := l.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}
		// log.Println("connection established")

		// Create a connection with the custom handler
		go func(conn net.Conn) {
			defer conn.Close()

			mysqlConn, err := server.NewConn(conn, "root", "rootpassword", &CustomHandler{db: masterDB})
			if err != nil {
				log.Println("Failed to create MySQL connection:", err)
				return
			}

			// Handle commands from the client
			for {
				// log.Println("Handling commands")
				if err := mysqlConn.HandleCommand(); err != nil {
					log.Printf("Connection error:%v\n", err)
					return
				}
				// log.Println("Handled commands")
			}
			log.Println("connection closed")
		}(c)
	}
}

func NewResultsetFromRows(rows *sql.Rows) (*mysql.Resultset, error) {

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	fieldTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	resultSet := &mysql.Resultset{
		Fields:     make([]*mysql.Field, len(columns)),
		FieldNames: make(map[string]int),
		Values:     [][]mysql.FieldValue{},
		RowDatas:   []mysql.RowData{},

		Streaming: mysql.StreamingNone,
	}

	for i, fieldType := range fieldTypes {
		resultSet.FieldNames[fieldType.Name()] = i

		resultSet.Fields[i] = &mysql.Field{
			Name:     []byte(fieldType.Name()),
			OrgName:  []byte(fieldType.Name()),
			Table:    nil,
			OrgTable: nil,
			Charset:  33,
			Type:     mapColumnType(fieldType.DatabaseTypeName()),
			Flag:     0,
		}
		// prec,scale,ok :=fieldType.DecimalSize()
		// if ok{
		// 	resultSet.Fields[i].
		// }
	}
	for rows.Next() {

		rawRow := make([]interface{}, len(columns))
		rawRowPointers := make([]interface{}, len(columns))
		for i := range rawRowPointers {
			rawRowPointers[i] = &rawRow[i]
		}
		if err := rows.Scan(rawRowPointers...); err != nil {
			return nil, err
		}
		rowValues := make([]mysql.FieldValue, len(columns))

		//create the values[][]*FieldValues

		for i, val := range rawRow {
			if val == nil {
				rowValues[i] = mysql.NewFieldValue(mysql.FieldValueTypeNull, 0, nil)
			} else {
				switch v := val.(type) {
				case int64:
					rowValues[i] = mysql.NewFieldValue(mysql.FieldValueTypeSigned, uint64(v), nil)
				case uint64:
					rowValues[i] = mysql.NewFieldValue(mysql.FieldValueTypeUnsigned, v, nil)
				case float64:
					rowValues[i] = mysql.NewFieldValue(mysql.FieldValueTypeFloat, 0, []byte(fmt.Sprintf("%f", v)))
				case string:
					rowValues[i] = mysql.NewFieldValue(mysql.FieldValueTypeString, 0, []byte(v))
				case []byte:
					rowValues[i] = mysql.NewFieldValue(mysql.FieldValueTypeString, 0, v)
				default:
					return nil, fmt.Errorf("unsupported value type: %T", v)
				}
			}
		}

		resultSet.Values = append(resultSet.Values, rowValues)

		var rowData mysql.RowData
		for _, val := range rawRow {
			switch v := val.(type) {
			case []byte:
				rowData = append(rowData, v...)
			}
		}
		resultSet.RowDatas = append(resultSet.RowDatas, rowData)

	}

	return resultSet, nil
}

func mapColumnType(sqlType string) uint8 {
	switch sqlType {
	case "VARCHAR", "TEXT":
		return 253 // MYSQL_TYPE_STRING
	case "INT":
		return 3 // MYSQL_TYPE_LONG
	case "BIGINT":
		return 8 // MYSQL_TYPE_LONGLONG
	case "FLOAT":
		return 4 // MYSQL_TYPE_FLOAT
	case "DOUBLE":
		return 5 // MYSQL_TYPE_DOUBLE
	case "BLOB":
		return 252 // MYSQL_TYPE_BLOB
	default:
		return 0 // Unknown type
	}
}
func toUint64(value interface{}) uint64 {
	switch v := value.(type) {
	case int64:
		return uint64(v)
	case float64:
		return uint64(v)
	default:
		return 0
	}
}
