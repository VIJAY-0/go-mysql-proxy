package main

import (
	"MYSQLPROXY/mysql_proxy"
	"fmt"
)

func main() {
	fmt.Println("hi..")
	mysql_proxy.RunProxyServer("localhost", "3305")
}
