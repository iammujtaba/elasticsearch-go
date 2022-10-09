package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/iammujtaba/elasticsearch-go/elasticsearch"
)

func getInsertData() *bytes.Reader {
	document := struct {
		Id    int    `json:"id"`
		Name  string `json:"name"`
		Price int    `json:"price"`
	}{
		Id:    4,
		Name:  "Chalis Tees Bar Foo Bar Tees Chalis Lakh Lega",
		Price: 40,
	}
	data, err := json.Marshal(document)
	if err != nil {
		log.Fatal(err)
	}
	reader := bytes.NewReader(data)
	return reader
}

func main() {
	index_name := "stdout"
	es := elasticsearch.NewElasticSearch("/Users/programmer/sshconnect/http_ca.crt", "elastic", "=cOIlXEiNk6jBwjEfkPt", "https://localhost:9200")
	es.Connect()
	es.CreateIndex(index_name)
	input := getInsertData()

	err := es.InsertOne(index_name, "4", input) // Document should be unique id.
	if err != nil {
		fmt.Println(err.Error())
	}
	es.FindById(index_name, "1")

	search_field := "name"
	value := "f"

	query_1 := es.MatchSearchQueryBuilder(search_field, value, 10) // size needs to be defined.
	es.Search(index_name, query_1)

	fmt.Println("Query-2 using MULTI-MATCH")

	query_2 := es.MultiMatchSearchQueryBuilder("*foo*", 10)
	es.Search(index_name, query_2)

}
