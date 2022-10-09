package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/iammujtaba/elasticsearch-go/utility"
)

type ElasticSearch struct {
	file_path string
	username  string
	password  string
	host      string
	client    *elasticsearch.Client
	index     string
	alias     string
}

func NewElasticSearch(file_path string, username string, password string, host string) (self *ElasticSearch) {
	self = new(ElasticSearch)
	self.file_path = file_path
	self.username = username
	self.password = password
	self.host = host
	return
}

func (self *ElasticSearch) Connect() {
	cert, _ := ioutil.ReadFile(self.file_path)

	cfg := elasticsearch.Config{
		Addresses: []string{
			self.host,
		},
		Username: self.username,
		Password: self.password,
		CACert:   cert,
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	self.client = es
}

func (self *ElasticSearch) CreateIndex(index string) error {
	self.index = index
	self.alias = index + "_alias"

	res, err := self.client.Indices.Exists([]string{self.index})
	if err != nil {
		return fmt.Errorf("cannot check index existence: %w", err)
	}
	if res.StatusCode == 200 {
		fmt.Println("Index already exists returning.")
		return nil
	}
	if res.StatusCode != 404 {
		return fmt.Errorf("error in index existence response: %s", res.String())
	}

	res, err = self.client.Indices.Create(self.index)
	if err != nil {
		return fmt.Errorf("cannot create index: %w", err)
	}
	if res.IsError() {
		return fmt.Errorf("error in index creation response: %s", res.String())
	}

	res, err = self.client.Indices.PutAlias([]string{self.index}, self.alias)
	if err != nil {
		return fmt.Errorf("cannot create index alias: %w", err)
	}
	if res.IsError() {
		return fmt.Errorf("error in index alias creation response: %s", res.String())
	}

	return nil
}

func (self *ElasticSearch) InsertOne(index, document_id string, data io.Reader) error {

	res, err := self.client.Create(index, document_id, data)
	if err != nil {
		fmt.Println("Data creation failed", err)
		return err
	}
	if res.StatusCode != 201 {
		return fmt.Errorf("Data not inserted, status:%d ", res.StatusCode)
	}
	fmt.Println("Data creation Success", res.StatusCode)
	return nil

}

func (self *ElasticSearch) FindById(index string, document_id string) error {

	res, err := self.client.Get(index, document_id)
	if err != nil {
		fmt.Println("Data creation failed", err)
		return err
	}
	if res.StatusCode != 200 {
		fmt.Println("Data not found, status", res.StatusCode)
	}

	var mapResp map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&mapResp); err == nil {
		fmt.Println(mapResp)
	}
	return nil
}

func (self *ElasticSearch) MatchSearchQueryBuilder(search_field, value string, size int) *strings.Reader {
	var query = fmt.Sprintf(`
	"match": {
		"%s": {
		  "query": "%s"
		}
	  }`, search_field, value)

	read := utility.ConstructQuery(query, size)

	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(read); err != nil {
		log.Fatalf("json.NewEncoder() ERROR:", err)
	}
	return read
}

func (self *ElasticSearch) MultiMatchSearchQueryBuilder(value string, size int) *strings.Reader {
	var query = fmt.Sprintf(`
	"multi_match": {
		  "query": "%s",
		  "fields": ["name^2", "email^3", "username^3", "phone_number"]
		}`, value) // fields are variable can be change according to document.

	read := utility.ConstructQuery(query, size)

	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(read); err != nil {
		log.Fatalf("json.NewEncoder() ERROR:", err)
	}
	return read
}

func (self *ElasticSearch) Search(index string, query *strings.Reader) error {
	ctx := context.Background()

	var mapResp map[string]interface{}

	res, err := self.client.Search(
		self.client.Search.WithContext(ctx),
		self.client.Search.WithIndex(index),
		self.client.Search.WithBody(query),
		self.client.Search.WithTrackTotalHits(true),
	)

	if err != nil {
		log.Fatalf("Elasticsearch Search() API ERROR:", err)
		return err
	}

	defer res.Body.Close()

	if err := json.NewDecoder(res.Body).Decode(&mapResp); err == nil {
		fmt.Println(mapResp)
	}

	return nil
}
