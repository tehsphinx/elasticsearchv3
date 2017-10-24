package elasticsearchv3

import (
	"errors"
	"io/ioutil"
	"log"
	"os"

	el "gopkg.in/olivere/elastic.v3"
)

// Elastic interface handles ElasticSearch connections. Manages connection internally.
type Elastic struct {
	index    string
	docType  string
	mapping  string
	bulk     *el.BulkService
	bulkSize int
}

var client *el.Client
var url = "http://127.0.0.1:9200"

// New creates a new Elastic client. All elastic clients use the same connection.
func New(elasticURL string, index string, docType string, mapping string) (*Elastic, error) {
	if elasticURL != "" {
		url = elasticURL
	}
	el := &Elastic{
		index:   index,
		docType: docType,
		mapping: mapping,
	}
	err := el.checkClient(false)
	return el, err
}

// SetIndex ...
func (es *Elastic) SetIndex(index string) {
	es.index = index
}

// StartBulk ...
func (es *Elastic) StartBulk(size int) {
	es.bulk = client.Bulk()
	es.bulkSize = size
}

// StopBulk ...
func (es *Elastic) StopBulk() error {
	_, err := es.bulk.Do()
	es.bulk = nil
	return err
}

// Index creates a document in elasticsearch
func (es *Elastic) Index(doc interface{}, id string) (string, error) {
	if es.bulk != nil {
		return "", es.bulkIndex(doc, id)
	}

	q := client.Index().Index(es.index).Type(es.docType).BodyJson(doc)
	if id != "" {
		q = q.Id(id)
	}

	res, err := q.Do()
	if err != nil {
		return "", err
	}
	return res.Id, nil
}

func (es *Elastic) bulkIndex(doc interface{}, id string) error {
	q := el.NewBulkIndexRequest().Index(es.index).Type(es.docType).Doc(doc)
	if id != "" {
		q = q.Id(id)
	}
	es.bulk.Add(q)

	if es.bulk.NumberOfActions() >= es.bulkSize {
		if _, err := es.bulk.Do(); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a document from elasticsearch by id
func (es *Elastic) Get(id string) (*el.GetResult, error) {
	res, err := client.Get().Index(es.index).Type(es.docType).Id(id).Do()
	return res, err
}

// Delete removes one document from elasticsearch by id
func (es *Elastic) Delete(id string) (bool, error) {
	res, err := client.Delete().Index(es.index).Type(es.docType).Id(id).Do()
	return res.Found, err
}

// Search takes a json search string and executes it, returning the result
func (es *Elastic) Search(json interface{}) (*el.SearchResult, error) {
	return client.Search(es.index).Source(json).Pretty(true).Do()
}

// IndexExists checks if index exists
func (es *Elastic) IndexExists(index string) (bool, error) {
	return client.IndexExists(index).Do()
}

// CreateIndex creates a index by name. The index specified in the struct is created anyway if it doesnt exist.
func (es *Elastic) CreateIndex(index string) error {
	q := client.CreateIndex(index)
	if es.mapping != "" {
		q.BodyString(es.mapping)
	}
	createIndex, err := q.Do()
	if err == nil && !createIndex.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge new index")
	}
	return err
}

// DeleteIndex deletes the index specified in the struct.
func (es *Elastic) DeleteIndex(index string) error {
	deleteIndex, err := client.DeleteIndex(index).Do()
	if err == nil && !deleteIndex.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge deletion of index")
	}
	return err
}

func (es *Elastic) createIndex() error {
	err := es.CreateIndex(es.index)
	return err
}

// PutIndexTemplate ...
func (es *Elastic) PutIndexTemplate(name string, body string) error {
	res, err := client.IndexPutTemplate(name).BodyString(body).Do()
	if err == nil && !res.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge creation of template")
	}
	return err
}

// DeleteIndexTemplate ...
func (es *Elastic) DeleteIndexTemplate(name string) error {
	res, err := client.IndexDeleteTemplate(name).Do()
	if err == nil && !res.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge deletion of tempate")
	}
	return err
}

func (es *Elastic) checkClient(checkIndex bool) error {
	var err error
	if client == nil {
		err = es.newClient()
		if err != nil {
			log.Println(err)
		} else if checkIndex {
			es.checkOwnIndex()
		}
	}
	return err
}

func (es *Elastic) checkOwnIndex() error {
	exists, err := client.IndexExists(es.index).Do()
	if err == nil && !exists {
		err = es.createIndex()
	}
	if err != nil {
		log.Println(err)
	}
	return err
}

func (es *Elastic) newClient() error {
	log.Printf("Opening new Elastic connection to %s", url)
	cl, err := el.NewSimpleClient(el.SetURL(url),
		el.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		el.SetInfoLog(log.New(ioutil.Discard, "", log.LstdFlags)),
		el.SetBasicAuth("elastic", "changeme"))
	if err == nil {
		client = cl
	}
	return err
}
