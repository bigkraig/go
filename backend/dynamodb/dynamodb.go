package dynamodb

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"

	"github.com/kellegous/go/internal"
)

type Document struct {
	Key   string `dynamodbav:"key"`
	Route *internal.Route
}

// NextID is the next numeric ID to use for auto-generated IDs
type NextID struct {
	ID uint32 `json:"id" dynamodbav:"id"`
}

// Backend provides access to Google Firestore.
type Backend struct {
	db    *dynamodb.Client
	table *string
}

// New instantiates a new Backend
func New(ctx context.Context, table string) (*Backend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return nil, err
	}

	client := dynamodb.New(cfg)

	backend := Backend{
		db:    client,
		table: aws.String(table),
	}

	return &backend, nil
}

// Close the resources associated with this backend.
func (backend *Backend) Close() error {
	return nil
}

// Get retreives a shortcut from the data store.
func (backend *Backend) Get(ctx context.Context, name string) (*internal.Route, error) {
	input := &dynamodb.GetItemInput{
		TableName: backend.table,
		Key: map[string]dynamodb.AttributeValue{
			"key": {S: aws.String("routes/" + name),},
		},
	}

	req := backend.db.GetItemRequest(input)
	result, err := req.Send(ctx)
	if err != nil {
		return nil, err
	}

	doc := Document{}
	err = dynamodbattribute.UnmarshalMap(result.Item, &doc)
	if err != nil {
		return nil, err
	}
	if doc.Route == nil {
		return nil, internal.ErrRouteNotFound
	}

	return doc.Route, nil
}

// Put stores a new shortcut in the data store.
func (backend *Backend) Put(ctx context.Context, key string, rt *internal.Route) error {
	doc := &Document{
		Key:   "routes/" + key,
		Route: rt,
	}

	av, err := dynamodbattribute.MarshalMap(doc)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: backend.table,
	}

	req := backend.db.PutItemRequest(input)
	_, err = req.Send(ctx)
	return err
}

// Del removes an existing shortcut from the data store.
func (backend *Backend) Del(ctx context.Context, key string) error {
	input := &dynamodb.DeleteItemInput{
		TableName: backend.table,
		Key: map[string]dynamodb.AttributeValue{
			"route": {S: aws.String("routes/" + key),},
		},
	}

	req := backend.db.DeleteItemRequest(input)
	_, err := req.Send(ctx)
	return err
}

// List all routes in an iterator, starting with the key prefix of start (which can also be nil).
func (backend *Backend) List(ctx context.Context, start string) (internal.RouteIterator, error) {
	// TODO
	return nil, fmt.Errorf("not implemented")
	//params := &dynamodb.ScanInput{
	//	TableName: backend.table,
	//}
	//
	//if start != "" {
	//	params.ExclusiveStartKey = map[string]dynamodb.AttributeValue{
	//		"route": {S: aws.String("routes/" + start),},
	//	}
	//}
	//
	//scanReq := backend.db.ScanRequest(params)
	//scanResponse, err := scanReq.Send(ctx)
	//if err != nil {
	//	return nil, err
	//}
	//
	//return &RouteIterator{
	//	ctx: ctx,
	//	db:  backend.db,
	//	it:  scanResponse,
	//}, nil
}

// GetAll gets everything in the db to dump it out for backup purposes
func (backend *Backend) GetAll(ctx context.Context) (map[string]internal.Route, error) {
	golinks := map[string]internal.Route{}

	params := &dynamodb.ScanInput{
		TableName: backend.table,
		//FilterExpression: aws.String("begins_with(key, \"routes/\""),
	}

	scanReq := backend.db.ScanRequest(params)
	result, err := scanReq.Send(ctx)
	if err != nil {
		return nil, err
	}

	docs := []Document{}

	// Unmarshal the Items field in the result value to the Item Go type.
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &docs)
	if err != nil {
		return nil, err
	}

	for _, doc := range docs {
		if !strings.HasPrefix(doc.Key, "routes/") {
			continue
		}
		golinks[strings.TrimPrefix(doc.Key, "routes/")] = *doc.Route
	}

	return golinks, nil
}

// NextID generates the next numeric ID to be used for an auto-named shortcut.
// TODO: contains race condition
func (backend *Backend) NextID(ctx context.Context) (uint64, error) {
	_, err := backend.db.UpdateItemRequest(&dynamodb.UpdateItemInput{
		TableName: backend.table,
		ExpressionAttributeValues: map[string]dynamodb.AttributeValue{
			":inc": {N: aws.String("1")},
		},
		UpdateExpression: aws.String("ADD nextID :inc"),
		Key: map[string]dynamodb.AttributeValue{
			"key": {S: aws.String("IDs/nextID"),},
		},
	}).Send(ctx)
	if err != nil {
		return 0, err
	}

	result, err := backend.db.GetItemRequest(&dynamodb.GetItemInput{
		TableName: backend.table,
		Key: map[string]dynamodb.AttributeValue{
			"key": {S: aws.String("IDs/nextID"),},
		},
	}).Send(ctx)
	if err != nil {
		return 0, err
	}

	item := result.Item["nextID"]
	if item.N == nil {
		return 0, fmt.Errorf("no next id returned")
	}

	i, err := strconv.ParseUint(*item.N, 10, 64)
	if err != nil {
		return 0, err
	}

	return i, err
}
