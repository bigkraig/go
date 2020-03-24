// Code generated by private/model/cli/gen-api/main.go. DO NOT EDIT.

package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/internal/awsutil"
)

// Represents the input of a Scan operation.
type ScanInput struct {
	_ struct{} `type:"structure"`

	// This is a legacy parameter. Use ProjectionExpression instead. For more information,
	// see AttributesToGet (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.AttributesToGet.html)
	// in the Amazon DynamoDB Developer Guide.
	AttributesToGet []string `min:"1" type:"list"`

	// This is a legacy parameter. Use FilterExpression instead. For more information,
	// see ConditionalOperator (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.ConditionalOperator.html)
	// in the Amazon DynamoDB Developer Guide.
	ConditionalOperator ConditionalOperator `type:"string" enum:"true"`

	// A Boolean value that determines the read consistency model during the scan:
	//
	//    * If ConsistentRead is false, then the data returned from Scan might not
	//    contain the results from other recently completed write operations (PutItem,
	//    UpdateItem, or DeleteItem).
	//
	//    * If ConsistentRead is true, then all of the write operations that completed
	//    before the Scan began are guaranteed to be contained in the Scan response.
	//
	// The default setting for ConsistentRead is false.
	//
	// The ConsistentRead parameter is not supported on global secondary indexes.
	// If you scan a global secondary index with ConsistentRead set to true, you
	// will receive a ValidationException.
	ConsistentRead *bool `type:"boolean"`

	// The primary key of the first item that this operation will evaluate. Use
	// the value that was returned for LastEvaluatedKey in the previous operation.
	//
	// The data type for ExclusiveStartKey must be String, Number or Binary. No
	// set data types are allowed.
	//
	// In a parallel scan, a Scan request that includes ExclusiveStartKey must specify
	// the same segment whose previous Scan returned the corresponding value of
	// LastEvaluatedKey.
	ExclusiveStartKey map[string]AttributeValue `type:"map"`

	// One or more substitution tokens for attribute names in an expression. The
	// following are some use cases for using ExpressionAttributeNames:
	//
	//    * To access an attribute whose name conflicts with a DynamoDB reserved
	//    word.
	//
	//    * To create a placeholder for repeating occurrences of an attribute name
	//    in an expression.
	//
	//    * To prevent special characters in an attribute name from being misinterpreted
	//    in an expression.
	//
	// Use the # character in an expression to dereference an attribute name. For
	// example, consider the following attribute name:
	//
	//    * Percentile
	//
	// The name of this attribute conflicts with a reserved word, so it cannot be
	// used directly in an expression. (For the complete list of reserved words,
	// see Reserved Words (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html)
	// in the Amazon DynamoDB Developer Guide). To work around this, you could specify
	// the following for ExpressionAttributeNames:
	//
	//    * {"#P":"Percentile"}
	//
	// You could then use this substitution in an expression, as in this example:
	//
	//    * #P = :val
	//
	// Tokens that begin with the : character are expression attribute values, which
	// are placeholders for the actual value at runtime.
	//
	// For more information on expression attribute names, see Specifying Item Attributes
	// (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html)
	// in the Amazon DynamoDB Developer Guide.
	ExpressionAttributeNames map[string]string `type:"map"`

	// One or more values that can be substituted in an expression.
	//
	// Use the : (colon) character in an expression to dereference an attribute
	// value. For example, suppose that you wanted to check whether the value of
	// the ProductStatus attribute was one of the following:
	//
	// Available | Backordered | Discontinued
	//
	// You would first need to specify ExpressionAttributeValues as follows:
	//
	// { ":avail":{"S":"Available"}, ":back":{"S":"Backordered"}, ":disc":{"S":"Discontinued"}
	// }
	//
	// You could then use these values in an expression, such as this:
	//
	// ProductStatus IN (:avail, :back, :disc)
	//
	// For more information on expression attribute values, see Condition Expressions
	// (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html)
	// in the Amazon DynamoDB Developer Guide.
	ExpressionAttributeValues map[string]AttributeValue `type:"map"`

	// A string that contains conditions that DynamoDB applies after the Scan operation,
	// but before the data is returned to you. Items that do not satisfy the FilterExpression
	// criteria are not returned.
	//
	// A FilterExpression is applied after the items have already been read; the
	// process of filtering does not consume any additional read capacity units.
	//
	// For more information, see Filter Expressions (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#FilteringResults)
	// in the Amazon DynamoDB Developer Guide.
	FilterExpression *string `type:"string"`

	// The name of a secondary index to scan. This index can be any local secondary
	// index or global secondary index. Note that if you use the IndexName parameter,
	// you must also provide TableName.
	IndexName *string `min:"3" type:"string"`

	// The maximum number of items to evaluate (not necessarily the number of matching
	// items). If DynamoDB processes the number of items up to the limit while processing
	// the results, it stops the operation and returns the matching values up to
	// that point, and a key in LastEvaluatedKey to apply in a subsequent operation,
	// so that you can pick up where you left off. Also, if the processed dataset
	// size exceeds 1 MB before DynamoDB reaches this limit, it stops the operation
	// and returns the matching values up to the limit, and a key in LastEvaluatedKey
	// to apply in a subsequent operation to continue the operation. For more information,
	// see Working with Queries (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html)
	// in the Amazon DynamoDB Developer Guide.
	Limit *int64 `min:"1" type:"integer"`

	// A string that identifies one or more attributes to retrieve from the specified
	// table or index. These attributes can include scalars, sets, or elements of
	// a JSON document. The attributes in the expression must be separated by commas.
	//
	// If no attribute names are specified, then all attributes will be returned.
	// If any of the requested attributes are not found, they will not appear in
	// the result.
	//
	// For more information, see Specifying Item Attributes (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html)
	// in the Amazon DynamoDB Developer Guide.
	ProjectionExpression *string `type:"string"`

	// Determines the level of detail about provisioned throughput consumption that
	// is returned in the response:
	//
	//    * INDEXES - The response includes the aggregate ConsumedCapacity for the
	//    operation, together with ConsumedCapacity for each table and secondary
	//    index that was accessed. Note that some operations, such as GetItem and
	//    BatchGetItem, do not access any indexes at all. In these cases, specifying
	//    INDEXES will only return ConsumedCapacity information for table(s).
	//
	//    * TOTAL - The response includes only the aggregate ConsumedCapacity for
	//    the operation.
	//
	//    * NONE - No ConsumedCapacity details are included in the response.
	ReturnConsumedCapacity ReturnConsumedCapacity `type:"string" enum:"true"`

	// This is a legacy parameter. Use FilterExpression instead. For more information,
	// see ScanFilter (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.ScanFilter.html)
	// in the Amazon DynamoDB Developer Guide.
	ScanFilter map[string]Condition `type:"map"`

	// For a parallel Scan request, Segment identifies an individual segment to
	// be scanned by an application worker.
	//
	// Segment IDs are zero-based, so the first segment is always 0. For example,
	// if you want to use four application threads to scan a table or an index,
	// then the first thread specifies a Segment value of 0, the second thread specifies
	// 1, and so on.
	//
	// The value of LastEvaluatedKey returned from a parallel Scan request must
	// be used as ExclusiveStartKey with the same segment ID in a subsequent Scan
	// operation.
	//
	// The value for Segment must be greater than or equal to 0, and less than the
	// value provided for TotalSegments.
	//
	// If you provide Segment, you must also provide TotalSegments.
	Segment *int64 `type:"integer"`

	// The attributes to be returned in the result. You can retrieve all item attributes,
	// specific item attributes, the count of matching items, or in the case of
	// an index, some or all of the attributes projected into the index.
	//
	//    * ALL_ATTRIBUTES - Returns all of the item attributes from the specified
	//    table or index. If you query a local secondary index, then for each matching
	//    item in the index, DynamoDB fetches the entire item from the parent table.
	//    If the index is configured to project all item attributes, then all of
	//    the data can be obtained from the local secondary index, and no fetching
	//    is required.
	//
	//    * ALL_PROJECTED_ATTRIBUTES - Allowed only when querying an index. Retrieves
	//    all attributes that have been projected into the index. If the index is
	//    configured to project all attributes, this return value is equivalent
	//    to specifying ALL_ATTRIBUTES.
	//
	//    * COUNT - Returns the number of matching items, rather than the matching
	//    items themselves.
	//
	//    * SPECIFIC_ATTRIBUTES - Returns only the attributes listed in AttributesToGet.
	//    This return value is equivalent to specifying AttributesToGet without
	//    specifying any value for Select. If you query or scan a local secondary
	//    index and request only attributes that are projected into that index,
	//    the operation reads only the index and not the table. If any of the requested
	//    attributes are not projected into the local secondary index, DynamoDB
	//    fetches each of these attributes from the parent table. This extra fetching
	//    incurs additional throughput cost and latency. If you query or scan a
	//    global secondary index, you can only request attributes that are projected
	//    into the index. Global secondary index queries cannot fetch attributes
	//    from the parent table.
	//
	// If neither Select nor AttributesToGet are specified, DynamoDB defaults to
	// ALL_ATTRIBUTES when accessing a table, and ALL_PROJECTED_ATTRIBUTES when
	// accessing an index. You cannot use both Select and AttributesToGet together
	// in a single request, unless the value for Select is SPECIFIC_ATTRIBUTES.
	// (This usage is equivalent to specifying AttributesToGet without any value
	// for Select.)
	//
	// If you use the ProjectionExpression parameter, then the value for Select
	// can only be SPECIFIC_ATTRIBUTES. Any other value for Select will return an
	// error.
	Select Select `type:"string" enum:"true"`

	// The name of the table containing the requested items; or, if you provide
	// IndexName, the name of the table to which that index belongs.
	//
	// TableName is a required field
	TableName *string `min:"3" type:"string" required:"true"`

	// For a parallel Scan request, TotalSegments represents the total number of
	// segments into which the Scan operation will be divided. The value of TotalSegments
	// corresponds to the number of application workers that will perform the parallel
	// scan. For example, if you want to use four application threads to scan a
	// table or an index, specify a TotalSegments value of 4.
	//
	// The value for TotalSegments must be greater than or equal to 1, and less
	// than or equal to 1000000. If you specify a TotalSegments value of 1, the
	// Scan operation will be sequential rather than parallel.
	//
	// If you specify TotalSegments, you must also specify Segment.
	TotalSegments *int64 `min:"1" type:"integer"`
}

// String returns the string representation
func (s ScanInput) String() string {
	return awsutil.Prettify(s)
}

// Validate inspects the fields of the type to determine if they are valid.
func (s *ScanInput) Validate() error {
	invalidParams := aws.ErrInvalidParams{Context: "ScanInput"}
	if s.AttributesToGet != nil && len(s.AttributesToGet) < 1 {
		invalidParams.Add(aws.NewErrParamMinLen("AttributesToGet", 1))
	}
	if s.IndexName != nil && len(*s.IndexName) < 3 {
		invalidParams.Add(aws.NewErrParamMinLen("IndexName", 3))
	}
	if s.Limit != nil && *s.Limit < 1 {
		invalidParams.Add(aws.NewErrParamMinValue("Limit", 1))
	}

	if s.TableName == nil {
		invalidParams.Add(aws.NewErrParamRequired("TableName"))
	}
	if s.TableName != nil && len(*s.TableName) < 3 {
		invalidParams.Add(aws.NewErrParamMinLen("TableName", 3))
	}
	if s.TotalSegments != nil && *s.TotalSegments < 1 {
		invalidParams.Add(aws.NewErrParamMinValue("TotalSegments", 1))
	}
	if s.ScanFilter != nil {
		for i, v := range s.ScanFilter {
			if err := v.Validate(); err != nil {
				invalidParams.AddNested(fmt.Sprintf("%s[%v]", "ScanFilter", i), err.(aws.ErrInvalidParams))
			}
		}
	}

	if invalidParams.Len() > 0 {
		return invalidParams
	}
	return nil
}

// Represents the output of a Scan operation.
type ScanOutput struct {
	_ struct{} `type:"structure"`

	// The capacity units consumed by the Scan operation. The data returned includes
	// the total provisioned throughput consumed, along with statistics for the
	// table and any indexes involved in the operation. ConsumedCapacity is only
	// returned if the ReturnConsumedCapacity parameter was specified. For more
	// information, see Provisioned Throughput (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ProvisionedThroughputIntro.html)
	// in the Amazon DynamoDB Developer Guide.
	ConsumedCapacity *ConsumedCapacity `type:"structure"`

	// The number of items in the response.
	//
	// If you set ScanFilter in the request, then Count is the number of items returned
	// after the filter was applied, and ScannedCount is the number of matching
	// items before the filter was applied.
	//
	// If you did not use a filter in the request, then Count is the same as ScannedCount.
	Count *int64 `type:"integer"`

	// An array of item attributes that match the scan criteria. Each element in
	// this array consists of an attribute name and the value for that attribute.
	Items []map[string]AttributeValue `type:"list"`

	// The primary key of the item where the operation stopped, inclusive of the
	// previous result set. Use this value to start a new operation, excluding this
	// value in the new request.
	//
	// If LastEvaluatedKey is empty, then the "last page" of results has been processed
	// and there is no more data to be retrieved.
	//
	// If LastEvaluatedKey is not empty, it does not necessarily mean that there
	// is more data in the result set. The only way to know when you have reached
	// the end of the result set is when LastEvaluatedKey is empty.
	LastEvaluatedKey map[string]AttributeValue `type:"map"`

	// The number of items evaluated, before any ScanFilter is applied. A high ScannedCount
	// value with few, or no, Count results indicates an inefficient Scan operation.
	// For more information, see Count and ScannedCount (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Count)
	// in the Amazon DynamoDB Developer Guide.
	//
	// If you did not use a filter in the request, then ScannedCount is the same
	// as Count.
	ScannedCount *int64 `type:"integer"`
}

// String returns the string representation
func (s ScanOutput) String() string {
	return awsutil.Prettify(s)
}

const opScan = "Scan"

// ScanRequest returns a request value for making API operation for
// Amazon DynamoDB.
//
// The Scan operation returns one or more items and item attributes by accessing
// every item in a table or a secondary index. To have DynamoDB return fewer
// items, you can provide a FilterExpression operation.
//
// If the total number of scanned items exceeds the maximum dataset size limit
// of 1 MB, the scan stops and results are returned to the user as a LastEvaluatedKey
// value to continue the scan in a subsequent operation. The results also include
// the number of items exceeding the limit. A scan can result in no table data
// meeting the filter criteria.
//
// A single Scan operation reads up to the maximum number of items set (if using
// the Limit parameter) or a maximum of 1 MB of data and then apply any filtering
// to the results using FilterExpression. If LastEvaluatedKey is present in
// the response, you need to paginate the result set. For more information,
// see Paginating the Results (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination)
// in the Amazon DynamoDB Developer Guide.
//
// Scan operations proceed sequentially; however, for faster performance on
// a large table or secondary index, applications can request a parallel Scan
// operation by providing the Segment and TotalSegments parameters. For more
// information, see Parallel Scan (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan)
// in the Amazon DynamoDB Developer Guide.
//
// Scan uses eventually consistent reads when accessing the data in a table;
// therefore, the result set might not include the changes to data in the table
// immediately before the operation began. If you need a consistent copy of
// the data, as of the time that the Scan begins, you can set the ConsistentRead
// parameter to true.
//
//    // Example sending a request using ScanRequest.
//    req := client.ScanRequest(params)
//    resp, err := req.Send(context.TODO())
//    if err == nil {
//        fmt.Println(resp)
//    }
//
// Please also see https://docs.aws.amazon.com/goto/WebAPI/dynamodb-2012-08-10/Scan
func (c *Client) ScanRequest(input *ScanInput) ScanRequest {
	op := &aws.Operation{
		Name:       opScan,
		HTTPMethod: "POST",
		HTTPPath:   "/",
		Paginator: &aws.Paginator{
			InputTokens:     []string{"ExclusiveStartKey"},
			OutputTokens:    []string{"LastEvaluatedKey"},
			LimitToken:      "Limit",
			TruncationToken: "",
		},
	}

	if input == nil {
		input = &ScanInput{}
	}

	req := c.newRequest(op, input, &ScanOutput{})

	if req.Config.EnableEndpointDiscovery {
		de := discovererDescribeEndpoints{
			Client:        c,
			Required:      false,
			EndpointCache: c.endpointCache,
			Params: map[string]*string{
				"op": &req.Operation.Name,
			},
		}

		for k, v := range de.Params {
			if v == nil {
				delete(de.Params, k)
			}
		}

		req.Handlers.Build.PushFrontNamed(aws.NamedHandler{
			Name: "crr.endpointdiscovery",
			Fn:   de.Handler,
		})
	}
	return ScanRequest{Request: req, Input: input, Copy: c.ScanRequest}
}

// ScanRequest is the request type for the
// Scan API operation.
type ScanRequest struct {
	*aws.Request
	Input *ScanInput
	Copy  func(*ScanInput) ScanRequest
}

// Send marshals and sends the Scan API request.
func (r ScanRequest) Send(ctx context.Context) (*ScanResponse, error) {
	r.Request.SetContext(ctx)
	err := r.Request.Send()
	if err != nil {
		return nil, err
	}

	resp := &ScanResponse{
		ScanOutput: r.Request.Data.(*ScanOutput),
		response:   &aws.Response{Request: r.Request},
	}

	return resp, nil
}

// NewScanRequestPaginator returns a paginator for Scan.
// Use Next method to get the next page, and CurrentPage to get the current
// response page from the paginator. Next will return false, if there are
// no more pages, or an error was encountered.
//
// Note: This operation can generate multiple requests to a service.
//
//   // Example iterating over pages.
//   req := client.ScanRequest(input)
//   p := dynamodb.NewScanRequestPaginator(req)
//
//   for p.Next(context.TODO()) {
//       page := p.CurrentPage()
//   }
//
//   if err := p.Err(); err != nil {
//       return err
//   }
//
func NewScanPaginator(req ScanRequest) ScanPaginator {
	return ScanPaginator{
		Pager: aws.Pager{
			NewRequest: func(ctx context.Context) (*aws.Request, error) {
				var inCpy *ScanInput
				if req.Input != nil {
					tmp := *req.Input
					inCpy = &tmp
				}

				newReq := req.Copy(inCpy)
				newReq.SetContext(ctx)
				return newReq.Request, nil
			},
		},
	}
}

// ScanPaginator is used to paginate the request. This can be done by
// calling Next and CurrentPage.
type ScanPaginator struct {
	aws.Pager
}

func (p *ScanPaginator) CurrentPage() *ScanOutput {
	return p.Pager.CurrentPage().(*ScanOutput)
}

// ScanResponse is the response type for the
// Scan API operation.
type ScanResponse struct {
	*ScanOutput

	response *aws.Response
}

// SDKResponseMetdata returns the response metadata for the
// Scan request.
func (r *ScanResponse) SDKResponseMetdata() *aws.Response {
	return r.response
}
