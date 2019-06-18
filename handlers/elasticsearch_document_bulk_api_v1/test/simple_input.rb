{
  'info' => {
    'api_server' => 'http://localhost:9200/',
    'api_username' => '',
    'api_password' => ''
  },
  'parameters' => {
    'error_handling'   => 'Error Message',
    'return_response'  => 'yes',
    'payload_ndjson'   => %q({ "index" : { "_index" : "test", "_type" : "type1", "_id" : "1" } }
{ "field1" : "value1" }
{ "delete" : { "_index" : "test", "_type" : "type1", "_id" : "2" } }
{ "create" : { "_index" : "test", "_type" : "type1", "_id" : "3" } }
{ "field1" : "value3" }
{ "update" : {"_id" : "1", "_type" : "type1", "_index" : "test"} }
{ "doc" : {"field2" : "value2"} }
)
  }
}
