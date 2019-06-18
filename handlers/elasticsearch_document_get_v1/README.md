== Elasticsearch Document Get
Retrieves a document from Elasticsearch
[API Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html)  

=== Parameters
[Index]
  Required. Newline Delimited JSON to specify creating, updating, or deleting documents for indices.  
  [API Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)  
  
[Type]
  Required. The type of the document.

[Id]
  Required. The ID of the document.
  
[Include source fields]
  Comma delimited list of source fields to return.  
  Leave this parameter and the Exclude source fields parameter blank to return all source fields.

[Exclude source fields]
  Comma delimited list of source fields to exclude from being returned.  
  Leave this parameter and the Include source fields parameter blank to return all source fields.

  
=== Results
Elasticsearch document

=== Detailed Description
This handler returns an Elasticsearch document.