/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kineticdata.bridgehub.adapter.elasticsearch;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;


/**
 *
 * @author austin.peters
 */
public class ElasticsearchAdapterTest {
    
    private final String elasticUrl = "http://localhost:9200";
    
    @Test
    public void test_escapedSearchUrl() throws Exception {
        
        //"http://localhost:9200/filebeat-*/_search?q=message%3A%22This%5C+is%5C+an%5C+error.%22+AND+_timestamp%3A%3E2021%5C-01%5C-01&size=1000&from=0";
        StringBuilder expectedUrl = new StringBuilder();
        String actualUrl = null;
        String logLevel = "This is an error.";
        String date = "2021-01-01";
        String pageSize = "1000";
        String offset = "0";
        String structure = "filebeat-*";
        String queryMethod = "search";
        String query = "message:\"<%= parameter[\"log level\"] %>\" AND _timestamp:><%= parameter[\"date\"] %>";
        
        BridgeRequest request = new BridgeRequest();
        ElasticsearchQualificationParser parser = new ElasticsearchQualificationParser();
        
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Elastic URL",elasticUrl);
        
        request.setStructure(structure);
        request.setQuery(query);
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("log level", logLevel);
        bridgeParameters.put("date", date);
        request.setParameters(bridgeParameters);
        
        ElasticsearchAdapter adapter = new ElasticsearchAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", pageSize);
        bridgeMetadata.put("offset", offset);
        request.setMetadata(bridgeMetadata);
        
        expectedUrl.append(elasticUrl)
            .append("/")
            .append(structure)
            .append("/")
            .append("_")
            .append(queryMethod)
            .append("?q=message%3A%22This%5C+is%5C+an%5C+error.%22+AND+_timestamp%3A%3E2021%5C-01%5C-01")
            .append("&size=")
            .append(pageSize)
            .append("&from=")
            .append(offset);

        try {
            actualUrl = adapter.buildUrl("search", ElasticsearchAdapter.JSON_ROOT_DEFAULT, request, parser);
        } catch (BridgeError e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        
        assertEquals(expectedUrl.toString(), actualUrl);
        
        try {
            bridgeMetadata.put("order", "<%=field[\"_timestamp\"]%>:DESC,<%=field[\"_source.message\"]%>:ASC");
            request.setMetadata(bridgeMetadata);
            actualUrl = adapter.buildUrl("search", ElasticsearchAdapter.JSON_ROOT_DEFAULT, request, parser);
        } catch (BridgeError e) {
            throw new RuntimeException(e);
        }
        
        expectedUrl.append("&sort=_timestamp%3Adesc%2Cmessage%3Aasc");
        assertEquals(expectedUrl.toString(), actualUrl);
        
    }
    
    @Test
    public void testCountResults_UriSearch() throws Exception {
        Integer expectedCount = 1;
        String expectedUrl = elasticUrl + "/examples/doc/_count?q=message%3Aerror";
        Count actualCount;
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Elastic URL",elasticUrl);
        
        ElasticsearchAdapter adapter = new ElasticsearchAdapter();
        ElasticsearchQualificationParser parser = new ElasticsearchQualificationParser();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("log level", "error");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);        
        request.setStructure("examples/doc");
        request.setQuery("message:<%= parameter[\"log level\"] %>");
        
        assertEquals(expectedUrl, adapter.buildUrl("count", ElasticsearchAdapter.JSON_ROOT_DEFAULT, request, parser));
        
        try {
            actualCount = adapter.count(request);
        } catch (BridgeError e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        
        assertEquals(expectedCount, actualCount.getValue());
    }
    
    @Test
    public void testCountResults_RequestBodySearch() throws Exception {
        Integer expectedCount = 1;
        String expectedUrl = elasticUrl + "/examples/doc/_count";
        Count actualCount;
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Elastic URL",elasticUrl);
        
        ElasticsearchAdapter adapter = new ElasticsearchAdapter();
        ElasticsearchQualificationParser parser = new ElasticsearchQualificationParser();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("elastic json query", "{\"query\":{\"match\":{\"message\": \"error\"}}}");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);        
        request.setStructure("examples/doc");
        request.setQuery("{\"type\": \"Elasticsearch DSL\", \"query\": \"<%= parameter[\"elastic json query\"] %>\"}");
        
        assertEquals(expectedUrl, adapter.buildUrl("count", ElasticsearchAdapter.JSON_ROOT_DEFAULT, request, parser));
        
        try {
            actualCount = adapter.count(request);
        } catch (BridgeError e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        
        assertEquals(expectedCount, actualCount.getValue());
    }
    
    @Test
    public void testRetrieveResults() throws Exception {
        String expectedUrl = elasticUrl + "/examples/doc/_search?q=message%3Aerror&size=1000&from=0&_source=app.username%2Capp.name";
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Elastic URL", elasticUrl);
        
        ElasticsearchAdapter adapter = new ElasticsearchAdapter();
        ElasticsearchQualificationParser parser = new ElasticsearchQualificationParser();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("log level", "error");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);        
        request.setStructure("examples/doc");
        request.setQuery("message:<%= parameter[\"log level\"] %>");
        request.setFields(
            Arrays.asList(
                "_source.app.username",
                "_source.app.name"
            )
        );
        
        assertEquals(expectedUrl, adapter.buildUrl("search", ElasticsearchAdapter.JSON_ROOT_DEFAULT, request, parser));
        
        Record bridgeRecord = adapter.retrieve(request);
        
    }

    @Test
    public void testRetrieveResults_RequestBodySearch() throws Exception {
        String expectedUrl = elasticUrl + "/examples/doc/_search?size=1000&from=0&_source=message";
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Elastic URL", elasticUrl);
        
        ElasticsearchAdapter adapter = new ElasticsearchAdapter();
        ElasticsearchQualificationParser parser = new ElasticsearchQualificationParser();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("log level", "error");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);        
        request.setStructure("examples/doc");
        request.setQuery("{\"type\": \"Elasticsearch DSL\", \"query\": \"{\\\"query\\\":{\\\"term\\\":{\\\"message\\\":\\\"<%= parameter[\"log level\"] %>\\\"}}}\"}");
        request.setFields(
            Arrays.asList(
                "_source.message"
            )
        );
        
        assertEquals(expectedUrl, adapter.buildUrl("search", ElasticsearchAdapter.JSON_ROOT_DEFAULT, request, parser));
        Record bridgeRecord = adapter.retrieve(request);
        
        Map<String, Object> expectedValues = new HashMap();
        expectedValues.put("_source.message", "this is an error message.");
        
        assertEquals(expectedValues, bridgeRecord.getRecord());
                
        
    }    
        
    @Test
    public void testJsonQualificationParsing() throws Exception {
        
        ElasticsearchQualificationParser parser = new ElasticsearchQualificationParser();
        String originalQuery = "{\"type\": \"Elasticsearch DSL\", \"query\": \"{\\\"size\\\":0,\\\"query\\\":{\\\"bool\\\":{\\\"must\\\":[{\\\"term\\\":{\\\"field 1\\\":\\\"<%= parameter[\"reserved lucene characters test\"] %>\\\"}},{\\\"term\\\":{\\\"field 2\\\":\\\"<%= parameter[\"json characters test\"] %>\\\"}},{\\\"term\\\":{\\\"field 3\\\":\\\"<%= parameter[\"space slug\"] %>\\\"}},{\\\"range\\\":{\\\"timestamp\\\":{\\\"gte\\\":\\\"now-<%= parameter[\"number of previous days\"] %>d\\\",\\\"lte\\\":\\\"now\\\"}}}],}}}\"}";
        String expectedParsedQuery = "{\"size\":0,\"query\":{\"bool\":{\"must\":[{\"term\":{\"field 1\":\"AND - OR *+\"}},{\"term\":{\"field 2\":\"\\\" \\\\r\\\\n \\\\ \"}},{\"term\":{\"field 3\":\"kinetic-data-slug\"}},{\"range\":{\"timestamp\":{\"gte\":\"now-14d\",\"lte\":\"now\"}}}],}}}";
        
        Map<String, String> parameters = new HashMap();
        parameters.put("space slug", "kinetic-data-slug");
        parameters.put("reserved lucene characters test", "AND - OR *+");
        parameters.put("json characters test", "\" \\r\\n \\ ");
        parameters.put("number of previous days", "14");
        
        String actualParsedQuery = parser.parse(originalQuery, parameters);
        
        assertEquals(expectedParsedQuery, actualParsedQuery);
        
    }
    
    @Test
    public void testUriQualificationParsing() throws Exception {
        
        ElasticsearchQualificationParser parser = new ElasticsearchQualificationParser();
        String originalQuery = "message:<%= parameter[\"reserved lucene characters test\"] %> AND field1:<%= parameter[\"json characters test\"] %>";
        String expectedParsedQuery = "message:\\\\AND\\ \\-\\ \\\\OR\\ \\*\\+ AND field1:\\\"\\ \\\\r\\\\n\\ \\\\";
        
        
        Map<String, String> parameters = new HashMap();
        parameters.put("reserved lucene characters test", "AND - OR *+");
        parameters.put("json characters test", "\" \\r\\n \\");
        
        String actualParsedQuery = parser.parse(originalQuery, parameters);
        
        assertEquals(expectedParsedQuery, actualParsedQuery);
        
    }
    
}
