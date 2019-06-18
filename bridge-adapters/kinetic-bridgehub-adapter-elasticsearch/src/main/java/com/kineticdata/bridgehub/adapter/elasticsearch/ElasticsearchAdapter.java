package com.kineticdata.bridgehub.adapter.elasticsearch;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.DocumentContext;
import com.kineticdata.bridgehub.helpers.http.HttpGetWithEntity;
import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.LoggerFactory;
import com.jayway.jsonpath.JsonPath;

public class ElasticsearchAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/

    /** Defines the adapter display name */
    public static final String NAME = "Elasticsearch Bridge";
    public static final String JSON_ROOT_DEFAULT = "$.hits.hits";

    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(ElasticsearchAdapter.class);

    /** Adapter version constant. */
    public static String VERSION;
    /** Load the properties version from the version.properties file. */
    static {
        try {
            java.util.Properties properties = new java.util.Properties();
            properties.load(ElasticsearchAdapter.class.getResourceAsStream("/"+ElasticsearchAdapter.class.getName()+".version"));
            VERSION = properties.getProperty("version");
        } catch (IOException e) {
            logger.warn("Unable to load "+ElasticsearchAdapter.class.getName()+" version properties.", e);
            VERSION = "Unknown";
        }
    }

    private String username;
    private String password;
    private String apiEndpoint;

    /** Defines the collection of property names for the adapter */
    public static class Properties {
        public static final String USERNAME = "Username";
        public static final String PASSWORD = "Password";
        public static final String API_URL = "Elastic URL";
    }

    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(Properties.USERNAME),
        new ConfigurableProperty(Properties.PASSWORD).setIsSensitive(true),
        new ConfigurableProperty(Properties.API_URL)
    );


    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public void initialize() throws BridgeError {
        this.username = properties.getValue(Properties.USERNAME);
        this.password = properties.getValue(Properties.PASSWORD);
        // Remove any trailing forward slash.
        this.apiEndpoint = properties.getValue(Properties.API_URL).replaceFirst("(\\/)$", "");
        testAuthenticationValues(this.apiEndpoint, this.username, this.password);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getVersion() {
        return VERSION;
    }

    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }

    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }

    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public Count count(BridgeRequest request) throws BridgeError {

        ElasticsearchQualificationParser elasticParser = new ElasticsearchQualificationParser();
        String jsonResponse = elasticQuery("count", null, request, elasticParser);
        Long count = JsonPath.parse(jsonResponse).read("$.count", Long.class);

        // Create and return a Count object.
        return new Count(count);
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {

        ElasticsearchQualificationParser elasticParser = new ElasticsearchQualificationParser();
        String metadataRoot = elasticParser.getJsonRootPath(request.getQuery());
        String jsonRootPath = JSON_ROOT_DEFAULT;
        if (StringUtils.isNotBlank(metadataRoot)) {
            jsonRootPath = metadataRoot;
        }
       
        String jsonResponse = elasticQuery("search", jsonRootPath, request, elasticParser);

        DocumentContext jsonDocument = JsonPath.parse(jsonResponse);
        Object objectRoot = jsonDocument.read(jsonRootPath);
        Record recordResult = new Record(null);

        if (objectRoot instanceof List) {
            List<Object> listRoot = (List)objectRoot;
            if (listRoot.size() == 1) {
                Map<String, Object> recordValues = new HashMap();
                for (String field : request.getFields()) {
                    try {
                        recordValues.put(field, JsonPath.parse(listRoot.get(0)).read(field));
                    } catch (InvalidPathException e) {
                        recordValues.put(field, null);
                    }
                }
                recordResult = new Record(recordValues);
            } else {
                throw new BridgeError("Multiple results matched an expected single match query");
            }
        } else if (objectRoot instanceof Map) {
            Map<String, Object> recordValues = new HashMap();
            for (String field : request.getFields()) {
                try {
                    recordValues.put(field, JsonPath.parse(objectRoot).read(field));
                } catch (InvalidPathException e) {
                    recordValues.put(field, null);
                }
            }
            recordResult = new Record(recordValues);
        }

        return recordResult;

    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {

        ElasticsearchQualificationParser elasticParser = new ElasticsearchQualificationParser();
        String metadataRoot = elasticParser.getJsonRootPath(request.getQuery());
        String jsonRootPath = JSON_ROOT_DEFAULT;
        if (StringUtils.isNotBlank(metadataRoot)) {
            jsonRootPath = metadataRoot;
        }
        
        String jsonResponse = elasticQuery("search", jsonRootPath, request, elasticParser);

        List<Record> recordList = new ArrayList<Record>();
        DocumentContext jsonDocument = JsonPath.parse(jsonResponse);
        Object objectRoot = jsonDocument.read(jsonRootPath);
        Map<String,String> metadata = new LinkedHashMap<String,String>();
        metadata.put("count",JsonPath.parse(jsonResponse).read("$.hits.total", String.class));

        if (objectRoot instanceof List) {
            List<Object> listRoot = (List)objectRoot;
            metadata.put("size", String.valueOf(listRoot.size()));
            for (Object arrayElement : listRoot) {
                Map<String, Object> recordValues = new HashMap();
                DocumentContext jsonObject = JsonPath.parse(arrayElement);
                for (String field : request.getFields()) {
                    try {
                        recordValues.put(field, jsonObject.read(field));
                    } catch (InvalidPathException e) {
                        recordValues.put(field, null);
                    }
                }
                recordList.add(new Record(recordValues));
            }
        } else if (objectRoot instanceof Map) {
            metadata.put("size", "1");
            Map<String, Object> recordValues = new HashMap();
            DocumentContext jsonObject = JsonPath.parse(objectRoot);
            for (String field : request.getFields()) {
                recordValues.put(field, jsonObject.read(field));
            }
            recordList.add(new Record(recordValues));
        }

        return new RecordList(request.getFields(), recordList, metadata);

    }


    /*----------------------------------------------------------------------------------------------
     * PUBLIC HELPER METHODS
     *--------------------------------------------------------------------------------------------*/    
    public String buildUrl(String queryMethod, String jsonRootPath, BridgeRequest request, ElasticsearchQualificationParser elasticParser) throws BridgeError {

        Map<String,String> metadata = BridgeUtils.normalizePaginationMetadata(request.getMetadata());
        String pageSize = "1000";
        String offset = "0";

        if (StringUtils.isNotBlank(metadata.get("pageSize")) && metadata.get("pageSize").equals("0") == false) {
            pageSize = metadata.get("pageSize");
        }
        if (StringUtils.isNotBlank(metadata.get("offset"))) {
            offset = metadata.get("offset");
        }

        String query = null;
        query = elasticParser.parse(request.getQuery(),request.getParameters());

        // Build up the url that you will use to retrieve the source data. Use the query variable
        // instead of request.getQuery() to get a query without parameter placeholders.
        StringBuilder url = new StringBuilder();
        url.append(this.apiEndpoint)
            .append("/")
            .append(request.getStructure())
            .append("/_")
            .append(queryMethod);

        // if the query is not a request body JSON query...
        if (query.matches("^\\s*\\{.*?\\}\\s*$") == false) {
            addParameter(url, "q", query);
        }

        //only set pagination if we're not counting.
        if (queryMethod.equals("count") == false) {
            addParameter(url, "size", pageSize);
            addParameter(url, "from", offset);
            // only set field limitation if we're not counting
            //   *and* the request specified fields to be returned
            //   *and* the JSON root path has not changed.
            if (request.getFields() != null &&
                request.getFields().isEmpty() == false &&
                jsonRootPath.equals(JSON_ROOT_DEFAULT)
            ) {
                StringBuilder includedFields = new StringBuilder();
                String[] bridgeFields = request.getFieldArray();
                for (int i = 0; i < request.getFieldArray().length; i++) {
                    //strip _source from the beginning of the specified field name as this is redundent to Elasticsearch.
                    includedFields.append(bridgeFields[i].replaceFirst("^_source\\.(.*)", "$1"));
                    //only append a comma if this is not the last field
                    if (i != (request.getFieldArray().length -1)) {
                        includedFields.append(",");
                    }
                }
                addParameter(url, "_source", includedFields.toString());
            }
            //only set sorting if we're not counting *and* the request specified a sort order.
            if (request.getMetadata("order") != null) {
                List<String> orderList = new ArrayList<String>();
                //loop over every defined sort order and add them to the Elasicsearch URL
                for (Map.Entry<String,String> entry : BridgeUtils.parseOrder(request.getMetadata("order")).entrySet()) {
                    String key = entry.getKey().replaceFirst("^_source\\.(.*)", "$1");
                    if (entry.getValue().equals("DESC")) {
                        orderList.add(String.format("%s:desc", key));
                    }
                    else {
                        orderList.add(String.format("%s:asc", key));
                    }
                }
                String order = StringUtils.join(orderList,",");
                addParameter(url, "sort", order);
            }

        }

        logger.trace("Elasticsearch URL: {}", url.toString());
        return url.toString();

    }

    /*----------------------------------------------------------------------------------------------
     * PRIVATE HELPER METHODS
     *--------------------------------------------------------------------------------------------*/
    private void addBasicAuthenticationHeader(HttpGetWithEntity get, String username, String password) {
        String creds = username + ":" + password;
        byte[] basicAuthBytes = Base64.encodeBase64(creds.getBytes());
        get.setHeader("Authorization", "Basic " + new String(basicAuthBytes));
    }
    private void addParameter(StringBuilder url, String parameterName, String parameterValue) {
        if (url.toString().contains("?") == false) {
            url.append("?");
        } else {
            url.append("&");
        }
        url.append(URLEncoder.encode(parameterName))
            .append("=")
            .append(URLEncoder.encode(parameterValue));
    }
    private String elasticQuery(String queryMethod, String jsonRootPath, BridgeRequest request, ElasticsearchQualificationParser elasticParser) throws BridgeError{
        
        String query = elasticParser.parse(request.getQuery(),request.getParameters());
        //Set query to return everything if no qualification defined.
        if (StringUtils.isBlank(query)) {
            query = "*:*";
        }

        String result = null;
        String url = buildUrl(queryMethod, jsonRootPath, request, elasticParser);
        
        // Initialize the HTTP Client, Response, and Get objects.
        HttpClient client = HttpClients.createDefault();
        HttpResponse response;
        HttpGetWithEntity get = new HttpGetWithEntity();
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new BridgeError(e);
        }
        get.setURI(uri);

        // If the query is a JSON object, assume JSON and Request Body searching.
        if (query.matches("^\\s*\\{.*?\\}\\s*$")) {
            HttpEntity entity;
            try {
                String parsedQuery = elasticParser.parse(request.getQuery(), request.getParameters());

                // Set the parsed query as the request body payload.
                entity = new ByteArrayEntity(parsedQuery.getBytes("UTF-8"));
                get.setEntity(entity);
            } catch (UnsupportedEncodingException e) {
                throw new BridgeError(e);
            }
        }

        // Append the authentication to the call. This example uses Basic Authentication but other
        // types can be added as HTTP GET or POST headers as well.
        if (this.username != null && this.password != null) {
            addBasicAuthenticationHeader(get, this.username, this.password);
        }

        // Make the call to the REST source to retrieve data and convert the response from an
        // HttpEntity object into a Java string so more response parsing can be done.
        try {
            response = client.execute(get);
            Integer responseStatus = response.getStatusLine().getStatusCode();

            HttpEntity entity = response.getEntity();
            if (responseStatus >= 300 || responseStatus < 200) {
                String errorMessage = EntityUtils.toString(entity);
                throw new BridgeError(
                    String.format(
                        "The Elasicsearch server returned a HTTP status code of %d, 200 was expected. Response body: %s",
                        responseStatus,
                        errorMessage
                    )
                );
            }

            result = EntityUtils.toString(entity);
            logger.trace(String.format("Request response code: %s", response.getStatusLine().getStatusCode()));
        } catch (IOException e) {
            throw new BridgeError("Unable to make a connection to the Elasticsearch server", e);
        }
        logger.trace(String.format("Elasticsearch response - Raw Output: %s", result));

        return result;
    }

    private void testAuthenticationValues(String restEndpoint, String username, String password) throws BridgeError {
        logger.debug("Testing the authentication credentials");
        HttpGetWithEntity get = new HttpGetWithEntity();
        URI uri;
        try {
            uri = new URI(String.format("%s/_cat/health",restEndpoint));
        } catch (URISyntaxException e) {
            throw new BridgeError(e);
        }
        get.setURI(uri);

        if (username != null && password != null) {
            addBasicAuthenticationHeader(get, this.username, this.password);
        }

        HttpClient client = HttpClients.createDefault();
        HttpResponse response;
        try {
            response = client.execute(get);
            HttpEntity entity = response.getEntity();
            EntityUtils.consume(entity);
            Integer responseCode = response.getStatusLine().getStatusCode();
            if (responseCode == 401) {
                throw new BridgeError("Unauthorized: The inputted Username/Password combination is not valid.");
            }
            if (responseCode < 200 || responseCode >= 300) {
                throw new BridgeError(
                    String.format(
                        "Unsuccessful HTTP response - the server returned a %s status code, expected 200.",
                        responseCode
                    )
                );
            }
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to the Elasticsearch health check API.");
        }
    }

}