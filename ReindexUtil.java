package edu.mayo.registry.fhir.format;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.mayo.registry.fhir.util.HttpMethodExecutor;
import edu.mayo.registry.fhir.util.entity.GenericHbaseEntity;
import edu.mayo.registry.fhir.util.exception.UtilException;

@PropertySource(value="classpath:application.properties")
public class ReindexUtil {

    @Autowired
    private HttpMethodExecutor httpMethodExecutor;
    
    @Autowired
    private FormatService formatService;
    
	@Autowired
	private HbaseTemplate hbaseTemplate;

	@Autowired
	@Value("#{'${ELASTIC_SEARCH_SERVER_BASE_URL}'}")
	private String elasticSearchClusterUrl;

	@Autowired
	@Value("#{'${ELASTIC_SEARCH_CLUSTER_USERNAME}'}")
	private String elasticSearchClusterUsername;

	@Autowired
	@Value("#{'${ELASTIC_SEARCH_CLUSTER_PASSWORD}'}")
	private String elasticSearchClusterPassword;

	private static final byte[] ID_FAMILY = Bytes.toBytes("id");
	private static final byte[] FHIR_FAMILY = Bytes.toBytes("fhir");
	private static final byte[] KEY_QUALIFIER = Bytes.toBytes("key");
	private static final byte[] VERSION_QUALIFIER = Bytes.toBytes("version");
	private static final byte[] PROFILE_QUALIFIER = Bytes.toBytes("profile");
	private static final byte[] RESOURCE_QUALIFIER = Bytes.toBytes("resource");
	
	private static final String convertsionEndpointUrl = "http://rof0000959:9024/registry-format-conv/Format_Conversion";

	/**
	 * Process the reindexing for the given resource name
	 * @param resourceName
	 * @throws IOException
	 * @throws ParserConfigurationException 
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public void processRequest(String resourceName) throws IOException, InterruptedException, ExecutionException, ParserConfigurationException {

		//delete the index
       /* httpMethodExecutor  = new HttpMethodExecutor(elasticSearchClusterUsername, elasticSearchClusterPassword);
        String webServiceUrl = getResourceUri(resourceName);
        ResponseEntity<String> response = httpMethodExecutor.executeDelete(webServiceUrl, null, null);
        
        //if the index is deleted successfully
        if (response.getStatusCode().equals(HttpStatus.CREATED) || response.getStatusCode().equals(HttpStatus.OK))
        {
        	//create the index 
			String mapping = getFile(resourceName);
			httpMethodExecutor  = new HttpMethodExecutor(elasticSearchClusterUsername, elasticSearchClusterPassword);
			webServiceUrl = getResourceUri(resourceName);
			response = httpMethodExecutor.executePut(webServiceUrl, null, null, null, mapping); //executePost

        	// if the index is created successfully
			 if (response.getStatusCode().equals(HttpStatus.CREATED) || response.getStatusCode().equals(HttpStatus.OK))
		     {
				 reIndexData(resourceName);
		     }
			 else 
			 {
				throw new UtilException("Index creation failed for [" + resourceName + "]");
			 }			
        }
        else 
        {
        	throw new UtilException("Index deleted failed for [" + resourceName + "]");
        }*/
        reIndexData(resourceName);
	}
	
	/**
	 * Reindex the data for the given resource
	 * @param resourceName
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws ParserConfigurationException 
	 * @throws IOException 
	 */
	public void reIndexData(String resourceName) throws InterruptedException, ExecutionException, ParserConfigurationException, IOException
	{
		List<String> failedIds = new ArrayList<String>();
		
		//String uType = StringUtils.capitalize(resourceName);
		
		httpMethodExecutor  = new HttpMethodExecutor(elasticSearchClusterUsername, elasticSearchClusterPassword);
		long startTime = System.currentTimeMillis();
		List<GenericHbaseEntity> genericHbaseEntityList = this.partialScan(resourceName);
		long endTime = System.currentTimeMillis();
		System.out.println("took " + (endTime - startTime) + " milliseconds");
		if (genericHbaseEntityList != null && 
                genericHbaseEntityList.size() > 0) 
		{
			int size = genericHbaseEntityList.size();
			int threads = Runtime.getRuntime().availableProcessors();
			
			if (size < threads)
			{
				for (GenericHbaseEntity genericHbaseEntity : genericHbaseEntityList) 
				{	                
	                String payload = genericHbaseEntity.getResource();
	                
	               //new way to call the conversion endpoint
    	           String authHeader = getAuthHeader();
                   String json = getResourceInJson(payload, authHeader ,"application/json");
     
	                //old way
	               //String json = formatService.convert(payload, "json", resourceName);
	                
	                String indexedId = genericHbaseEntity.getId();
	                String id = indexedId.substring(indexedId.lastIndexOf("/") + 1);
	                String version = genericHbaseEntity.getVersion();
	                if (StringUtils.isNotBlank(json))
	                {
		                String jsonWithAtomEntry = formatService.toAtomEntry(json, resourceName, id, version);		                
		                String webServiceUrl = getResourceUri(resourceName, id);
		                ResponseEntity<String> response = httpMethodExecutor.executePut(webServiceUrl, null, null, null, jsonWithAtomEntry);
		                if ( !(response.getStatusCode().equals(HttpStatus.CREATED) || response.getStatusCode().equals(HttpStatus.OK)) )
		                {
		                	failedIds.add(id );
		                }	                	
	                }
	                else
	                {
	                	failedIds.add(id );
	                }
				}
			}
			else 
			{
				process(genericHbaseEntityList, resourceName, threads, size);					
			}	
			
			if (failedIds.size() > 0)
			{
				throw new UtilException("Records Failed to migrate of [" + resourceName + "]. failedIds: " + StringUtils.join(failedIds, ","));
			}
		}
		else 
		{
			throw new UtilException("No records retreived from Hbase for insertion of [" + resourceName + "]");				
		}
	}
	
	/**
	 * Process the data concurrently
	 * @param genericHbaseEntityList
	 * @param resourceName
	 * @param uType
	 * @param threads
	 * @param size
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void process(List<GenericHbaseEntity> genericHbaseEntityList, String resourceName, int threads, int size) throws InterruptedException, ExecutionException
	{
		List<List<String>> failedIds = new ArrayList<List<String>>();
		ExecutorService service = Executors.newFixedThreadPool(threads);		
		List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();
		int portion = size / threads;		
		for (int i = 0; i < threads; ++i)
		{
			int from = i * portion;
			int to = i < threads - 1 ? (i+1) * portion : size;			
			Callable<List<String>> worker = new ESTask(genericHbaseEntityList.subList(from, to), resourceName);
			Future<List<String>> submit = service.submit(worker);			
			futures.add(submit);
		}

		for (Future<List<String>> future : futures) 
		{
			List<String> ids = (List<String>)future.get();
			if (ids.size() > 0)
			{
				failedIds.add(future.get());	
			}
		}

		if (failedIds.size() > 0)
		{
			throw new UtilException("Records Failed to migrate of [" + resourceName + "]. failedIds: " + StringUtils.join(failedIds, ","));
		}

		service.shutdown();
	}

	/**
	 * Method for the partial scan from HBase
	 * @param type
	 * @return
	 */
	private List<GenericHbaseEntity> partialScan(String type){		
		byte[] prefix=Bytes.toBytes("/"+type+"/");

		Scan scan = new Scan(prefix);
		scan.addFamily(ID_FAMILY);
		scan.addFamily(FHIR_FAMILY);

		PrefixFilter prefixFilter = new PrefixFilter(prefix);
		scan.setFilter(prefixFilter);

		return hbaseTemplate.find(toTable(), scan, new RowMapper<GenericHbaseEntity>() {
			public GenericHbaseEntity mapRow(Result result, int i) throws Exception {
				return rowToHbaseEntity(result, i);
			}
		});
	}

	/**
	 * Map Row data retrieved from HBase
	 * @param result
	 * @param i
	 * @return
	 * @throws SQLException
	 */
	protected static GenericHbaseEntity rowToHbaseEntity(Result result, int i) throws SQLException {
		GenericHbaseEntity entity = new GenericHbaseEntity();
		entity.setId(Bytes.toString(result.getValue(ID_FAMILY, KEY_QUALIFIER)));
		entity.setVersion(Bytes.toString(result.getValue(ID_FAMILY, VERSION_QUALIFIER)));
		entity.setProfile(Bytes.toString(result.getValue(FHIR_FAMILY, PROFILE_QUALIFIER)));
		entity.setResource(Bytes.toString(result.getValue(FHIR_FAMILY, RESOURCE_QUALIFIER)));
		Cell tsCell = result.getColumnLatestCell(FHIR_FAMILY, RESOURCE_QUALIFIER);
		entity.setTimestamp(tsCell == null ? null : tsCell.getTimestamp());
		return entity;
	}

	/**
	 * Get the mapping Json from the file 
	 * @param resourceName
	 * @return
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
    private String getFile(String resourceName) throws FileNotFoundException, UnsupportedEncodingException 
    {
		InputStream in = this.getClass().getResourceAsStream("/registry-" + resourceName.toLowerCase() + "-mapping.json");
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
		final JsonParser parser = new JsonParser();
		final JsonObject json =  (JsonObject) parser.parse(br);
		return json.toString();
	}
    
    /**
     * Worker Thread
     * @author 910948
     *
     */
    class ESTask implements Callable<List<String>>{
		
		private List<GenericHbaseEntity> data;
		private List<String> failedIds = new ArrayList<String>();
		private String resourceName;

		public ESTask(List<GenericHbaseEntity> data, String resourceName){
			this.data = data;
			this.resourceName = resourceName;
		}

		public List<String> call() throws Exception {

			int size = data.size();

			for(int j = 0; j < size ; j++)			
			{
				GenericHbaseEntity genericHbaseEntity = data.get(j);
				//process 
				String payload = genericHbaseEntity.getResource();
				
                //new way to call the conversion endpoint
                String authHeader = getAuthHeader();
                String json = getResourceInJson(payload, authHeader ,"application/json");
                
                //old way
				//String json = formatService.convert(payload, "json", resourceName);

				String indexedId = genericHbaseEntity.getId();
				String id = indexedId.substring(indexedId.lastIndexOf("/") + 1);
				String version = genericHbaseEntity.getVersion();
				String jsonWithAtomEntry = formatService.toAtomEntry(json, resourceName, id, version);

				String webServiceUrl = getResourceUri(resourceName, id);
				ResponseEntity<String> response = httpMethodExecutor.executePut(webServiceUrl, null, null, null, jsonWithAtomEntry);
				if ( !(response.getStatusCode().equals(HttpStatus.CREATED) || response.getStatusCode().equals(HttpStatus.OK)) )
				{
					failedIds.add(id );
				}
			}
			return failedIds;
		}
	}
	
    /**
     * Table info
     * @return
     */
	protected static String toTable() {
		return "registry-resources";
	}
	
	/**
	 * Get resource info
	 * @param resourceName
	 * @return
	 */
	private String getResourceUri(String resourceName) 
	{
        return  elasticSearchClusterUrl + "registry-" + resourceName.toLowerCase();
    }
	
	/**
	 * Get resource info
	 * @param resourceName
	 * @param id
	 * @return
	 */
	private String getResourceUri(String resourceName, String id) 
	{
        return  elasticSearchClusterUrl + "registry-" + resourceName.toLowerCase() + "/" + resourceName.toLowerCase() + "/"+ id;
    }

	/**
	 * 
	 * @return
	 */
	public String getAuthHeader()
	{
		//update your password
    	String userpass = String.format("%s:%s", "id","pwd");
    	return String.format("Basic %s", Base64.encodeBase64String(userpass.getBytes()));
	}
	
	/**
	 * Call the /Format_Conversion to get the required format
	 * @param resource
	 * @param requestAuthHeader
	 * @param toFormat
	 * @return
	 * @throws IOException
	 */
    public String getResourceInJson(String resource, String requestAuthHeader, String toFormat) throws IOException {
      HashMap<String, String> headersMap = new HashMap<String, String>();
      headersMap.put("Accept", toFormat);
      headersMap.put("Authorization", requestAuthHeader);

      HttpMethodExecutor formatHttpMethodExecutor = new HttpMethodExecutor();
      ResponseEntity<String> formattedResponse = formatHttpMethodExecutor.executePost(convertsionEndpointUrl, headersMap, null, null, resource);
      return formattedResponse.getBody();
  }
}
