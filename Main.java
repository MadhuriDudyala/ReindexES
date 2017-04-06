package edu.mayo.registry.fhir.format;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.mayo.registry.fhir.util.PropertyHandler;

public class Main {
	
	protected final static String PROPERTY_FILE_NAME = "resource.properties";
	
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException {
		
		AbstractApplicationContext context = new ClassPathXmlApplicationContext(
				"application-context.xml");
		
		ReindexUtil reindexUtil = (ReindexUtil)context.getBean("reindexUtil"); 
		try 
		{
			String resourceName = StringUtils.EMPTY;
			
			Properties properties = PropertyHandler.getProperties(PROPERTY_FILE_NAME);
			
			if(args.length == 0)
			{
				// get the resource from properties file
				resourceName = properties.getProperty("resource");				
				String[] res = resourceName.split("\\|");				
				for (int i = 0; i < res.length; i++) 
				{
					reindexUtil.processRequest(res[i]);		
				}				
			}
			else 
			{
				resourceName = args[0]; //specific resource
				reindexUtil.processRequest(resourceName);
			}
			
		} 
		catch (Exception e) 
		{		
			e.printStackTrace();
			System.exit(1);
		}		
	}
}
