package edu.format;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.xml.parsers.ParserConfigurationException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import ca.uhn.fhir.model.api.IResource;
import ca.uhn.fhir.parser.JsonParser;
import ca.uhn.fhir.parser.XmlParser;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import edu.local.LocalMeta;
import edu.local.LocalResource;
import edu.local.LocalResourceReference;
import edu.local.LocalValueString;
import edu.localformat.LocalMetaJsonDeserializer;
import edu.localformat.LocalMetaJsonSerializer;
import edu.localformat.LocalResourceReferenceJsonSerializer;
import edu.localformat.LocalValueStringJsonSerializer;
import edu.util.CommonUtilities;
import edu.util.exception.UtilException;

@Service
public class FormatService {

    static final String FORMAT_XML = "xml";
    static final String FORMAT_JSON = "json";
    static final String FORMAT_UNKNOWN = "unknown";
    static final String RESOURCE_UNKNOWN = "Unknown";
    static final String PACKAGE = "edu.local.";
	final static String XML_RESOURCE_TYPE_REGEX = "^\\s*<(\\w+)";
	final static Pattern xmlResourceTypePattern = Pattern.compile(XML_RESOURCE_TYPE_REGEX);
        
    @Autowired
    private CommonUtilities commonUtilities;
    
	public enum localObjects {
		Activity,
		Role,
		Program
	}

    public String convert(String payload, String toFormatType, String resourceName) throws UtilException
    {
        String convertedString;
        String toFormat = conversionFormat(toFormatType);

        if (toFormat.equals(FORMAT_JSON)) {
            convertedString = convertToJson(payload, resourceName);
        } else {
            throw new UtilException("Unknown format was entered.");
        }
        return convertedString;
    }

    //return the format you want to convert the payload to.
    private String conversionFormat(String toConvertType)
    {
    	if (toConvertType.equalsIgnoreCase(FORMAT_JSON))
            return FORMAT_JSON;
        else
            return FORMAT_UNKNOWN;
    }

    //Converts the payload from xml to json
    private String convertToJson(String xmlPayload, String resourceName) throws UtilException
    {
        String conversionString;        
        Object object = null;

        if (isLocalResource(resourceName)) {
        	object = this.toObjectFromXmlLocal(xmlPayload);
        }
        else {
        	object = this.toObjectFromXml(xmlPayload);	
        }        

        if (object != null) { // successful convert from XML, reserialize to JSON
        	if (isLocalResource(resourceName)) {
        		conversionString = this.toJsonLocal((LocalResource) object);
        	}
        	else
        	{
        		conversionString = this.toJson((IResource)object);	
        	}
        	
            if (conversionString == null) {
                throw new UtilException("Unknown Resource Type.  Unable to convert.");
            }
        } else {
            conversionString = xmlPayload;
        }
        return conversionString;
    }
    
    @SuppressWarnings("unchecked")
	private LocalResource toObjectFromXmlLocal(String xml) {
    	System.out.println(xml);
        Class<? extends LocalResource> clazz = null;
        LocalResource obj = null;
        try {
        	// hydrate object from XML
			clazz = (Class<? extends LocalResource>) Class.forName(PACKAGE + getXmlType(xml));
            if(clazz.getName().endsWith("Role")||clazz.getName().endsWith("Program")){
                clazz = (Class<? extends LocalResource>) Class.forName(PACKAGE + getXmlType(xml)+"Deser");
            }
			ObjectMapper mapper = new XmlMapper();
            JacksonXmlModule module = new JacksonXmlModule();
            module.setDefaultUseWrapper(false);
            mapper.registerModule(module);
            obj = mapper.readValue(xml, clazz);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return obj;
	}
    
    static String getXmlType(String xml) {
		Matcher matcher = xmlResourceTypePattern.matcher(xml);
		if (matcher.find() && matcher.groupCount() > 0) {
			String resourceName = matcher.group(1);
			if (resourceName.length() > 1) {
				return resourceName.substring(0, 1).toUpperCase() +
						resourceName.substring(1).toLowerCase();
			} else {
				return resourceName.toUpperCase();
			}
		} else {
			return "Unknown";
		}
	}
    
    private IResource toObjectFromXml(String xml) {
		XmlParser xmlParser = new XmlParser(commonUtilities.getFhirContext());

		try {
			IResource resource = xmlParser
					.parseResource(xml);

			return resource;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
    
    private String toJsonLocal(LocalResource resource) {
		ObjectMapper mapper = new ObjectMapper();
		SimpleModule module = new SimpleModule("TestModule", new Version(1, 0, 0, null, null, null));
		module.addSerializer(LocalMeta.class, new LocalMetaJsonSerializer(LocalMeta.class));
		module.addSerializer(LocalResourceReference.class, new LocalResourceReferenceJsonSerializer(LocalResourceReference.class));
		module.addSerializer(LocalValueString.class, new LocalValueStringJsonSerializer(LocalValueString.class));
		mapper.registerModule(module);
		 
		String serialized = null;
		try {
			serialized = mapper.writeValueAsString(resource);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return serialized;
	}
    
    private String toJson(IResource resource) {

		JsonParser jsonComposer = new JsonParser(commonUtilities.getFhirContext());
		String resourceStr = null;

		try {
			resourceStr = jsonComposer.encodeResourceToString(resource);
			return resourceStr;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

    public String toAtomEntry(String resource, String resourceType, String id, String version) throws ParserConfigurationException {
        Map<String, String> config = new LinkedHashMap<String, String>();
        JsonBuilderFactory factory = Json.createBuilderFactory(config);
        JsonArrayBuilder link = factory.createArrayBuilder()
                .add(factory.createObjectBuilder()
                        .add("relation", "self")
                        .add("url", resourceType + "/"
                                + id + "/_history/"
                                + version));

        JsonReader reader = Json.createReader(new StringReader(resource));
        JsonObject jsonResource = reader.readObject();

        JsonObject entry = factory.createObjectBuilder()
                .add("entry", factory.createObjectBuilder()
                        .add("link", link)
                        .add("resource", jsonResource)).build();

        return entry.toString();
    }
    
	static boolean isLocalResource(String resourceName) {
		boolean found = false;
		for (localObjects object: localObjects.values()) {
			if (object.name().equals(resourceName)) {
				found = true;
				break;
			}
		}
		return found;
	}
}
