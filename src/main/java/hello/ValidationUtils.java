package hello;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.http.util.TextUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

public class ValidationUtils {

	public static final String JSON_V4_SCHEMA_IDENTIFIER = "http://json-schema.org/draft-04/schema#";
	public static final String JSON_SCHEMA_IDENTIFIER_ELEMENT = "$schema";

	public static JsonNode getJsonNode(String jsonText) throws IOException {
		return JsonLoader.fromString(jsonText);
	} // getJsonNode(text) ends

	
	public static JsonNode getJsonNodeFromResource(String resource) throws IOException {
		return JsonLoader.fromResource(resource);
	} // getJsonNode(Resource) ends

	public static JsonSchema getSchemaNode(String schemaText) throws IOException, ProcessingException {
		final JsonNode schemaNode = getJsonNode(schemaText);
		return _getSchemaNode(schemaNode);
	} // getSchemaNode(text) ends


	public static void validateJson(JsonSchema jsonSchemaNode, JsonNode jsonNode) throws ProcessingException {
		ProcessingReport report = jsonSchemaNode.validate(jsonNode);
		if (!report.isSuccess()) {
			for (ProcessingMessage processingMessage : report) {
				throw new ProcessingException(processingMessage);
			}
		}
	} // validateJson(Node) ends

	public static String isJsonValid(JsonSchema jsonSchemaNode, JsonNode jsonNode) throws ProcessingException {
		ProcessingReport report = jsonSchemaNode.validate(jsonNode);

		if (!report.isSuccess()) {
			Iterator<ProcessingMessage> it = report.iterator();
			while (it.hasNext()) {
				ProcessingMessage pm = it.next();
				throw new ProcessingException(pm.toString());
			}
		} else {
			return "success";
		}
		return null;

	} // validateJson(Node) ends

	public static String isJsonValid(String schemaText, String jsonText) throws ProcessingException, IOException {
		final JsonSchema schemaNode = getSchemaNode(schemaText);
		final JsonNode jsonNode = getJsonNode(jsonText);
		return isJsonValid(schemaNode, jsonNode);
	} // validateJson(Node) ends
	

	private static JsonSchema _getSchemaNode(JsonNode jsonNode) throws ProcessingException {
		final JsonNode schemaIdentifier = jsonNode.get(JSON_SCHEMA_IDENTIFIER_ELEMENT);
		if (null == schemaIdentifier) {
			((ObjectNode) jsonNode).put(JSON_SCHEMA_IDENTIFIER_ELEMENT, JSON_V4_SCHEMA_IDENTIFIER);
		}

		final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
		return factory.getJsonSchema(jsonNode);
	} // _getSchemaNode() ends

	public static Map<String, String> splitToMap(String source, String entriesSeparator, String keyValueSeparator) {
		Map<String, String> map = new HashMap<String, String>();
		String[] entries = source.split(entriesSeparator);
		for (String entry : entries) {
			if (!TextUtils.isEmpty(entry) && entry.contains(keyValueSeparator)) {
				String[] keyValue = entry.split(keyValueSeparator);
				map.put(keyValue[0], keyValue[1]);
			}
		}
		return map;
	}
	
	public static boolean checkNumber(String content){
		String pattern = "^[0-9.]*$";
			return Pattern.matches(pattern, content);
		
	}
}
