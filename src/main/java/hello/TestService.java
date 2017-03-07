package hello;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.apache.catalina.connector.Request;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.mapred.gethistory_jsp;
import org.apache.tomcat.util.codec.binary.StringUtils;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.arjuna.ats.internal.jdbc.drivers.modifiers.extensions;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.google.gson.JsonArray;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import redis.clients.jedis.Jedis;

@RestController
@RequestMapping(value = "/")
public class TestService {
	public Jedis jedis;
	public JSONParser jsonparser;

	// @RequestMapping(value = "/plan", method = RequestMethod.POST)
	// @ResponseBody
	// public String storeTopLevel(@RequestBody JSONObject jsonObject)
	// throws FileNotFoundException, IOException, ParseException,
	// ProcessingException {
	//
	// JSONObject jsonSchemaObj = (JSONObject) jsonparser
	// .parse(new FileReader("src/main/resources/insurance_plan.json"));
	// System.out.println(jsonSchemaObj.toString());
	// String status = ValidationUtils.isJsonValid(jsonSchemaObj.toString(),
	// jsonObject.toString());
	// if (status == "success") {
	// String UUIDValue = getHash();
	// jsonObject.toString();
	// // Jedis jedis = getRedisConnection();
	// jedis.set(UUIDValue, jsonObject.toJSONString());
	//
	// // JSONObject test_jsonObject = new JSONObject();
	// return UUIDValue;
	// }
	// return status;
	//
	// }

	// @RequestMapping(value = "/plan/{id}", method = RequestMethod.PUT)
	// @ResponseBody
	// public String updateTopLevel(@RequestBody JSONObject jsonObject,
	// @PathVariable String id)
	// throws FileNotFoundException, IOException, ParseException,
	// ProcessingException {
	//
	// JSONObject jsonSchemaObj = (JSONObject) jsonparser
	// .parse(new FileReader("src/main/resources/insurance_plan.json"));
	// System.out.println(jsonSchemaObj.toString());
	// String status = ValidationUtils.isJsonValid(jsonSchemaObj.toString(),
	// jsonObject.toString());
	// if (status == "success") {
	// // Jedis jedis = getRedisConnection();
	// String jsonString = jedis.get(id);
	//
	// if (null != jsonString) {
	// jsonObject.toString();
	// jedis.set(id, jsonObject.toJSONString());
	// }
	// status = "Plan does not exist";
	// // JSONObject test_jsonObject = new JSONObject();
	// return status;
	// }
	// return status;
	//
	// }

	public TestService() {

		jedis = getRedisConnection();
		jsonparser = getJsonParser();

	}

	// @RequestMapping(value = "/plan/{id}", method = RequestMethod.GET)
	// @ResponseBody
	// public JSONObject getTopLevel(@PathVariable String id) {
	//
	// // Jedis jedis = getRedisConnection();
	//
	// String jsonString = jedis.get(id);
	//
	// JSONObject json = new JSONObject();
	//
	// if (null != jsonString) {
	// try {
	// json = (JSONObject) jsonparser.parse(jsonString);
	// } catch (ParseException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	//
	// json.put("Message", "Does not Exist");
	// return json;
	// }

	// @RequestMapping(value = "/plan", method = RequestMethod.GET)
	// @ResponseBody
	// public String getAllObjects() throws ParseException {
	//
	// // Jedis jedis = getRedisConnection();
	//
	// HashSet<String> as = (HashSet<String>) jedis.keys("*");
	// JSONObject json = new JSONObject();
	// String startJson = "[";
	// String endJson = "]";
	// String initial = "{}";
	// String jsonTotal = null;
	// for (String temp : as) {
	// // System.out.println("List of stored keys:: " + jedis.get(temp));
	//
	// if (jedis.type(temp).equals("string")) {
	// String jsonString = jedis.get(temp);
	//
	// initial = initial + "," + jsonString;
	//
	// }
	//
	// }
	// jsonTotal = startJson + "," + initial + "," + endJson;
	//
	// // json = (JSONObject) getJsonParser().parse(jsonTotal);
	// // JSONObject json = new JSONObject();
	// return jsonTotal;
	// }

	// @RequestMapping(value = "/plan/{id}", method = RequestMethod.DELETE)
	// @ResponseBody
	// public JSONObject deleteById(@PathVariable String id) {
	//
	// // Jedis jedis = getRedisConnection();
	//
	// String jsonString = jedis.get(id);
	// JSONParser jsonParser = new JSONParser();
	// JSONObject json = new JSONObject();
	//
	// if (null != jsonString) {
	// jedis.del(id);
	// try {
	// json = (JSONObject) jsonParser.parse(jsonString);
	// } catch (ParseException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// } else {
	// json.put("Plan:", "Does not exist");
	// }
	//
	// return json;
	// }

	@RequestMapping(value = "/test_nesting/{id}", method = RequestMethod.DELETE)
	@ResponseBody
	public JSONObject deleteNestingById(@PathVariable String id) {

		JSONObject jsonObject = new JSONObject();
		if (!jedis.hgetAll(id).isEmpty()) {

			// return reconstructJson(id, parentJson);
			jsonObject = (JSONObject) reconstructJson(id, new JSONObject());
			deleteById(id);
			return jsonObject;
		} else {
			jsonObject.put("Message", "ID does not exist");
			return jsonObject;
		}
	}

	// @RequestMapping(value = "/test_nesting/{id}", method = RequestMethod.PUT)
	// @ResponseBody
	// public String putExistingid(@PathVariable String id, @RequestBody
	// JSONObject jsonObject)
	// throws JsonParseException, JsonMappingException, IOException {
	// if (!jedis.hgetAll(id).isEmpty()) {
	// deleteById(id);
	//
	// recursion(jsonObject.toJSONString(), id);
	// return "Success";
	// } else {
	// return "Specified Id does not Exist";
	// }
	//
	// }

	private void deleteById(String id) {
		Set<String> keys = jedis.keys(id + "*");
		for (String key : keys) {
			jedis.del(key);
		}
	}

	// public Object deleteNestedObjects(String id, Object object) {
	// // Jedis jedis = getRedisConnection();
	// Map<String, String> jsonReconstruct = null;
	// Set jsonList = null;
	// JSONObject parentJson = null;
	// JSONArray parentArray = null;
	//
	// if (object instanceof JSONArray) {
	// jsonList = jedis.smembers(id);
	// parentArray = (JSONArray) object;
	//
	// for (Object entry : jsonList) {
	// // System.out.println(stock);
	//
	// if (getElementType(entry.toString()).equals("Object")) {
	// JSONObject childJson = new JSONObject();
	// parentArray.add(reconstructJson(entry.toString(), childJson));
	//
	// System.out.println("object");
	// } else if (getElementType(entry.toString()).equals("Array")) {
	//
	// JSONArray jsonArray = new JSONArray();
	// parentArray.add(reconstructJson(entry.toString(), jsonArray));
	//
	// System.out.println("Array");
	// } else {
	// // parentArray.add(entry.toString());
	// jedis.del(id);
	// }
	//
	// }
	// return parentArray;
	//
	// } else if (object instanceof JSONObject) {
	// jsonReconstruct = jedis.hgetAll(id);
	// parentJson = (JSONObject) object;
	//
	// for (Map.Entry<String, String> entry : jsonReconstruct.entrySet()) {
	// System.out.println("Key : " + entry.getKey() + " Value : " +
	// entry.getValue());
	//
	// if (getElementType(entry.toString()).equals("Object")) {
	// JSONObject childJson = new JSONObject();
	//
	// parentJson.put(entry.getKey(), reconstructJson(entry.getValue(),
	// childJson));
	// System.out.println("object");
	// } else if (getElementType(entry.toString()).equals("Array")) {
	//
	// // parentJson
	// JSONArray jsonArray = new JSONArray();
	// parentJson.put(entry.getKey(), reconstructJson(entry.getValue(),
	// jsonArray));
	// // JSONObject childJson = new JSONObject();
	// // childJson=reconstructJson(entry.getValue(), childJson);
	// // jsonArray.add(childJson);
	// // parentJson.put(entry.getKey(),jsonArray );
	//
	// System.out.println("Array");
	// } else {
	// // parentJson.put(entry.getKey(), entry.getValue());
	// jedis.del(id);
	// }
	//
	// // jsonObject.put(entry.getKey(), entry.getValue());
	//
	// }
	// return parentJson;
	//
	// } else {
	//
	// }
	// return parentJson;
	//
	// // jsonObject.putAll(jsonReconstruct);
	//
	// }

	@RequestMapping(value = "/test_nesting/jsonpath", method = RequestMethod.POST)
	@ResponseBody
	public void test_nesting_json_path(@RequestBody JSONObject jsonObject)
			throws JsonParseException, JsonMappingException, IOException, JSONException {

		String json_string = jsonObject.toJSONString();

		JsonParser_temp jsonParser_temp = new JsonParser_temp(json_string);

		Iterator<String> itr = jsonParser_temp.getPathList().iterator();

		while (itr.hasNext()) {
			String employee = itr.next();

			System.out.println(employee);

		}
	}

	@RequestMapping(value = "/test_nesting", method = RequestMethod.POST)
	@ResponseBody
	public void test_nesting(@RequestBody JSONObject jsonObject)
			throws JsonParseException, JsonMappingException, IOException {

		String json_string = jsonObject.toJSONString();
		// durin post i need to remove the implementation of getting the ids
		// from the user and instead generate ids on my own.
		// Also i need to set the ids inside the same json using value _id
		// also during a patch create a method to check if IDS are already
		// present then use the same id's instead of creating new ones again.
		//firstRecursiveSubmit(json_string, null,"");
		firstRecursiveSubmit_optimized_keys(json_string, null, "");
	}

	//////////////////////////////////////////////////////////////////////////////
	@RequestMapping(value = "/test_nesting/{id}", method = RequestMethod.GET)
	@ResponseBody
	public JSONObject test_nesting(@PathVariable String id)
			throws JsonParseException, JsonMappingException, IOException {
		JSONObject parentJson = new JSONObject();
		parentID = id;
		JSONObject returnJSON = (JSONObject) reconstructJson(id, parentJson, "");
		return returnJSON;

	}

	public JSONObject normal_get(String id) throws JsonParseException, JsonMappingException, IOException {
		JSONObject parentJson = new JSONObject();
		parentID = id;
		JSONObject returnJSON = (JSONObject) reconstructJson(id, parentJson);
		return returnJSON;

	}
	

	// @RequestMapping(value = { "/{level1}/{id1}",
	// "/{level1}/{id}/{level2}/{id2}",
	// "/{level1}/{id1}/{level2}/{id2}/{level3}/{id3}flushall",
	// "/{level1}/{id1}/{level2}/{id2}/{level3}/{id3}/{level4}/{id4}" }, method
	// = RequestMethod.PATCH)
	// @ResponseBody
	// public String test_nesting_patch(@RequestBody JSONObject newJsonObject,
	// @PathVariable Map<String, String> pathVariables) throws ParseException {
	//
	// if (pathVariables.containsKey("level4")) {
	// // return pathVariables.get("id4");
	// } else if (pathVariables.containsKey("level3")) {
	// // return pathVariables.get("id3");
	// } else if (pathVariables.containsKey("level2")) {
	// // return pathVariables.get("id2");
	// } else if (pathVariables.containsKey("level1")) {
	//
	// String key = pathVariables.get("id1");
	//
	// checkJsonAndMergeIfValid(newJsonObject, key);
	//
	// // return new JSONObject(jedis.hgetAll(pathVariables.get("id1")));
	//
	// } else {
	// return "";
	// }
	// return "";
	// }

	@RequestMapping(value = "/test_nesting/{id}", method = RequestMethod.PATCH)
	@ResponseBody
	public void test_nesting_lib_patch(@RequestBody JsonNode jsonNode, @PathVariable String id) throws ParseException,
			JsonParseException, JsonMappingException, IOException, JsonPatchException, ProcessingException {

		// convert received item to json node
		final ObjectMapper mapper = new ObjectMapper();
		JsonNode node = mapper.convertValue(jsonNode, JsonNode.class);
		final JsonPatch patch = JsonPatch.fromJson(node);

		// get original JSON object and convert to json node based on parent ID
		JSONObject originalJson = normal_get(id);
		JsonNode destiNode = mapper.convertValue(originalJson, JsonNode.class);

		// apply json patch on received JSON
		final JsonNode patched = patch.apply(destiNode);

		// add validate before deleting and adding a new object against the
		// schema

		JSONObject patchedJSONObject = mapper.convertValue(patched, JSONObject.class);
		JSONObject jsonSchemaObj = (JSONObject) jsonparser
				.parse(new FileReader("src/main/resources/insurance_plan.json"));
		// String status = ValidationUtils.isJsonValid(jsonSchemaObj.toString(),
		// patchedJSONObject.toString());
		// if (status == "success") {

		deleteNestingById(id);
		
		secondRecursiveSubmit(patchedJSONObject.toString(),"");
	//	test_nesting(patchedJSONObject);

		// }

	}

	public String checkJsonAndMergeIfValid(JSONObject newJsonObject, String key) throws ParseException {

		String json = newJsonObject.toString();
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(json);

		Map<String, String> map = (Map) obj;
		Map<String, String> map_key = jedis.hgetAll(key);

		for (Map.Entry<String, String> entry : map.entrySet()) {
			if (map_key.containsKey(entry.getKey()) && jedis.type(map_key.get(entry.getKey())).equals("none")
					&& !entry.getKey().equals("_id")) {
				map_key.put(entry.getKey(), entry.getValue());
			}
		}
		jedis.hmset(key, map_key);
		return "";

	}

	// @RequestMapping(value = "/test_string/{id}", method = RequestMethod.GET)
	// @ResponseBody
	// public JSONObject test_string(@PathVariable String id)
	// throws JsonParseException, JsonMappingException, IOException {
	// JSONObject parentJson = new JSONObject();
	// // return reconstructJson(id, parentJson);
	// parentJson.put("Value=", getElementType(id));
	// return parentJson;
	//
	// }

	public String getElementType(String id) {
		String value = "";
		if (id.contains("_")) {

			String[] arr = id.split("\\_");
			// System.out.println(arr.toString());
			if (arr.length > 2) {
				value = arr[arr.length - 2];
			}
		}
		return value;
	}

	// Method to convert the elements back to JSON Object
	private String parentID = null;
	// private ArrayList<String> arrayListIds = new ArrayList<String>();

	// public Boolean check(String id) {
	//
	// if (arrayListIds.contains(id)) {
	// return true;
	// } else {
	// arrayListIds.add(id);
	// return false;
	// }
	//
	// }

	public Object reconstructJson(String id, Object object, String jsonPath) {

		// Jedis jedis = getRedisConnection();
		Map<String, String> jsonReconstruct = null;
		ArrayList jsonList = null;
		JSONObject parentJson = null;
		JSONArray parentArray = null;

		if (object instanceof JSONArray) {
			String parentPath = jsonPath;
			jsonList = new ArrayList();
			jsonList.addAll(jedis.smembers(id));
			parentArray = (JSONArray) object;

			for (int i = 0; i < jsonList.size(); i++) {
				// System.out.println(stock);
				Object entry = jsonList.get(i);
				if (jedis.type(entry.toString()).equals("hash")) {

					System.out.println("id at this stage" + id);
					System.out.println("Jedis object type=" + jedis.type(entry.toString()));
					JSONObject childJson = new JSONObject();
					jsonPath = parentPath + "/" + i;
					childJson.put("_self", jsonPath);
					parentArray.add(reconstructJson(entry.toString(), childJson, jsonPath));

					System.out.println("object");
				} else if (jedis.type(entry.toString()).equals("set")) {
					System.out.println("id at this stage" + id);
					System.out.println("Jedis object type=" + jedis.type(entry.toString()));

					JSONArray jsonArray = new JSONArray();
					parentArray.add(reconstructJson(entry.toString(), jsonArray, jsonPath));

					System.out.println("Array");
				} else {
					System.out.println("id at this stage" + id);
					parentArray.add(entry.toString());
				}

			}
			return parentArray;

		} else if (object instanceof JSONObject) {
			jsonReconstruct = jedis.hgetAll(id);
			parentJson = (JSONObject) object;
			String parentPath = jsonPath;

			for (Map.Entry<String, String> entry : jsonReconstruct.entrySet()) {

				//if (entry.getKey().equals("_id") && entry.getValue().equals(parentID)) {
				if (entry.getKey().equals("_id")) {
					System.out.println("id at this stage" + id);
					parentJson.put(entry.getKey(), entry.getValue());
				} else {
					System.out.println("id at this stage" + id);
					System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());

					if (jedis.type(entry.getValue().toString()).equals("hash")) {
						// System.out.println("Jedis object type=" +
						// jedis.type(entry.getValue().toString()));
						JSONObject childJson = new JSONObject();
						jsonPath = parentPath + "/" + entry.getKey();
						childJson.put("_self", jsonPath);
						parentJson.put(entry.getKey(), reconstructJson(entry.getValue(), childJson, jsonPath));
						System.out.println("object");
					} else if (jedis.type(entry.getValue().toString()).equals("set")) {
						System.out.println("id at this stage" + id);
						System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));
						jsonPath = parentPath + "/" + entry.getKey();
						// parentJson
						JSONArray jsonArray = new JSONArray();
						parentJson.put(entry.getKey(), reconstructJson(entry.getValue(), jsonArray, jsonPath));
						// JSONObject childJson = new JSONObject();
						// childJson=reconstructJson(entry.getValue(),
						// childJson);
						// jsonArray.add(childJson);
						// parentJson.put(entry.getKey(),jsonArray );

						System.out.println("Array");
					} else {
						System.out.println("id at this stage" + id);
						parentJson.put(entry.getKey(), entry.getValue());
					}

					// jsonObject.put(entry.getKey(), entry.getValue());
				}

			}
			return parentJson;

		} else {

		}
		return parentJson;

		// jsonObject.putAll(jsonReconstruct);

	}

	// Method to store JSON Object in Redis Key Store in a nested fashion

	public Object reconstructJson(String id, Object object) {

		// Jedis jedis = getRedisConnection();
		Map<String, String> jsonReconstruct = null;
		Set jsonList = null;
		JSONObject parentJson = null;
		JSONArray parentArray = null;

		if (object instanceof JSONArray) {
			jsonList = jedis.smembers(id);
			parentArray = (JSONArray) object;

			for (Object entry : jsonList) {
				// System.out.println(stock);

				if (jedis.type(entry.toString()).equals("hash")) {
					System.out.println("id at this stage" + id);
					System.out.println("Jedis object type=" + jedis.type(entry.toString()));
					JSONObject childJson = new JSONObject();
					parentArray.add(reconstructJson(entry.toString(), childJson));

					System.out.println("object");
				} else if (jedis.type(entry.toString()).equals("set")) {
					System.out.println("id at this stage" + id);
					System.out.println("Jedis object type=" + jedis.type(entry.toString()));

					JSONArray jsonArray = new JSONArray();
					parentArray.add(reconstructJson(entry.toString(), jsonArray));

					System.out.println("Array");
				} else {
					System.out.println("id at this stage" + id);
					parentArray.add(entry.toString());
				}

			}
			return parentArray;

		} else if (object instanceof JSONObject) {
			jsonReconstruct = jedis.hgetAll(id);
			parentJson = (JSONObject) object;

			for (Map.Entry<String, String> entry : jsonReconstruct.entrySet()) {

				if (entry.getKey().equals("_id") && entry.getValue().equals(parentID)) {
					System.out.println("id at this stage" + id);
					parentJson.put(entry.getKey(), entry.getValue());
				} else {
					System.out.println("id at this stage" + id);
					System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());

					if (jedis.type(entry.getValue().toString()).equals("hash")) {
						System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));
						JSONObject childJson = new JSONObject();

						parentJson.put(entry.getKey(), reconstructJson(entry.getValue(), childJson));
						System.out.println("object");
					} else if (jedis.type(entry.getValue().toString()).equals("set")) {
						System.out.println("id at this stage" + id);
						System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));

						// parentJson
						JSONArray jsonArray = new JSONArray();
						parentJson.put(entry.getKey(), reconstructJson(entry.getValue(), jsonArray));
						// JSONObject childJson = new JSONObject();
						// childJson=reconstructJson(entry.getValue(),
						// childJson);
						// jsonArray.add(childJson);
						// parentJson.put(entry.getKey(),jsonArray );

						System.out.println("Array");
					} else {
						System.out.println("id at this stage" + id);
						parentJson.put(entry.getKey(), entry.getValue());
					}

					// jsonObject.put(entry.getKey(), entry.getValue());
				}

			}
			return parentJson;

		} else {

		}
		return parentJson;

		// jsonObject.putAll(jsonReconstruct);

	}

	// Method to convert the elements back to JSON Object
	public Object reconstructJson_no_string_operation(String id, Object object) {
		// Jedis jedis = getRedisConnection();
		Map<String, String> jsonReconstruct = null;
		Set jsonList = null;
		JSONObject parentJson = null;
		JSONArray parentArray = null;

		if (object instanceof JSONArray) {
			jsonList = jedis.smembers(id);
			parentArray = (JSONArray) object;

			for (Object entry : jsonList) {
				// System.out.println(stock);

				if (jedis.type(entry.toString()).equals("hash")) {
					System.out.println("Jedis object type=" + jedis.type(entry.toString()));
					JSONObject childJson = new JSONObject();
					parentArray.add(reconstructJson_no_string_operation(entry.toString(), childJson));

					// System.out.println("object");
				} else if (jedis.type(entry.toString()).equals("set")) {
					System.out.println("Jedis object type=" + jedis.type(entry.toString()));
					JSONArray jsonArray = new JSONArray();
					parentArray.add(reconstructJson_no_string_operation(entry.toString(), jsonArray));

					// System.out.println("Array");
				} else {
					parentArray.add(entry.toString());
				}

			}
			return parentArray;

		} else if (object instanceof JSONObject) {
			jsonReconstruct = jedis.hgetAll(id);
			parentJson = (JSONObject) object;

			for (Map.Entry<String, String> entry : jsonReconstruct.entrySet()) {
				// System.out.println("Key : " + entry.getKey() + " Value : " +
				// entry.getValue());

				if (jedis.type(entry.getValue().toString()).equals("hash")) {
					System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));
					JSONObject childJson = new JSONObject();

					parentJson.put(entry.getKey(), reconstructJson_no_string_operation(entry.getValue(), childJson));
					// System.out.println("object");
				} else if (jedis.type(entry.getValue().toString()).equals("set")) {
					System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));
					// parentJson
					JSONArray jsonArray = new JSONArray();
					parentJson.put(entry.getKey(), reconstructJson_no_string_operation(entry.getValue(), jsonArray));
					// JSONObject childJson = new JSONObject();
					// childJson=reconstructJson(entry.getValue(), childJson);
					// jsonArray.add(childJson);
					// parentJson.put(entry.getKey(),jsonArray );

					// System.out.println("Array");
				} else {

					System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));
					parentJson.put(entry.getKey(), entry.getValue());
				}

				// jsonObject.put(entry.getKey(), entry.getValue());

			}
			return parentJson;

		} else {

		}
		return parentJson;

		// jsonObject.putAll(jsonReconstruct);

	}

	public void recursionGetIds(String jsonTrial, String ParentUUID)
			throws JsonParseException, JsonMappingException, IOException {

		ObjectMapper om = new ObjectMapper();
		JsonNode node = om.readValue(jsonTrial, JsonNode.class);
		String firstLevel = null;

		Map<String, String> flatValues = new HashMap<String, String>();

		if (null == ParentUUID || ParentUUID.isEmpty()) {

			Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
			while (iteratorForId.hasNext()) {
				Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String, JsonNode>) iteratorForId.next();
				if (entryCheckForId.getKey().equals("_id") && !entryCheckForId.getValue().asText().isEmpty()
						&& !entryCheckForId.getValue().isObject() && !entryCheckForId.getValue().isArray()) {

					ParentUUID = entryCheckForId.getValue().asText();

				}
			}

			if (null == ParentUUID || ParentUUID.isEmpty()) {
				ParentUUID = getHash();
			}

		}

		Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

		// System.out.println("Jsontrial: " + js.toString());
		while (iterator.hasNext()) {
			Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) iterator.next();

			if (entry.getValue().isObject()) {

				firstLevel = ParentUUID + "_Object_" + getId(entry.getValue().toString());

				flatValues.put(entry.getKey(), firstLevel);
				// continue;
				// temporary skipping of below 2 lines
				// System.out.println("Object_key= " + entry.getKey());
				recursionGetIds(entry.getValue().toString(), firstLevel);
			} else if (entry.getValue().isArray()) {
				System.out.println("array");

				// setting the array in parent hashmap key as array_key and
				// value as array_uuid
				String ArrayKey = ParentUUID + "_" + "Array" + "_" + getHash();
				flatValues.put(entry.getKey(), ArrayKey);
				System.out.println("Array_key=    " + entry.getKey());

				for (JsonNode arrayElement : entry.getValue()) {
					if (arrayElement.isObject()) {

						String ObjectKey = ArrayKey + "_Object_" + getId(arrayElement.toString());
						// for iterating objects inside array and storing their
						// keys in array
						jedis.sadd(ArrayKey, ObjectKey);

						// recursively iterating objects inside array
						recursionGetIds(arrayElement.toString(), ObjectKey);

					} else {
						// if not an object then storing directly as element in
						// array
						jedis.sadd(ArrayKey, arrayElement.asText());

						System.out.println(arrayElement.toString());

					}
					// this is for iterating single element within array if it
					// is not an object and individual element
					// else {
					//
					// System.out.println("value= " + j.asText());
					// }
				}
				System.out.println("");
			} else {

				flatValues.put(entry.getKey(), entry.getValue().asText());

				System.out.println("inner_key=" + entry.getKey());
				System.out.println("value = " + entry.getValue().toString());
			}

		}

		jedis.hmset(ParentUUID, flatValues);

	}

	private String getId(String jsonTrial) throws JsonParseException, JsonMappingException, IOException {
		// TODO Auto-generated method stub
		
		ObjectMapper om = new ObjectMapper();
		JsonNode node = om.readValue(jsonTrial, JsonNode.class);
		String id = null;
		Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
		while (iteratorForId.hasNext()) {
			Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String, JsonNode>) iteratorForId.next();
			if (entryCheckForId.getKey().equals("_id") && !entryCheckForId.getValue().asText().isEmpty()
					&& !entryCheckForId.getValue().isObject() && !entryCheckForId.getValue().isArray()) {
				id = entryCheckForId.getValue().asText();
			}
		}
		if (null == id || id.isEmpty()) {
			id = getHash();
		}

		return id;
	}

	public Jedis getRedisConnection() {
		Jedis jedis = null;
		try {
			jedis = Application.pool.getResource();

		} finally {

		}
		return jedis;
	}

	public String getHash() {
		int hash = 7;
		String uuid = getNewUUID();
		for (int i = 0; i < uuid.length(); i++) {
			hash = hash * 31 + uuid.charAt(i);

		}
		if (hash < 0) {
			hash = hash * (-1);
		}

		return String.valueOf(hash);
	}

	private String getNewUUID() {
		return UUID.randomUUID().toString();
	}

	public JSONParser getJsonParser() {
		return new JSONParser();
	}

	public void recursion(String jsonTrial, String ParentUUID)
			throws JsonParseException, JsonMappingException, IOException {

		ObjectMapper om = new ObjectMapper();
		JsonNode node = om.readValue(jsonTrial, JsonNode.class);
		String firstLevel = ParentUUID;
		// create an initial map
		Map<String, String> flatValues = new HashMap<String, String>();
		// String firstLevel = getHash();

		Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
		// System.out.println("Jsontrial: " + js.toString());
		while (iterator.hasNext()) {
			Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) iterator.next();

			if (entry.getValue().isObject()) {

				firstLevel = ParentUUID + "_Object_" + getHash();

				flatValues.put(entry.getKey(), firstLevel);
				// continue;
				// temporary skipping of below 2 lines
				// System.out.println("Object_key= " + entry.getKey());
				recursion(entry.getValue().toString(), firstLevel);
			} else if (entry.getValue().isArray()) {
				System.out.println("array");

				// setting the array in parent hashmap key as array_key and
				// value as array_uuid
				String ArrayKey = ParentUUID + "_" + "Array" + "_" + getHash();
				flatValues.put(entry.getKey(), ArrayKey);
				System.out.println("Array_key=    " + entry.getKey());

				for (JsonNode arrayElement : entry.getValue()) {
					if (arrayElement.isObject()) {

						String ObjectKey = ArrayKey + "_Object_" + getHash();
						// for iterating objects inside array and storing their
						// keys in array
						jedis.sadd(ArrayKey, ObjectKey);

						// recursively iterating objects inside array
						recursion(arrayElement.toString(), ObjectKey);

					} else {
						// if not an object then storing directly as element in
						// array
						jedis.sadd(ArrayKey, arrayElement.asText());

						System.out.println(arrayElement.toString());

					}
					// this is for iterating single element within array if it
					// is not an object and individual element
					// else {
					//
					// System.out.println("value= " + j.asText());
					// }
				}
				System.out.println("");
			} else {

				flatValues.put(entry.getKey(), entry.getValue().asText());

				System.out.println("inner_key=" + entry.getKey());
				System.out.println("value = " + entry.getValue().toString());
			}

		}

		jedis.hmset(ParentUUID, flatValues);

	}
	
	
	
	

	public void secondRecursiveSubmit(String jsonTrial, String ParentUUID)
			throws JsonParseException, JsonMappingException, IOException {

		ObjectMapper om = new ObjectMapper();
		JsonNode node = om.readValue(jsonTrial, JsonNode.class);
		String firstLevel = null;

		Map<String, String> flatValues = new HashMap<String, String>();

		if (null == ParentUUID || ParentUUID.isEmpty()) {

			Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
			while (iteratorForId.hasNext()) {
				Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String, JsonNode>) iteratorForId.next();
				if (entryCheckForId.getKey().equals("_id") && !entryCheckForId.getValue().asText().isEmpty()
						&& !entryCheckForId.getValue().isObject() && !entryCheckForId.getValue().isArray()) {

					ParentUUID = entryCheckForId.getValue().asText();
				}
			}

			if (null == ParentUUID || ParentUUID.isEmpty()) {
				ParentUUID = getHash();
								
			}
		}

		Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

		// System.out.println("Jsontrial: " + js.toString());
		while (iterator.hasNext()) {
			Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) iterator.next();
			if (entry.getValue().isObject()) {
				String _id =getId(entry.getValue().toString());
				firstLevel = ParentUUID + "_" + entry.getKey().toString() + "_" + _id;
				flatValues.put(entry.getKey(), firstLevel);
				// continue;
				// temporary skipping of below 2 lines
				// System.out.println("Object_key= " + entry.getKey());
				secondRecursiveSubmit(entry.getValue().toString(), firstLevel);
			} else if (entry.getValue().isArray()) {
				System.out.println("array");

				// setting the array in parent hashmap key as array_key and
				// value as array_uuid
				String arrayKey = null;
				if (entry.getKey().toString() == null || entry.getKey().toString().equals("")) {
					arrayKey = ParentUUID + "_Array_" + getHash();
				} else {

					arrayKey = ParentUUID + "_" + entry.getKey().toString() + "_" + getHash();
				}

				flatValues.put(entry.getKey(), arrayKey);
				System.out.println("Array_key=    " + entry.getKey());

				for (JsonNode arrayElement : entry.getValue()) {
					if (arrayElement.isObject()) {

						String ObjectKey = arrayKey + "_" + entry.getKey().toString() + "_"
								+ getId(arrayElement.toString());
						// for iterating objects inside array and storing their
						// keys in array
						jedis.sadd(arrayKey, ObjectKey);

						// recursively iterating objects inside array
						secondRecursiveSubmit(arrayElement.toString(), ObjectKey);

					} else {
						// if not an object then storing directly as element in
						// array
						jedis.sadd(arrayKey, arrayElement.asText());

						System.out.println(arrayElement.toString());

					}
					// this is for iterating single element within array if it
					// is not an object and individual element
					// else {
					//
					// System.out.println("value= " + j.asText());
					// }
				}
				System.out.println("");
			} else {

				flatValues.put(entry.getKey(), entry.getValue().asText());

				System.out.println("inner_key=" + entry.getKey());
				System.out.println("value = " + entry.getValue().toString());
			}

		}
		if (!MapUtils.isEmpty(flatValues)) {
			jedis.hmset(ParentUUID, flatValues);
		}

	}
	


public void firstRecursiveSubmit(String jsonTrial, String ParentUUID,String identifier)
			throws JsonParseException, JsonMappingException, IOException {
		
		ObjectMapper om = new ObjectMapper();
		JsonNode node = om.readValue(jsonTrial, JsonNode.class);
		String firstLevel = null;

		Map<String, String> flatValues = new HashMap<String, String>();

		if (null == ParentUUID || ParentUUID.isEmpty()) {

			Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
			while (iteratorForId.hasNext()) {
				Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String, JsonNode>) iteratorForId.next();
				if (entryCheckForId.getKey().equals("_id") && !entryCheckForId.getValue().asText().isEmpty()
						&& !entryCheckForId.getValue().isObject() && !entryCheckForId.getValue().isArray()) {

					ParentUUID = entryCheckForId.getValue().asText();
				}
			}

			if (null == ParentUUID || ParentUUID.isEmpty()) {
				ParentUUID = getHash();
				identifier = ParentUUID;
				
			}
		}
		flatValues.put("_id", identifier);

		Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

		// System.out.println("Jsontrial: " + js.toString());
		while (iterator.hasNext()) {
			Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) iterator.next();
			if (entry.getValue().isObject()) {
				String _id =getId(entry.getValue().toString());
				firstLevel = ParentUUID + "_" + entry.getKey().toString() + "_" + _id;
				flatValues.put(entry.getKey(), firstLevel);
				
				// continue;
				// temporary skipping of below 2 lines
				// System.out.println("Object_key= " + entry.getKey());
				firstRecursiveSubmit(entry.getValue().toString(), firstLevel,_id);
			} else if (entry.getValue().isArray()) {
				System.out.println("array");

				// setting the array in parent hashmap key as array_key and
				// value as array_uuid
				String arrayKey = null;
//				if (entry.getKey().toString() == null || entry.getKey().toString().equals("")) {
					arrayKey = ParentUUID + "_" + getHash();
//				} else {

//					arrayKey = ParentUUID + "_" + entry.getKey().toString() + "_" + getHash();
//				}

				flatValues.put(entry.getKey(), arrayKey);
				System.out.println("Array_key=    " + entry.getKey());

				for (JsonNode arrayElement : entry.getValue()) {
					if (arrayElement.isObject()) {
						String _id =getId(arrayElement.toString());
						String ObjectKey = arrayKey + "_" + entry.getKey().toString() + "_"
								+ _id;
						// for iterating objects inside array and storing their
						// keys in array
						jedis.sadd(arrayKey, ObjectKey);

						// recursively iterating objects inside array
						firstRecursiveSubmit(arrayElement.toString(), ObjectKey,_id);

					} else {
						// if not an object then storing directly as element in
						// array
						jedis.sadd(arrayKey, arrayElement.asText());

						System.out.println(arrayElement.toString());

					}
					// this is for iterating single element within array if it
					// is not an object and individual element
					// else {
					//
					// System.out.println("value= " + j.asText());
					// }
				}
				System.out.println("");
			} else {

				flatValues.put(entry.getKey(), entry.getValue().asText());

				System.out.println("inner_key=" + entry.getKey());
				System.out.println("value = " + entry.getValue().toString());
			}

		}
		if (!MapUtils.isEmpty(flatValues)) {
			jedis.hmset(ParentUUID, flatValues);
		}

	}




public void firstRecursiveSubmit_optimized_keys(String jsonTrial, String ParentUUID,String identifier)
		throws JsonParseException, JsonMappingException, IOException {
	
	ObjectMapper om = new ObjectMapper();
	JsonNode node = om.readValue(jsonTrial, JsonNode.class);
	String firstLevel = null;

	Map<String, String> flatValues = new HashMap<String, String>();

	if (null == ParentUUID || ParentUUID.isEmpty()) {

		Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
		while (iteratorForId.hasNext()) {
			Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String, JsonNode>) iteratorForId.next();
			if (entryCheckForId.getKey().equals("_id") && !entryCheckForId.getValue().asText().isEmpty()
					&& !entryCheckForId.getValue().isObject() && !entryCheckForId.getValue().isArray()) {

				ParentUUID = entryCheckForId.getValue().asText();
			}
		}

		if (null == ParentUUID || ParentUUID.isEmpty()) {
			ParentUUID = getHash();
			identifier = "plan_"+ParentUUID;
			
		}
	}
	flatValues.put("_id", identifier);

	Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

	// System.out.println("Jsontrial: " + js.toString());
	while (iterator.hasNext()) {
		Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) iterator.next();
		if (entry.getValue().isObject()) {
			String _id =getId(entry.getValue().toString());
			firstLevel = ParentUUID + "_" + entry.getKey().toString() + "_" + _id;
			flatValues.put(entry.getKey(), _id);
			
			// continue;
			// temporary skipping of below 2 lines
			// System.out.println("Object_key= " + entry.getKey());
			firstRecursiveSubmit_optimized_keys(entry.getValue().toString(), _id,_id);
		} else if (entry.getValue().isArray()) {
			System.out.println("array");

			// setting the array in parent hashmap key as array_key and
			// value as array_uuid
			String arrayKey = null;
//			if (entry.getKey().toString() == null || entry.getKey().toString().equals("")) {
				arrayKey ="array_"+getHash();
//			} else {

//				arrayKey = ParentUUID + "_" + entry.getKey().toString() + "_" + getHash();
//			}

			flatValues.put(entry.getKey(), arrayKey);
			System.out.println("Array_key=    " + entry.getKey());

			for (JsonNode arrayElement : entry.getValue()) {
				if (arrayElement.isObject()) {
					String _id =getId(arrayElement.toString());
					String ObjectKey = arrayKey + "_" + entry.getKey().toString() + "_"
							+ _id;
					// for iterating objects inside array and storing their
					// keys in array
					jedis.sadd(arrayKey, _id);

					// recursively iterating objects inside array
					firstRecursiveSubmit_optimized_keys(arrayElement.toString(), _id,_id);

				} else {
					// if not an object then storing directly as element in
					// array
					jedis.sadd(arrayKey, arrayElement.asText());

					System.out.println(arrayElement.toString());

				}
				// this is for iterating single element within array if it
				// is not an object and individual element
				// else {
				//
				// System.out.println("value= " + j.asText());
				// }
			}
			System.out.println("");
		} else {

			flatValues.put(entry.getKey(), entry.getValue().asText());

			System.out.println("inner_key=" + entry.getKey());
			System.out.println("value = " + entry.getValue().toString());
		}

	}
	if (!MapUtils.isEmpty(flatValues)) {
		jedis.hmset(identifier, flatValues);
	}

}




}
