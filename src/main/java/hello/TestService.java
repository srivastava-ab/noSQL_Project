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
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.xml.ws.Response;

import org.apache.catalina.connector.Request;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.mapred.gethistory_jsp;
import org.apache.http.HttpRequest;
import org.apache.tomcat.util.codec.binary.StringUtils;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
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
import com.google.common.net.HttpHeaders;
import com.google.gson.JsonArray;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import redis.clients.jedis.Jedis;

@RestController
@RequestMapping(value = "/")
public class TestService {

	public Jedis jedis;
	public JSONParser jsonparser;
	private static boolean isMessageQueueInitialized = false;
	RabbitMQ rabbitMQ = null;

	// private final static String QUEUE_NAME = "task_queue";

	public TestService() throws IOException, TimeoutException {
		rabbitMQ = new RabbitMQ("uio");
		jedis = getRedisConnection();
		jsonparser = getJsonParser();

	}

	// public boolean initializeMsgQueue() {
	// if (!isMessageQueueInitialized) {
	// if (rabbitMQ.init()) {
	// if (rabbitMQ.declareMessageQueue(QUEUE_NAME)) {
	// isMessageQueueInitialized = true;
	// }
	// }
	// }
	// return isMessageQueueInitialized;
	// }

	@RequestMapping(value = "/medicalplans/{userId}", method = RequestMethod.DELETE)
	@ResponseBody
	public JSONObject deleteNestingById(@PathVariable String userId) {
		JSONObject jsonObject = null;
		String id = "plan_" + userId;
		if (!jedis.hgetAll(id).isEmpty()) {

			// return reconstructJson(id, parentJson);
			// jsonObject = (JSONObject) deleteJson(id, new JSONObject());
			jsonObject = deleteById(id);
			//delete message by queue
			String idToDelete= "delete"+"-"+id;
			rabbitMQ.postMessageToQueue(idToDelete);
			
			
			return jsonObject;
		} else {
			String idToDelete= "delete"+"-"+id;
			rabbitMQ.postMessageToQueue(idToDelete);
			jsonObject = new JSONObject();
			jsonObject.put("Message", "ID does not exist");
			return jsonObject;
		}
	}

	private JSONObject deleteById(String id) {

		HashSet<String> objectKeys = new HashSet<>();
		objectKeys.add(id);

		JSONObject parentJson = new JSONObject();
		// parentID = id;

		JSONObject returnJSON = (JSONObject) deleteJson(id, parentJson, objectKeys);

		if (null != objectKeys || !objectKeys.isEmpty()) {
			Iterator it = objectKeys.iterator();

			System.out.println("outputting keys");
			while (it.hasNext()) {
				String keyToDelete = it.next().toString();
				jedis.del(keyToDelete);
				System.out.println(keyToDelete);
			}

		}
		return returnJSON;

	}

	// @RequestMapping(value = "/test_nesting/jsonpath", method =
	// RequestMethod.POST)
	// @ResponseBody
	// public void test_nesting_json_path(@RequestBody JSONObject jsonObject)
	// throws JsonParseException, JsonMappingException, IOException,
	// JSONException {
	//
	// String json_string = jsonObject.toJSONString();
	//
	// JsonParser_temp jsonParser_temp = new JsonParser_temp(json_string);
	//
	// Iterator<String> itr = jsonParser_temp.getPathList().iterator();
	//
	// while (itr.hasNext()) {
	// String employee = itr.next();
	//
	// System.out.println(employee);
	//
	// }
	// }

	@RequestMapping(value = "/medicalplans", method = RequestMethod.POST)
	@ResponseBody
	public String test_nesting(@RequestBody JSONObject jsonObject, HttpServletRequest request,
			HttpServletResponse response)
			throws JsonParseException, JsonMappingException, IOException, ParseException, ProcessingException {

		String userToken = request.getHeader("Authorization");
		JSONObject jo = checkValidAccess(userToken);

		if (!jo.isEmpty() && (jo.get("role").equals("user") || jo.get("role").equals("admin"))) {
			String json_string = jsonObject.toJSONString();
			// JSONObject jsonSchemaObj = (JSONObject) jsonparser
			// .parse(new FileReader("src/main/resources/insurance_plan.json"));

			JSONObject jsonSchemaObj = getSchema();
			String status = ValidationUtils.isJsonValid(jsonSchemaObj.toString(), jsonObject.toString());
			if (status == "success") {
				String data = indexer_test(json_string, null, "");
				 

				rabbitMQ.postMessageToQueue(data);
				String[] onlyPlanId = data.split("_");
				return onlyPlanId[1];

			} else {
				return "Not Valid JSON";
			}

		} else {
			response.setStatus(401);
			return "Not Authorized";
		}

	}

	//////////////////////////////////////////////////////////////////////////////
	@RequestMapping(value = "/medicalplans/{userId}", method = RequestMethod.GET)
	@ResponseBody
	public JSONObject test_nesting(@PathVariable String userId, HttpServletResponse response,
			HttpServletRequest request) throws JsonParseException, JsonMappingException, IOException, ParseException {

		String id = "plan_" + userId;

		JSONObject parentJson = new JSONObject();

		String userToken = request.getHeader("Authorization");
		JSONObject jo = checkValidAccess(userToken);

//		if (!jo.isEmpty() && jo.get("id").equals(id) && 
		if (	(jo.get("role").equals("user") || jo.get("role").equals("admin"))) {

			// JSONObject actualJson = new JSONObject();
			HashSet<String> allKeys = new HashSet<>();
			parentID = id;
			parentJson = (JSONObject) indexer_test_get(id, parentJson, "", allKeys);

			StringBuilder sb = new StringBuilder();

			if (null != allKeys || !allKeys.isEmpty()) {
				Iterator it = allKeys.iterator();

				System.out.println("outputting keys");
				while (it.hasNext()) {
					String keyToDelete = it.next().toString();
					sb.append(keyToDelete);
					System.out.println(keyToDelete);
				}

			}
			String eTag = getHash(sb);

			jedis.set("eTag_" + id, eTag);

			if (null != jedis.hgetAll(id) && jedis.type(id).equals("hash")) {

				request.getHeaderNames();

				if (null == request.getHeader("eTag") || !request.getHeader("eTag").equals(eTag)) {
					response.setHeader("eTag", eTag);
					return parentJson;

				} else {
					parentJson = new JSONObject();
					parentJson.put("message", "not modified since");
					response.setHeader("eTag", eTag);
					response.setStatus(304);

				}

			}

			else {
				parentJson = new JSONObject();
				parentJson.put("message", "id does not exist");
				response.setStatus(404);
			}

			return parentJson;

		}

		else {

			response.setStatus(401);
			return parentJson;
		}

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

	@RequestMapping(value = "/medicalplans/{userId}", method = RequestMethod.PATCH)
	@ResponseBody
	public JSONObject test_nesting_lib_patch(@RequestBody JsonNode jsonNode, @PathVariable String userId)
			throws ParseException, JsonParseException, JsonMappingException, IOException, JsonPatchException,
			ProcessingException {
		String id = "plan_" + userId;
		JSONObject jsonObject = null;
		// convert received item to json node
		if (id.contains("plan_") && jedis.type(id).equals("hash")) {

			jsonObject = new JSONObject();
			final ObjectMapper mapper = new ObjectMapper();
			JsonNode node = mapper.convertValue(jsonNode, JsonNode.class);
			final JsonPatch patch = JsonPatch.fromJson(node);

			// get original JSON object and convert to json node based on parent
			// ID
			JSONObject originalJson = normal_get(id);
			JsonNode destiNode = mapper.convertValue(originalJson, JsonNode.class);

			// apply json patch on received JSON
			final JsonNode patched = patch.apply(destiNode);

			// add validate before deleting and adding a new object against the
			// schema

			JSONObject patchedJSONObject = mapper.convertValue(patched, JSONObject.class);

			// JSONObject jsonSchemaObj = (JSONObject) jsonparser
			// .parse(new FileReader("src/main/resources/insurance_plan.json"));

			JSONObject jsonSchemaObj = getSchema();

			String status = ValidationUtils.isJsonValid(jsonSchemaObj.toString(), patchedJSONObject.toString());
			if (status == "success") {

				deleteNestingById(id);

				HashSet<String> allKeys = new HashSet<>();
				// firstRecursiveSubmit_optimized_keys(patchedJSONObject.toString(),
				// null, "",allKeys);
				firstRecursiveSubmit_optimized_keys(patchedJSONObject.toString(), null, "");
				//String data = indexer_test(patchedJSONObject.toString(), null, "");
				JSONObject parentJson = new JSONObject();
				parentJson = (JSONObject) reconstructJsonEtag(id, parentJson, "", allKeys);

				StringBuilder sb = new StringBuilder();
				if (null != allKeys || !allKeys.isEmpty()) {
					Iterator it = allKeys.iterator();

					System.out.println("outputting keys");
					while (it.hasNext()) {
						String keyToDelete = it.next().toString();
						sb.append(keyToDelete);
						System.out.println(keyToDelete);
					}

				}
				String eTag = getHash(sb);

				jedis.set("eTag_" + id, eTag);

				// secondRecursiveSubmit(patchedJSONObject.toString(),"");
				// test_nesting(patchedJSONObject);
				// reconstructJsonEtag(id, object, jsonPath, allKeys)
				// }
				String idToDelete= "update"+"-"+id;
				rabbitMQ.postMessageToQueue(idToDelete);
			//	rabbitMQ.postMessageToQueue(jsonObject.toString());

				jsonObject.put("Message", id + " update successfully");

			} else {
				jsonObject.put("Message", "Invalid documen");
			}

		} else {
			jsonObject.put("Message", id + " not found");

		}

		return jsonObject;

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

				// if (entry.getKey().equals("_id") &&
				// entry.getValue().equals(parentID)) {
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

	public Object reconstructJsonEtag(String id, Object object, String jsonPath, HashSet<String> allKeys) {

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
					parentArray.add(reconstructJsonEtag(entry.toString(), childJson, jsonPath, allKeys));

					System.out.println("object");
				} else if (jedis.type(entry.toString()).equals("set")) {
					System.out.println("id at this stage" + id);
					System.out.println("Jedis object type=" + jedis.type(entry.toString()));

					JSONArray jsonArray = new JSONArray();
					parentArray.add(reconstructJsonEtag(entry.toString(), jsonArray, jsonPath, allKeys));

					System.out.println("Array");
				} else {
					allKeys.add(entry.toString());
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

				// if (entry.getKey().equals("_id") &&
				// entry.getValue().equals(parentID)) {
				if (entry.getKey().equals("id")) {
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
						parentJson.put(entry.getKey(),
								reconstructJsonEtag(entry.getValue(), childJson, jsonPath, allKeys));
						System.out.println("object");
					} else if (jedis.type(entry.getValue().toString()).equals("set")) {
						System.out.println("id at this stage" + id);
						System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));
						jsonPath = parentPath + "/" + entry.getKey();
						// parentJson
						JSONArray jsonArray = new JSONArray();
						parentJson.put(entry.getKey(),
								reconstructJsonEtag(entry.getValue(), jsonArray, jsonPath, allKeys));
						// JSONObject childJson = new JSONObject();
						// childJson=reconstructJson(entry.getValue(),
						// childJson);
						// jsonArray.add(childJson);
						// parentJson.put(entry.getKey(),jsonArray );

						System.out.println("Array");
					} else {
						allKeys.add(entry.getValue());
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

	
	public Object indexer_test_get(String id, Object object, String jsonPath, HashSet<String> allKeys) {

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
					parentArray.add(indexer_test_get(entry.toString(), childJson, jsonPath, allKeys));

					System.out.println("object");
				} else if (jedis.type(entry.toString()).equals("set")) {
					System.out.println("id at this stage" + id);
					System.out.println("Jedis object type=" + jedis.type(entry.toString()));

					JSONArray jsonArray = new JSONArray();
					parentArray.add(indexer_test_get(entry.toString(), jsonArray, jsonPath, allKeys));

					System.out.println("Array");
				} else {
					allKeys.add(entry.toString());
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

				// if (entry.getKey().equals("_id") &&
				// entry.getValue().equals(parentID)) {
				if (entry.getKey().equals("id")) {
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
						parentJson.put(entry.getKey(),
								indexer_test_get(entry.getValue(), childJson, jsonPath, allKeys));
						System.out.println("object");
					} else if (jedis.type(entry.getValue().toString()).equals("set")) {
						System.out.println("id at this stage" + id);
						System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));
						jsonPath = parentPath + "/" + entry.getKey();
						// parentJson
						JSONArray jsonArray = new JSONArray();
						parentJson.put(entry.getKey(),
								indexer_test_get(entry.getValue(), jsonArray, jsonPath, allKeys));
						// JSONObject childJson = new JSONObject();
						// childJson=reconstructJson(entry.getValue(),
						// childJson);
						// jsonArray.add(childJson);
						// parentJson.put(entry.getKey(),jsonArray );

						System.out.println("Array");
					} else {
						allKeys.add(entry.getValue());
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

				// if (entry.getKey().equals("_id") &&
				// entry.getValue().equals(parentID)) {
				if (entry.getKey().equals("id")) {
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

	public Object deleteJson(String id, Object object, HashSet<String> objectKeys) {

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
					objectKeys.add(entry.toString());
					parentArray.add(deleteJson(entry.toString(), childJson, objectKeys));

					System.out.println("object");
				} else if (jedis.type(entry.toString()).equals("set")) {
					System.out.println("id at this stage" + id);
					System.out.println("Jedis object type=" + jedis.type(entry.toString()));

					JSONArray jsonArray = new JSONArray();
					objectKeys.add(entry.toString());
					parentArray.add(deleteJson(entry.toString(), jsonArray, objectKeys));

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

				// if (entry.getKey().equals("_id") &&
				// entry.getValue().equals(parentID)) {
				if (entry.getKey().equals("id")) {
					System.out.println("id at this stage" + id);
					parentJson.put(entry.getKey(), entry.getValue());
				} else {
					System.out.println("id at this stage" + id);
					System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());

					if (jedis.type(entry.getValue().toString()).equals("hash")) {
						System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));
						JSONObject childJson = new JSONObject();
						objectKeys.add(entry.getValue().toString());
						parentJson.put(entry.getKey(), deleteJson(entry.getValue(), childJson, objectKeys));
						System.out.println("object");
					} else if (jedis.type(entry.getValue().toString()).equals("set")) {
						System.out.println("id at this stage" + id);
						System.out.println("Jedis object type=" + jedis.type(entry.getValue().toString()));

						// parentJson
						JSONArray jsonArray = new JSONArray();
						objectKeys.add(entry.getValue().toString());
						parentJson.put(entry.getKey(), deleteJson(entry.getValue(), jsonArray, objectKeys));
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
	
	private String getIdWithoutUnderScore(String jsonTrial) throws JsonParseException, JsonMappingException, IOException {
		// TODO Auto-generated method stub

		ObjectMapper om = new ObjectMapper();
		JsonNode node = om.readValue(jsonTrial, JsonNode.class);
		String id = null;
		Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
		while (iteratorForId.hasNext()) {
			Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String, JsonNode>) iteratorForId.next();
			if (entryCheckForId.getKey().equals("id") && !entryCheckForId.getValue().asText().isEmpty()
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

	public String getHash(StringBuilder plainString) {
		int hash = 7;

		for (int i = 0; i < plainString.length(); i++) {
			hash = hash * 31 + plainString.charAt(i);

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

	public void firstRecursiveSubmit(String jsonTrial, String ParentUUID, String identifier)
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
				String _id = getId(entry.getValue().toString());
				firstLevel = ParentUUID + "_" + entry.getKey().toString() + "_" + _id;
				flatValues.put(entry.getKey(), firstLevel);

				// continue;
				// temporary skipping of below 2 lines
				// System.out.println("Object_key= " + entry.getKey());
				firstRecursiveSubmit(entry.getValue().toString(), firstLevel, _id);
			} else if (entry.getValue().isArray()) {
				System.out.println("array");

				// setting the array in parent hashmap key as array_key and
				// value as array_uuid
				String arrayKey = null;
				// if (entry.getKey().toString() == null ||
				// entry.getKey().toString().equals("")) {
				arrayKey = ParentUUID + "_" + getHash();
				// } else {

				// arrayKey = ParentUUID + "_" + entry.getKey().toString() + "_"
				// + getHash();
				// }

				flatValues.put(entry.getKey(), arrayKey);
				System.out.println("Array_key=    " + entry.getKey());

				for (JsonNode arrayElement : entry.getValue()) {
					if (arrayElement.isObject()) {
						String _id = getId(arrayElement.toString());
						String ObjectKey = arrayKey + "_" + entry.getKey().toString() + "_" + _id;
						// for iterating objects inside array and storing their
						// keys in array
						jedis.sadd(arrayKey, ObjectKey);

						// recursively iterating objects inside array
						firstRecursiveSubmit(arrayElement.toString(), ObjectKey, _id);

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

	// public void secondRecursiveSubmit(String jsonTrial, String ParentUUID)
	// throws JsonParseException, JsonMappingException, IOException {
	//
	// ObjectMapper om = new ObjectMapper();
	// JsonNode node = om.readValue(jsonTrial, JsonNode.class);
	// String firstLevel = null;
	//
	// Map<String, String> flatValues = new HashMap<String, String>();
	//
	// if (null == ParentUUID || ParentUUID.isEmpty()) {
	//
	// Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
	// while (iteratorForId.hasNext()) {
	// Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String,
	// JsonNode>) iteratorForId.next();
	// if (entryCheckForId.getKey().equals("_id") &&
	// !entryCheckForId.getValue().asText().isEmpty()
	// && !entryCheckForId.getValue().isObject() &&
	// !entryCheckForId.getValue().isArray()) {
	//
	// ParentUUID = entryCheckForId.getValue().asText();
	// }
	// }
	//
	// if (null == ParentUUID || ParentUUID.isEmpty()) {
	// ParentUUID = getHash();
	//
	// }
	// }
	//
	// Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
	//
	// // System.out.println("Jsontrial: " + js.toString());
	// while (iterator.hasNext()) {
	// Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>)
	// iterator.next();
	// if (entry.getValue().isObject()) {
	// String _id =getId(entry.getValue().toString());
	// //firstLevel = ParentUUID + "_" + entry.getKey().toString() + "_" + _id;
	// flatValues.put(entry.getKey(), _id);
	// // continue;
	// // temporary skipping of below 2 lines
	// // System.out.println("Object_key= " + entry.getKey());
	// secondRecursiveSubmit(entry.getValue().toString(), _id);
	// } else if (entry.getValue().isArray()) {
	// System.out.println("array");
	//
	// // setting the array in parent hashmap key as array_key and
	// // value as array_uuid
	// String arrayKey = null;
	// // if (entry.getKey().toString() == null ||
	// entry.getKey().toString().equals("")) {
	// arrayKey = ParentUUID + "_Array_" + getHash();
	// // } else {
	//
	// // arrayKey = ParentUUID + "_" + entry.getKey().toString() + "_" +
	// getHash();
	// // }
	//
	// flatValues.put(entry.getKey(), arrayKey);
	// System.out.println("Array_key= " + entry.getKey());
	//
	// for (JsonNode arrayElement : entry.getValue()) {
	// if (arrayElement.isObject()) {
	//
	// String ObjectKey = arrayKey + "_" + entry.getKey().toString() + "_"
	// + getId(arrayElement.toString());
	// // for iterating objects inside array and storing their
	// // keys in array
	// jedis.sadd(arrayKey, ObjectKey);
	//
	// // recursively iterating objects inside array
	// secondRecursiveSubmit(arrayElement.toString(), ObjectKey);
	//
	// } else {
	// // if not an object then storing directly as element in
	// // array
	// jedis.sadd(arrayKey, arrayElement.asText());
	//
	// System.out.println(arrayElement.toString());
	//
	// }
	// // this is for iterating single element within array if it
	// // is not an object and individual element
	// // else {
	// //
	// // System.out.println("value= " + j.asText());
	// // }
	// }
	// System.out.println("");
	// } else {
	//
	// flatValues.put(entry.getKey(), entry.getValue().asText());
	//
	// System.out.println("inner_key=" + entry.getKey());
	// System.out.println("value = " + entry.getValue().toString());
	// }
	//
	// }
	// if (!MapUtils.isEmpty(flatValues)) {
	// jedis.hmset(ParentUUID, flatValues);
	// }
	//
	// }

	public String firstRecursiveSubmit_optimized_keys(String jsonTrial, String ParentUUID, String identifier)
			throws JsonParseException, JsonMappingException, IOException {

		ObjectMapper om = new ObjectMapper();
		JsonNode node = om.readValue(jsonTrial, JsonNode.class);
		String firstLevel = null;

		Map<String, String> flatValues = new HashMap<String, String>();

		if (null == ParentUUID || ParentUUID.isEmpty()) {

			Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
			while (iteratorForId.hasNext()) {
				Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String, JsonNode>) iteratorForId.next();
				if (entryCheckForId.getKey().equals("id") && !entryCheckForId.getValue().asText().isEmpty()
						&& !entryCheckForId.getValue().isObject() && !entryCheckForId.getValue().isArray()) {

					ParentUUID = entryCheckForId.getValue().asText();
				}
			}

			if (null == ParentUUID || ParentUUID.isEmpty()) {
				ParentUUID = getHash();
				identifier = "plan_" + ParentUUID;

			}

			if (ParentUUID.toString().contains("plan_")) {
				identifier = ParentUUID;
			}
		}
		flatValues.put("id", identifier);

		Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

		// System.out.println("Jsontrial: " + js.toString());
		while (iterator.hasNext()) {
			Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) iterator.next();
			if (entry.getValue().isObject()) {
				String _id = getId(entry.getValue().toString());
				firstLevel = ParentUUID + "_" + entry.getKey().toString() + "_" + _id;
				flatValues.put(entry.getKey(), _id);

				// continue;
				// temporary skipping of below 2 lines
				// System.out.println("Object_key= " + entry.getKey());
				firstRecursiveSubmit_optimized_keys(entry.getValue().toString(), _id, _id);
			} else if (entry.getValue().isArray()) {
				System.out.println("array");

				// setting the array in parent hashmap key as array_key and
				// value as array_uuid
				String arrayKey = null;
				// if (entry.getKey().toString() == null ||
				// entry.getKey().toString().equals("")) {
				arrayKey = "array_" + getHash();
				// } else {

				// arrayKey = ParentUUID + "_" + entry.getKey().toString() + "_"
				// + getHash();
				// }

				flatValues.put(entry.getKey(), arrayKey);
				System.out.println("Array_key=    " + entry.getKey());

				for (JsonNode arrayElement : entry.getValue()) {
					if (arrayElement.isObject()) {
						String _id = getId(arrayElement.toString());
						String ObjectKey = arrayKey + "_" + entry.getKey().toString() + "_" + _id;
						// for iterating objects inside array and storing their
						// keys in array
						jedis.sadd(arrayKey, _id);

						// recursively iterating objects inside array
						firstRecursiveSubmit_optimized_keys(arrayElement.toString(), _id, _id);

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
		return identifier;

	}

	public void firstRecursiveSubmit_optimized_keys(String jsonTrial, String ParentUUID, String identifier,
			HashSet<String> loadNewHash) throws JsonParseException, JsonMappingException, IOException {

		ObjectMapper om = new ObjectMapper();
		JsonNode node = om.readValue(jsonTrial, JsonNode.class);
		String firstLevel = null;

		Map<String, String> flatValues = new HashMap<String, String>();

		if (null == ParentUUID || ParentUUID.isEmpty()) {

			Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
			while (iteratorForId.hasNext()) {
				Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String, JsonNode>) iteratorForId.next();
				if (entryCheckForId.getKey().equals("id") && !entryCheckForId.getValue().asText().isEmpty()
						&& !entryCheckForId.getValue().isObject() && !entryCheckForId.getValue().isArray()) {

					ParentUUID = entryCheckForId.getValue().asText();
				}
			}

			if (null == ParentUUID || ParentUUID.isEmpty()) {
				ParentUUID = getHash();
				identifier = "plan_" + ParentUUID;

			}

			if (ParentUUID.toString().contains("plan_")) {
				identifier = ParentUUID;

			}
		}

		flatValues.put("id", identifier);

		Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

		// System.out.println("Jsontrial: " + js.toString());
		while (iterator.hasNext()) {
			Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) iterator.next();
			if (entry.getValue().isObject()) {
				String _id = getId(entry.getValue().toString());
				firstLevel = ParentUUID + "_" + entry.getKey().toString() + "_" + _id;
				flatValues.put(entry.getKey(), _id);

				// continue;
				// temporary skipping of below 2 lines
				// System.out.println("Object_key= " + entry.getKey());
				firstRecursiveSubmit_optimized_keys(entry.getValue().toString(), _id, _id, loadNewHash);
			} else if (entry.getValue().isArray()) {
				System.out.println("array");

				// setting the array in parent hashmap key as array_key and
				// value as array_uuid
				String arrayKey = null;
				// if (entry.getKey().toString() == null ||
				// entry.getKey().toString().equals("")) {
				arrayKey = "array_" + getHash();
				// } else {

				// arrayKey = ParentUUID + "_" + entry.getKey().toString() + "_"
				// + getHash();
				// }

				flatValues.put(entry.getKey(), arrayKey);
				System.out.println("Array_key=    " + entry.getKey());

				for (JsonNode arrayElement : entry.getValue()) {
					if (arrayElement.isObject()) {
						String _id = getId(arrayElement.toString());
						String ObjectKey = arrayKey + "_" + entry.getKey().toString() + "_" + _id;
						// for iterating objects inside array and storing their
						// keys in array
						jedis.sadd(arrayKey, _id);

						// recursively iterating objects inside array
						firstRecursiveSubmit_optimized_keys(arrayElement.toString(), _id, _id, loadNewHash);

					} else {

						loadNewHash.add(arrayElement.toString());
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

				if (!entry.getKey().equals("id")) {

					loadNewHash.add(entry.getValue().asText());
				}
				flatValues.put(entry.getKey(), entry.getValue().asText());
				System.out.println("inner_key=" + entry.getKey());
				System.out.println("value = " + entry.getValue().toString());
			}

		}
		if (!MapUtils.isEmpty(flatValues)) {
			jedis.hmset(identifier, flatValues);
		}
	}

	@ExceptionHandler({ org.springframework.http.converter.HttpMessageNotReadableException.class })
	@RequestMapping(value = "/medicalplans/getToken", method = RequestMethod.POST)
	@ResponseBody
	public String getToken(@Valid @RequestBody(required = false) JSONObject jsonObject, HttpServletResponse response)
			throws JSONException, FileNotFoundException {

		if (null != jsonObject) {

			String token = Encryptor.encryptData(jsonObject.toString());

			// Iterator<String> itr = jsonParser_temp.getPathList().iterator();

			return token;

		} else {
			response.setStatus(400);
			return null;
		}

	}

	public JSONObject checkValidAccess(String token)
			throws JsonParseException, JsonMappingException, IOException, ParseException {
		JSONObject jo = new JSONObject();
		if (null != token && (token.length() % 2) == 0) {

			String json = Encryptor.decryptData(token);
			JSONParser jp = new JSONParser();

			jo = (JSONObject) jp.parse(json);

			String role = (String) jo.get("role");
			String id = (String) jo.get("id");
			// System.out.println(id + role);

		}

		return jo;

	}

	@RequestMapping(value = "/schema", method = RequestMethod.POST)
	@ResponseBody
	public String postSchema(@RequestBody JSONObject jsonObject, HttpServletRequest request,
			HttpServletResponse response) throws JsonParseException, JsonMappingException, IOException, ParseException {

		jedis.set("json_schema", jsonObject.toString());

		return "json_schema";

	}

	@RequestMapping(value = "/schema", method = RequestMethod.GET)
	@ResponseBody
	public JSONObject getSchema() throws JsonParseException, JsonMappingException, IOException, ParseException {
		JSONParser jsonparser = getJsonParser();
		JSONObject json = (JSONObject) jsonparser.parse(jedis.get("json_schema"));
		return json;

	}

	
	
	public String indexer_test(String jsonTrial, String ParentUUID, String identifier)
			throws JsonParseException, JsonMappingException, IOException {

		ObjectMapper om = new ObjectMapper();
		JsonNode node = om.readValue(jsonTrial, JsonNode.class);
		String firstLevel = null;

		Map<String, String> flatValues = new HashMap<String, String>();

		if (null == ParentUUID || ParentUUID.isEmpty()) {

			Iterator<Map.Entry<String, JsonNode>> iteratorForId = node.fields();
			while (iteratorForId.hasNext()) {
				Map.Entry<String, JsonNode> entryCheckForId = (Map.Entry<String, JsonNode>) iteratorForId.next();
				if (entryCheckForId.getKey().equals("id") && !entryCheckForId.getValue().asText().isEmpty()
						&& !entryCheckForId.getValue().isObject() && !entryCheckForId.getValue().isArray()) {

					ParentUUID = entryCheckForId.getValue().asText();
				}
			}

			if (null == ParentUUID || ParentUUID.isEmpty()) {
				ParentUUID = getHash();
				identifier = "plan_" + ParentUUID;

			}

			if (ParentUUID.toString().contains("plan_")) {
				identifier = ParentUUID;
			}
		}
		flatValues.put("id", identifier);

		Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();

		// System.out.println("Jsontrial: " + js.toString());
		while (iterator.hasNext()) {
			Map.Entry<String, JsonNode> entry = (Map.Entry<String, JsonNode>) iterator.next();
			if (entry.getValue().isObject()) {
				String _id = getIdWithoutUnderScore(entry.getValue().toString());
				firstLevel = ParentUUID + "_" + entry.getKey().toString() + "_" + _id;
				flatValues.put(entry.getKey(), _id);

				// continue;
				// temporary skipping of below 2 lines
				// System.out.println("Object_key= " + entry.getKey());
				indexer_test(entry.getValue().toString(), _id, _id);
			} else if (entry.getValue().isArray()) {
				System.out.println("array");

				// setting the array in parent hashmap key as array_key and
				// value as array_uuid
				String arrayKey = null;
				// if (entry.getKey().toString() == null ||
				// entry.getKey().toString().equals("")) {
				arrayKey = "array_" + getHash();
				// } else {

				// arrayKey = ParentUUID + "_" + entry.getKey().toString() + "_"
				// + getHash();
				// }

				flatValues.put(entry.getKey(), arrayKey);
				System.out.println("Array_key=    " + entry.getKey());

				for (JsonNode arrayElement : entry.getValue()) {
					if (arrayElement.isObject()) {
						String _id = getId(arrayElement.toString());
						String ObjectKey = arrayKey + "_" + entry.getKey().toString() + "_" + _id;
						// for iterating objects inside array and storing their
						// keys in array
						jedis.sadd(arrayKey, _id);

						// recursively iterating objects inside array
						indexer_test(arrayElement.toString(), _id, _id);

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
		return identifier;

	}
	
}
