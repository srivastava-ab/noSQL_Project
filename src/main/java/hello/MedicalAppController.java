package hello;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;

@RestController
@RequestMapping(value = "/new")
public class MedicalAppController {

	public Jedis jedis;
	public JSONParser jsonparser;

	public MedicalAppController() {

		jedis = getRedisConnection();
		jsonparser = getJsonParser();

	}

	@RequestMapping(value = "/submit", method = RequestMethod.POST)
	@ResponseBody
	public void test_nesting(@RequestBody org.json.simple.JSONObject jsonObject)
			throws JsonParseException, JsonMappingException, IOException, JSONException, ParseException {

		readObject(new JSONObject(jsonObject.toString()), "");
	}

	private void readObject(JSONObject object, String ParentUUID) throws JSONException {

		object.toString();
		Map<String, String> flatValues = new HashMap<String, String>();
		Iterator<String> keysItr = object.keys();

		if (null == ParentUUID || ParentUUID.isEmpty()) {
			ParentUUID = getHash();
			flatValues.put("_id", ParentUUID);
		} else {

			flatValues.put("_id", ParentUUID);
		}

		while (keysItr.hasNext()) {
			String key = keysItr.next();
			Object value = object.get(key);

			if (value instanceof JSONArray) {
				String array_id = getHash();
				flatValues.put(key, array_id);
				readArray((JSONArray) value, array_id);

			} else if (value instanceof JSONObject) {
				String json_id = getHash();
				flatValues.put(key, json_id);
				readObject((JSONObject) value, json_id);

			} else {

				flatValues.put(key, value.toString());

			}
		}

		jedis.hmset(ParentUUID, flatValues);
	}

	private void readArray(JSONArray array, String array_id) throws JSONException {

		for (int i = 0; i < array.length(); i++) {
			Object value = array.get(i);

			if (value instanceof JSONArray) {
				readArray((JSONArray) value, array_id);

			} else if (value instanceof JSONObject) {

				String json_id = getHash();
				jedis.sadd(array_id, json_id);
				readObject((JSONObject) value, json_id);

			} else {
				jedis.sadd(array_id, value.toString());

			}
		}
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

	public Jedis getRedisConnection() {
		Jedis jedis = null;
		try {
			jedis = Application.pool.getResource();

		} finally {

		}
		return jedis;
	}

}
