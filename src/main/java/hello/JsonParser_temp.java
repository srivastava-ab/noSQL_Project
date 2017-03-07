package hello;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public class JsonParser_temp {

    private List<String> pathList;
    private String json;

    public JsonParser_temp(String json) throws JSONException {
        this.json = json;
        this.pathList = new ArrayList<String>();
        setJsonPaths(json);
    }

    public List<String> getPathList() {
        return this.pathList;
    }

    private void setJsonPaths(String json) throws JSONException {
        this.pathList = new ArrayList<String>();
        JSONObject object = new JSONObject(json);
        String jsonPath = "";
        if(json != JSONObject.NULL) {
            readObject(object, jsonPath);
        }   
    }

    private void readObject(JSONObject object, String jsonPath) throws JSONException {
        Iterator<String> keysItr = object.keys();
        String parentPath = jsonPath;
        while(keysItr.hasNext()) {
            String key = keysItr.next();
            Object value = object.get(key);
            jsonPath = parentPath + "/" + key;

            if(value instanceof JSONArray) {            
                readArray((JSONArray) value, jsonPath);
            }
            else if(value instanceof JSONObject) {
                readObject((JSONObject) value, jsonPath);
            } else { // is a value
                this.pathList.add(jsonPath);    
            }          
        }  
    }

    private void readArray(JSONArray array, String jsonPath) throws JSONException {      
        String parentPath = jsonPath;
        for(int i = 0; i <  array.length(); i++) {
            Object value = array.get(i);        
            jsonPath = parentPath + "/" + i;

            if(value instanceof JSONArray) {
                readArray((JSONArray) value, jsonPath);
            } else if(value instanceof JSONObject) {                
                readObject((JSONObject) value, jsonPath);
            } else { // is a value
                this.pathList.add(jsonPath);
            }       
        }
    }

}