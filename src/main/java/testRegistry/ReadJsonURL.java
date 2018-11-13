package testRegistry;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ReadJsonURL {
    
    public static String getJsonStringFromURI(String uri) {
        JSONParser parser = new JSONParser();
        String strTrade=null;

        try {         
        	SSLUtilities.trustAllHostnames();
        	SSLUtilities.trustAllHttpsCertificates();
            URL endpoint = new URL(uri); // URL to Parse
            URLConnection yc = endpoint.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
            
            String inputLine;
            while ((inputLine = in.readLine()) != null) {               
                //JSONArray a = (JSONArray) parser.parse(inputLine);
                JSONObject job=(JSONObject) parser.parse(inputLine);
                JSONArray joba= (JSONArray) job.get("results");
                JSONObject jobao=(JSONObject)joba.get(0);
                JSONObject jobaoo=(JSONObject) jobao.get("vwtrade");
                // Loop through each item
strTrade=jobaoo.toJSONString();
            }
            in.close();
           
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
		return strTrade;   
    }   
}