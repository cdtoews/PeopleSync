package edu.mit.ist.salesforce.sync;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class Persons {

	String myLog;
	public static final String TEST_JSON_STRING;
	
	static{
		TEST_JSON_STRING =  "{"
				+ "    \"kerberosId\": \"wwhite\","
				+ "    \"givenName\": \"Walter\","
				+ "    \"familyName\": \"White\","
				+ "    \"middleName\": \"Danger\","
				+ "    \"displayName\": \"Walter White\","
				+ "    \"email\": \"wwhite@mit.edu\","
				+ "    \"phoneNumber\": \"617-252-1906\","
				+ "    \"website\": \"http://www.broadinstitute.org\","
				+ "    \"affiliations\": ["
				+ "      {"
				+ "        \"type\": \"staff\","
				+ "        \"title\": \"Professor\","
				+ "        \"office\": \"NE30-6013\","
				+ "            \"Primary\": \"Yes\","
				+ "        \"departments\": ["
				+ "          {"
				+ "            \"orgUnitId\": \"10000429\","
				+ "            \"name\": \"Department of Biology\""
				+ "          },"
				+ "{"
				+ "            \"orgUnitId\": \"10000887\","
				+ "            \"name\": \"Department of Radiology\""
				+ "          }"
				+ ""
				+ "        ]"
				+ "      },"
				+ "                    {"
				+ "        \"type\": \"staff\","
				+ "        \"title\": \"Professor\","
				+ "        \"office\": \"NW30-5548\","
				+ "            \"Primary\": \"No\","
				+ "        \"departments\": ["
				+ "          {"
				+ "            \"orgUnitId\": \"10000129\","
				+ "            \"name\": \"Department of Chemistry\""
				+ "          }"
				+ "        ]"
				+ "      }"
				+ "    ]"
				+ "  }";
	}
	
	public static void main(String[] args) throws UnirestException{
		Persons me = new Persons();
		me.getPerson("elander");
		me.getPerson("ctoews");
		
		JSONObject testJson = new JSONObject(TEST_JSON_STRING);
		Person testPerson = parsePersonJson(testJson);
		System.out.print(testPerson.toString());
	}
	
	public Persons(){
		myLog = "";
	}
	
	public Person getPerson(String kerbID) {
		if(kerbID == null){
			return null;
		}
		kerbID = kerbID.toLowerCase();
		try{
			
			HttpResponse<String> response = Unirest.get("https://mit-public.cloudhub.io/people/v3/people/"  + kerbID)
					  //.header("authorization", "Basic NmJmMjhjMGZlN2Y3NGEzYWJlZmRkZWYyYzQ5ZDljMzc6OWQxYjA0ZDgwNDczNDEzMzgxMEZEQTA2Q0M0MUMxNjM=")
					  .header("client_id", "6bf28c0fe7f74a3abefddef2c49d9c37")
					  .header("client_secret", "9d1b04d804734133810FDA06CC41C163")
					  .header("cache-control", "no-cache")
					  //.header("postman-token", "bbb7ce03-8aee-fef1-97c6-a137796366e4")
					  .asString();
			JSONObject responeJson = new JSONObject(response.getBody());
			JSONObject personJson = responeJson.getJSONObject("item");
			Person person = parsePersonJson(personJson);
			System.out.print(person.toString());
			//System.out.println("\nRESPONSE FOR " + kerbID + "\n" + response.getBody());
		}catch(UnirestException ex){
			logThis(" TASK=READING_PEOPLE_API STATUS=EXCEPTION EXCEPTION_TYPE=UNIREST KERBID=" + kerbID);
		}
		

		return null;
	}//end of getPerson
	
	public static Person parsePersonJson(JSONObject personJson){
		if(personJson == null){
			return null;
		}
		
		String kerbID      = personJson.getString("kerberosId");
		String firstName   = personJson.optString("givenName"  , null);
		String lastName    = personJson.optString("familyName" , null);
		String middleName  = personJson.optString("middleName" , null);
		String displayName = personJson.optString("displayName", null);
		String email       = personJson.optString("email"      , null);
		String phoneNumber = personJson.optString("phoneNumber", null);
		String website     = personJson.optString("website"    , null);
		
		Person person = new Person(kerbID,firstName,middleName,lastName,displayName,email,phoneNumber,website);
		
		JSONArray affsJson = personJson.optJSONArray("affiliations");
		if(affsJson == null){
			//if no affiliations, person is complete
			return person;
		}
		
		//we have at least 1 affiliation if we are here. iterate affiliations
		for(int i=0;i<affsJson.length();i++){
			JSONObject affJson = affsJson.getJSONObject(i);
			String type   = affJson.optString("type"  , null);
			String title  = affJson.optString("title" , null);
			String office = affJson.optString("office", null);
			Affiliation aff = new Affiliation(type,title,office);
			JSONArray deptsJson = affJson.optJSONArray("departments");
			if(deptsJson != null){
				//we have at least 1 dept
				for(int j=0;j<deptsJson.length();j++){
					JSONObject deptJson = deptsJson.getJSONObject(j);
					try{
						String orgUnitId = deptJson.getString("orgUnitId");
						String name      = deptJson.optString("name", null);
						Department dept  = new Department(orgUnitId,name);
						aff.addDept(dept);
					}catch(Exception ex){
						//if orgUnitID didn't have a value, walk away
					}
				}//end of for(deptsJson
			}//end of if(deptsJson not null
			person.addAffiliation(aff);
		}//end of for(affsJson
		
		
		
		return person;
		
	}//end of parsePersonJson
	
	public String getMyLog() {
		return myLog;
	}

	public String getMyLog(int sizeLimit){
		String result = myLog;
		if(result.length() > Main.MAX_LOG_SIZE){
			result = result.substring(0, sizeLimit);
		}
		return result;
	}


	private void logThis(String whatToWrite){
		myLog += Main.writeLog(whatToWrite) + "\n";
	}
	
	private void logThis(Department dept,String whatToWrite){
		myLog += Main.writeLog(dept, whatToWrite) + "\n";
	}
	
	
	
	
}
