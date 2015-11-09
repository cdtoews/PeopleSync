package edu.mit.ist.salesforce.sync;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class Departments {
	
	public static final String SF_ACCOUNTS_SQL = "select * from sftest1.account where from_api__c = true and active__c = true and orgunitid__c is not null";
	public static final String SF_DEACTIVEATE_ACCOUNT_SQL = "update sftest1.account set active__c = false, inactive_date__c = current_date where sfid = ?";
	public static final String INSERT_ACCOUNT_SQL = 
												"insert into sftest1.account \n" +
												"(name,orgunitid__c, active_date__c, active__c, from_api__c) \n" +
												"values \n" +
												"(?, ?, current_date, true, true) \n";
	
	public static final String UPDATE_ACCOUNT_SQL = "update sftest1.account set name = ? where sfid like ?";
	
	TreeMap<String, Department> apiMap ;
	TreeMap<String, Department> sfMap;
	TreeMap<String, Department> addMap;
	Connection conn;
	
	public static void main(String[] args) throws URISyntaxException, SQLException{
		Departments me = new Departments();
		me.loadAPIdepts();
		System.out.println("\n\n------ API Map ------\n\n");
		System.out.print(listDepts(me.apiMap));
		
		me.loadSFdepts();
		System.out.println("\n\n------ SF  Map ------\n\n");
		System.out.print(listDepts(me.sfMap));
		//TryThisAtHome();
	}
	
	
	public Departments() throws URISyntaxException, SQLException{
		conn = Main.getConnection();
		apiMap  = new TreeMap<String, Department>();
		sfMap   = new TreeMap<String, Department>();
		addMap  = new TreeMap<String, Department>();
		
	}
	
	public void loadData(){
		loadAPIdepts();
		loadSFdepts();
	}
	
	public void CompareUpdate(){
		compareMaps();
		deactivateDepts();
		insertDepts();
		updateDepts();
	}
		

	private  void loadAPIdepts(){

		Main.writeLog(" TASK=LOAD_API_DATA STATUS=STARTING");
		try {
			HttpResponse<String> response = Unirest.get("https://mit-public.cloudhub.io/departments/v1/departments")
					  .header("authorization", "Basic NmJmMjhjMGZlN2Y3NGEzYWJlZmRkZWYyYzQ5ZDljMzc6OWQxYjA0ZDgwNDczNDEzMzgxMEZEQTA2Q0M0MUMxNjM=")
					  .header("client_id", "6bf28c0fe7f74a3abefddef2c49d9c37")
					  .header("client_secret", "9d1b04d804734133810FDA06CC41C163")
					  .header("cache-control", "no-cache")
					  .header("postman-token", "e52a374d-1022-49d3-6f1a-ed7bcb6aa4e1")
					  .asString();
			
			JSONObject responeJson = new JSONObject(response.getBody());
            int listSize = responeJson.getJSONObject("metadata").getInt("size");
			
           
            JSONArray jsonArray = responeJson.getJSONArray("items");

            
            for (int i=0;i<jsonArray.length();i++){
            	apiMap.put(jsonArray.getJSONObject(i).getString("orgUnitId"), 
            			new Department(jsonArray.getJSONObject(i).getString("orgUnitId"),
            					jsonArray.getJSONObject(i).getString("name") )
            			);
            	
            	//System.out.println("orgUnitId : "+jsonArray.getJSONObject(i).getString("orgUnitId"));
                //System.out.println("name : "+jsonArray.getJSONObject(i).getString("name"));
            }
            Main.writeLog(" TASK=LOAD_API_DATA COUNT=" + listSize);
            if(apiMap.size() != listSize){
            	Main.writeLog(" TASK=LOAD_API_DATA STATUS=ERROR");
            	System.exit(1);
            }
			
		} catch (UnirestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Main.writeLog(" TASK=LOAD_API_DATA STATUS=FINISHED");
	}
	
	
	private  void loadSFdepts(){
		try {
			Main.writeLog(" TASK=LOAD_SF_DATA STATUS=STARTING");
			PreparedStatement readPS = conn.prepareStatement(SF_ACCOUNTS_SQL);
			ResultSet readRS = readPS.executeQuery();
			while(readRS.next()){
				//System.out.println(readRS.getString("sfid") + "  " + readRS.getString("name"));
				sfMap.put(readRS.getString("orgunitid__c"), 
            			new Department(readRS.getString("orgunitid__c"),
            				readRS.getString("name"),
            				readRS.getString("sfid"))
            			);
			}
			
			readRS.close();
			readPS.close();
			Main.writeLog(" TASK=LOAD_SF_DATA COUNT=" + sfMap.size());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Main.writeLog(" TASK=LOAD_SF_DATA STATUS=FINISHED");
	}
	
	
	private void compareMaps(){
		Main.writeLog(" TASK=COMPARE_DATA STATUS=STARTING");
		Iterator<String> it = apiMap.keySet().iterator();
		while(it.hasNext()){
			String apiID = it.next();
			Department apiDept = apiMap.get(apiID);
			
			if(sfMap.containsKey(apiID)){
				//there is an entry in SF with the orgunitid
				Department sfDept = sfMap.get(apiID);
				if(sfDept.getName().equals(apiDept.getName())){
					//everything matches, remove from both maps
					it.remove();
					sfMap.remove(apiID);
				}else{
					//set sfID for the dept so we can use it to update later
					apiDept.setSfID(sfMap.get(apiID).getSfID());
					sfMap.remove(apiID);
				}
			}else{
				//there is not an entry in SF, NEW ACCOUNT
				addMap.put(apiID, apiDept);
				it.remove();
			}
			
		}
		Main.writeLog(" TASK=COMPARE_DATA STATUS=FINISHED ITEMS_TO_ADD=" + addMap.size() + " ITEMS_TO_DEACTIVATE=" + sfMap.size() + " ITEMS_TO_UPDATE=" + apiMap.size());
	}//end of compareMaps
	
	private void deactivateDepts() {
		Main.writeLog(" TASK=DEACTIVATING_ACCOUNTS STATUS=STARTING");
		try {
			PreparedStatement removePS = conn.prepareStatement(SF_DEACTIVEATE_ACCOUNT_SQL);
			Iterator<String> it = sfMap.keySet().iterator();
			int updatedCount = 0;
			while(it.hasNext()){
				updatedCount++;
				String thisID = it.next();
				removePS.clearParameters();
				removePS.setString(1, sfMap.get(thisID).getSfID());
				int numUpdated = removePS.executeUpdate();
				if(numUpdated == 1){
					Main.writeLog(" TASK=DEACTIVATING_ACCOUNT STATUS=SUCCESS ORGUNITID=" + thisID);
				}else{
					Main.writeLog(" TASK=DEACTIVATING_ACCOUNT STATUS=ERROR ORGUNITID=" + thisID + " UPDATE_COUNT=" + numUpdated);
				}
				
			}
			Main.writeLog(" TASK=DEACTIVATING_ACCOUNTS STATUS=FINISHED COUNT=" + updatedCount);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			Main.writeLog(" TASK=DEACTIVATING_ACCOUNTS STATUS=EXCEPTION");
			e.printStackTrace();
		}
		
	}//end of deactivateDepts
	
	private void insertDepts(){
		Main.writeLog(" TASK=INSERTING_ACCOUNTS STATUS=STARTING");
		int updatedCount = 0;
		try{
			PreparedStatement insertPS = conn.prepareStatement(INSERT_ACCOUNT_SQL);
			Iterator<String> it = addMap.keySet().iterator();
			while(it.hasNext()){
				updatedCount ++;
				String thisID = it.next();
				String thisName = addMap.get(thisID).getName();
				insertPS.clearParameters();
				int c =1;
				insertPS.setString(c++, thisName);
				insertPS.setString(c++, thisID);
				int numUpdated = insertPS.executeUpdate();
				if(numUpdated == 1){
					Main.writeLog(" TASK=INSERTING_ACCOUNT STATUS=SUCCESS ORGUNITID=" + thisID);
				}else{
					Main.writeLog(" TASK=INSERTING_ACCOUNT STATUS=ERROR ORGUNITID=" + thisID + " UPDATE_COUNT=" + numUpdated);
				}
			}
			Main.writeLog(" TASK=INSERTING_ACCOUNTS STATUS=FINISHED COUNT=" + updatedCount);
			
		}catch(SQLException ex){
			Main.writeLog(" TASK=INSERTING_ACCOUNTS STATUS=EXCEPTION");
			ex.printStackTrace();
		}
		Main.writeLog(" TASK=INSERTING_ACCOUNTS STATUS=FINISHED");
		
	}
	
	private void updateDepts(){
		Main.writeLog(" TASK=UPDATING_ACCOUNTS STATUS=STARTING");
		try{
			PreparedStatement updatePS = conn.prepareStatement(UPDATE_ACCOUNT_SQL);
			Iterator<String> it = apiMap.keySet().iterator();
			int updatedCount = 0;
			while(it.hasNext()){
				updatedCount ++;
				String thisID = it.next();
				Department thisDept = apiMap.get(thisID);
				String thisSFid = thisDept.getSfID();
				String thisName = thisDept.getName();
				updatePS.clearParameters();
				int c = 1;
				updatePS.setString(c++, thisName);
				updatePS.setString(c++, thisSFid);
				int numUpdated = updatePS.executeUpdate();
				if(numUpdated == 1){
					Main.writeLog(" TASK=UPDATING_ACCOUNT STATUS=SUCCESS ORGUNITID=" + thisID + " SFID=" + thisSFid );
				}else{
					Main.writeLog(" TASK=UPDATING_ACCOUNT STATUS=ERROR ORGUNITID=" + thisID + " SFID=" + thisSFid + " UPDATE_COUNT=" + numUpdated);
				}
			}
		}catch(SQLException ex){
			Main.writeLog(" TASK=UPDATING_ACCOUNTS STATUS=EXCEPTION");
			ex.printStackTrace();
		}
		Main.writeLog(" TASK=UPDATING_ACCOUNTS STATUS=FINISHED");
	}
	
	public static String listDepts(TreeMap<String, Department> thisMap){
		return listDepts(thisMap,"\n");
	}
	
	
	
	public static String listDepts(TreeMap<String, Department> thisMap, String cr){
		String result = "";
		Iterator<String> it = thisMap.keySet().iterator();
		
		while(it.hasNext()){
			String thisID = it.next();
			String thisDept = thisMap.get(thisID).getName();
			result += thisID + " : " + thisDept + cr;
		}
		result += "end of list" + cr;
		return result;
	}
	
	public static void TryThisAtHome(){
		String myJson = "";
		myJson += "{" ;
		myJson += "  \"metadata\": {" ;
		myJson += "    \"size\": 11" ;
		myJson += "  }," ;
		myJson += "  \"items\": [" ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10002833\"," ;
		myJson += "      \"name\": \"AMPS-Libraries\"" ;
		myJson += "    }," ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10004084\"," ;
		myJson += "      \"name\": \"Abdul Latif Jameel Poverty Action Lab\"" ;
		myJson += "    }," ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10000644\"," ;
		myJson += "      \"name\": \"Academic Media Production Services\"" ;
		myJson += "    }," ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10003483\"," ;
		myJson += "      \"name\": \"Accounts Payable\"" ;
		myJson += "    }," ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10000328\"," ;
		myJson += "      \"name\": \"Administrative Services: Chem E/DMSE\"" ;
		myJson += "    }," ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10000755\"," ;
		myJson += "      \"name\": \"Admissions Office\"" ;
		myJson += "    }," ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10000928\"," ;
		myJson += "      \"name\": \"Advanced Study Program\"" ;
		myJson += "    }," ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10004436\"," ;
		myJson += "      \"name\": \"Alumni Association SWEB\"" ;
		myJson += "    }," ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10004408\"," ;
		myJson += "      \"name\": \"Alumni Association Strategic Initiatives\"" ;
		myJson += "    }," ;
		myJson += "    " ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10004334\"," ;
		myJson += "      \"name\": \"World Wide Web Consortium\"" ;
		myJson += "    }," ;
		myJson += "    {" ;
		myJson += "      \"orgUnitId\": \"10004358\"," ;
		myJson += "      \"name\": \"edX\"" ;
		myJson += "    }" ;
		myJson += "  ]" ;
		myJson += "}" ;

	        try{

	        	
	            JSONObject responeJson = new JSONObject(myJson);
	            System.out.println(responeJson.getJSONObject("metadata").getInt("size"));
	           
                JSONArray jsonArray = responeJson.getJSONArray("items");

                
                for (int i=0;i<jsonArray.length();i++){
                    System.out.println("orgUnitId : "+jsonArray.getJSONObject(i).getString("orgUnitId"));
                    System.out.println("name : "+jsonArray.getJSONObject(i).getString("name"));
                }
	            
	        }catch (Throwable e){
	            e.printStackTrace();
	        }
	}
	
}
