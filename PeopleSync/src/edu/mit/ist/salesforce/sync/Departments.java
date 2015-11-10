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

public class Departments {
	
	public static final String SF_ACCOUNTS_SQL = "select * from <schema>.account where from_api__c = true and active__c = true and orgunitid__c is not null";
	public static final String SF_DEACTIVEATE_ACCOUNT_SQL = "update <schema>.account set active__c = false, inactive_date__c = current_date where sfid = ?";
	public static final String INSERT_ACCOUNT_SQL = 
												"insert into <schema>.account \n" +
												"(name,orgunitid__c, active_date__c, active__c, from_api__c) \n" +
												"values \n" +
												"(?, ?, current_date, true, true) \n";
	
	public static final String UPDATE_ACCOUNT_SQL = "update <schema>.account set name = ? where sfid like ?";
	
	String schema;
	
	TreeMap<String, Department> apiMap ;
	TreeMap<String, Department> sfMap;
	TreeMap<String, Department> addMap;
	Connection conn;
	boolean updateSF;
	
	public static void main(String[] args) throws URISyntaxException, SQLException{
		Departments me = new Departments("sftest1", Main.getAPIdepts(), Main.getConnection());
		
		System.out.println("\n\n------ API Map ------\n\n");
		System.out.print(listDepts(me.apiMap));
		
		me.loadSFdepts();
		System.out.println("\n\n------ SF  Map ------\n\n");
		System.out.print(listDepts(me.sfMap));
		//TryThisAtHome();
	}
	
	public Departments(String schema, TreeMap<String, Department> apiMap, Connection conn) throws URISyntaxException, SQLException{
		 this(schema,apiMap,conn,true);
		
	}
	
	public Departments(String schema, TreeMap<String, Department> apiMap, Connection conn, boolean updateSF) throws URISyntaxException, SQLException{
		this.schema = schema;
		this.conn = conn;
		this.updateSF = updateSF;
		this.apiMap  = apiMap;
		sfMap   = new TreeMap<String, Department>();
		addMap  = new TreeMap<String, Department>();
		
	}
	
	public void loadData(){
		
		loadSFdepts();
	}
	
	public void CompareUpdate(){
		compareMaps();
		if(updateSF){
			deactivateDepts();
			insertDepts();
			updateDepts();
		}else{
			Main.writeLog(" STATUS=NOT_UPDATING_SF");
		}
		
		
	}
		

	
	
	
	private  void loadSFdepts(){
		try {
			Main.writeLog(" TASK=LOAD_SF_DATA STATUS=STARTING");
			PreparedStatement readPS = conn.prepareStatement(SF_ACCOUNTS_SQL.replace("<schema>", schema));
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
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Main.writeLog(" TASK=LOAD_SF_DATA STATUS=FINISHED COUNT=" + sfMap.size());
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
					Main.writeLog(sfDept, " STATUS=DIFFERENCE_FOUND API_NAME=\"" + apiDept.getName() + "\"");
					apiDept.setSfID(sfMap.get(apiID).getSfID());
					sfMap.remove(apiID);
				}
			}else{
				//there is not an entry in SF, NEW ACCOUNT
				Main.writeLog(apiDept, " STATUS=NEW_ITEM_FOUND");
				addMap.put(apiID, apiDept);
				it.remove();
			}
			
		}
		Main.writeLog(" TASK=COMPARE_DATA STATUS=FINISHED ITEMS_TO_ADD=" + addMap.size() + " ITEMS_TO_DEACTIVATE=" + sfMap.size() + " ITEMS_TO_UPDATE=" + apiMap.size());
	}//end of compareMaps
	
	private void deactivateDepts() {
		Main.writeLog(" TASK=DEACTIVATING_ACCOUNTS STATUS=STARTING");
		try {
			PreparedStatement removePS = conn.prepareStatement(SF_DEACTIVEATE_ACCOUNT_SQL.replace("<schema>", schema));
			Iterator<String> it = sfMap.keySet().iterator();
			int updatedCount = 0;
			while(it.hasNext()){
				updatedCount++;
				String thisID = it.next();
				Department thisDept = sfMap.get(thisID);
				removePS.clearParameters();
				removePS.setString(1, thisDept.getSfID());
				int numUpdated = removePS.executeUpdate();
				if(numUpdated == 1){
					Main.writeLog(thisDept," TASK=DEACTIVATING_ACCOUNT STATUS=SUCCESS ");
				}else{
					Main.writeLog(thisDept," TASK=DEACTIVATING_ACCOUNT STATUS=ERROR UPDATE_COUNT=" + numUpdated);
				}
				
			}
			removePS.close();
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
			PreparedStatement insertPS = conn.prepareStatement(INSERT_ACCOUNT_SQL.replace("<schema>", schema));
			Iterator<String> it = addMap.keySet().iterator();
			while(it.hasNext()){
				updatedCount ++;
				String thisID = it.next();
				Department thisDept = addMap.get(thisID);
				String thisName = thisDept.getName();
				insertPS.clearParameters();
				int c =1;
				insertPS.setString(c++, thisName);
				insertPS.setString(c++, thisID);
				int numUpdated = insertPS.executeUpdate();
				if(numUpdated == 1){
					Main.writeLog(thisDept, " TASK=INSERTING_ACCOUNT STATUS=SUCCESS ");
				}else{
					Main.writeLog(thisDept, " TASK=INSERTING_ACCOUNT STATUS=ERROR UPDATE_COUNT=" + numUpdated);
				}
			}
			insertPS.close();
			
		}catch(SQLException ex){
			Main.writeLog(" TASK=INSERTING_ACCOUNTS STATUS=EXCEPTION");
			ex.printStackTrace();
		}
		Main.writeLog(" TASK=INSERTING_ACCOUNTS STATUS=FINISHED COUNT=" + updatedCount);
		
	}
	
	private void updateDepts(){
		Main.writeLog(" TASK=UPDATING_ACCOUNTS STATUS=STARTING");
		int updatedCount = 0;
		try{
			PreparedStatement updatePS = conn.prepareStatement(UPDATE_ACCOUNT_SQL.replace("<schema>", schema));
			Iterator<String> it = apiMap.keySet().iterator();
			
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
					Main.writeLog(thisDept," TASK=UPDATING_ACCOUNT STATUS=SUCCESS " );
				}else{
					Main.writeLog(thisDept," TASK=UPDATING_ACCOUNT STATUS=ERROR UPDATE_COUNT=" + numUpdated);
				}
			}
			updatePS.close();
		}catch(SQLException ex){
			Main.writeLog(" TASK=UPDATING_ACCOUNTS STATUS=EXCEPTION");
			ex.printStackTrace();
		}
		Main.writeLog(" TASK=UPDATING_ACCOUNTS STATUS=FINISHED COUNT=" + updatedCount);
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
