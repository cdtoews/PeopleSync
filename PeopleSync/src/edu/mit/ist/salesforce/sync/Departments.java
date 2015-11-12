package edu.mit.ist.salesforce.sync;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import org.json.JSONArray;
import org.json.JSONObject;

public class Departments {
	
	public static final Integer MAX_LOG_SIZE = 130000;
	
	public static final String SF_ACCOUNTS_SQL_BASE = "select * from <name>.<object_name__c> where <from_api_field__c> = true and <active_field__c> = true and <orgunitid_field__c> is not null";
	String SF_ACCOUNTS_SQL;
	
	public static final String SF_DEACTIVEATE_ACCOUNT_SQL_BASE = "update <name>.<object_name__c> set <active_field__c> = false, <inactive_date_field__c> = current_date where sfid = ?";
	String SF_DEACTIVEATE_ACCOUNT_SQL;
	
	public static final String INSERT_ACCOUNT_SQL_BASE = 
												"insert into <name>.<object_name__c> \n" +
												"(<name_field__c>,<orgunitid_field__c>, <active_date_field__c>, <active_field__c>, <from_api_field__c>) \n" +
												"values \n" +
												"(?, ?, current_date, true, true) \n";
	String INSERT_ACCOUNT_SQL;
	
	public static final String UPDATE_ACCOUNT_SQL_BASE = "update <name>.<object_name__c> set <name_field__c> = ? where sfid = ?";
	String UPDATE_ACCOUNT_SQL;
	
	public static final String TRIM_LOGS_SQL_BASE = "delete from <name>.<log_object_name__c> where <log_datetime_field__c> < current_date - <log_archive_days__c> ";
	String TRIM_LOGS_SQL;
	
	public static final String INSERT_LOG_SQL_BASE = "insert into <name>.<log_object_name__c> (  <log_datetime_field__c> , <log_text_field__c>) values (current_timestamp, ?)";
	String INSERT_LOG_SQL;
	
	String schema;
	Properties props;
	String myLog;
	
	TreeMap<String, Department> apiMap ;
	TreeMap<String, Department> sfMap;
	TreeMap<String, Department> addMap;
	Connection conn;
	boolean updateSF;
	
	public static void main(String[] args) throws URISyntaxException, SQLException{
		//Departments me = new Departments("sftest1", Main.getAPIdepts(), Main.getConnection());
		
		//System.out.println("\n\n------ API Map ------\n\n");
		//System.out.print(listDepts(me.apiMap));
		
		//me.loadSFdepts();
		//System.out.println("\n\n------ SF  Map ------\n\n");
		//System.out.print(listDepts(me.sfMap));
		//TryThisAtHome();
	}
	

	
	public Departments(Properties props, TreeMap<String, Department> apiMap, Connection conn) throws URISyntaxException, SQLException{
		
		myLog = "";
		logThis( " STATUS=STARTING_SCHEMA " + Main.getEnvInfo() );
		this.props = props;
		this.schema = this.props.getProperty("name");
		this.conn = conn;
		
		
		//let's populate updateSF
		String updateSFtext = props.getProperty("update_salesforce__c");
		if(updateSFtext != null && updateSFtext.toLowerCase().equals("t")){
			updateSF = true;
		}else{
			updateSF = false;
		}
		
		this.apiMap  = apiMap;
		sfMap   = new TreeMap<String, Department>();
		addMap  = new TreeMap<String, Department>();
		//make my queries specific to this instance
		SF_ACCOUNTS_SQL = replaceProps(SF_ACCOUNTS_SQL_BASE);
		SF_DEACTIVEATE_ACCOUNT_SQL = replaceProps(SF_DEACTIVEATE_ACCOUNT_SQL_BASE);
		INSERT_ACCOUNT_SQL = replaceProps(INSERT_ACCOUNT_SQL_BASE);
		UPDATE_ACCOUNT_SQL = replaceProps(UPDATE_ACCOUNT_SQL_BASE);
		TRIM_LOGS_SQL = replaceProps(TRIM_LOGS_SQL_BASE);
		INSERT_LOG_SQL = replaceProps(INSERT_LOG_SQL_BASE);
	}
	
	
	private  String replaceProps(String input){
		String output = input;
		for(String eachProp:Main.SYNC_PROPERTIES){
			output = output.replace("<" + eachProp + ">", this.props.getProperty(eachProp));
		}
		return output;
		
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
			trimLogs();
			writeLog();
		}else{
			logThis(" STATUS=NOT_UPDATING_SF");
		}
		
		
	}
		

	
	
	
	private  void loadSFdepts(){
		try {
			logThis(" TASK=LOAD_SF_DATA STATUS=STARTING");
			String OrgIDField = props.getProperty("orgunitid_field__c");
			String nameField = props.getProperty("name_field__c");
			
			PreparedStatement readPS = conn.prepareStatement(SF_ACCOUNTS_SQL);
			ResultSet readRS = readPS.executeQuery();
			while(readRS.next()){
				//System.out.println(readRS.getString("sfid") + "  " + readRS.getString("name"));
				sfMap.put(readRS.getString(OrgIDField), 
            			new Department(readRS.getString(OrgIDField),
            				readRS.getString(nameField),
            				readRS.getString("sfid"))
            			);
			}
			
			readRS.close();
			readPS.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logThis(" TASK=LOAD_SF_DATA STATUS=FINISHED COUNT=" + sfMap.size());
	}
	
	
	private void compareMaps(){
		logThis(" TASK=COMPARE_DATA STATUS=STARTING");
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
					logThis(sfDept, " STATUS=DIFFERENCE_FOUND API_NAME=\"" + apiDept.getName() + "\"");
					apiDept.setSfID(sfMap.get(apiID).getSfID());
					sfMap.remove(apiID);
				}
			}else{
				//there is not an entry in SF, NEW ACCOUNT
				logThis(apiDept, " STATUS=NEW_ITEM_FOUND");
				addMap.put(apiID, apiDept);
				it.remove();
			}
			
		}
		logThis(" TASK=COMPARE_DATA STATUS=FINISHED ITEMS_TO_ADD=" + addMap.size() + " ITEMS_TO_DEACTIVATE=" + sfMap.size() + " ITEMS_TO_UPDATE=" + apiMap.size());
	}//end of compareMaps
	
	private void deactivateDepts() {
		logThis(" TASK=DEACTIVATING_ACCOUNTS STATUS=STARTING");
		try {
			PreparedStatement removePS = conn.prepareStatement(SF_DEACTIVEATE_ACCOUNT_SQL);
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
					logThis(thisDept," TASK=DEACTIVATING_ACCOUNT STATUS=SUCCESS ");
				}else{
					logThis(thisDept," TASK=DEACTIVATING_ACCOUNT STATUS=ERROR UPDATE_COUNT=" + numUpdated);
				}
				
			}
			removePS.close();
			logThis(" TASK=DEACTIVATING_ACCOUNTS STATUS=FINISHED COUNT=" + updatedCount);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logThis(" TASK=DEACTIVATING_ACCOUNTS STATUS=EXCEPTION");
			e.printStackTrace();
		}
		
	}//end of deactivateDepts
	
	private void insertDepts(){
		logThis(" TASK=INSERTING_ACCOUNTS STATUS=STARTING");
		int updatedCount = 0;
		try{
			PreparedStatement insertPS = conn.prepareStatement(INSERT_ACCOUNT_SQL);
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
					logThis(thisDept, " TASK=INSERTING_ACCOUNT STATUS=SUCCESS ");
				}else{
					logThis(thisDept, " TASK=INSERTING_ACCOUNT STATUS=ERROR UPDATE_COUNT=" + numUpdated);
				}
			}
			insertPS.close();
			
		}catch(SQLException ex){
			logThis(" TASK=INSERTING_ACCOUNTS STATUS=EXCEPTION");
			ex.printStackTrace();
		}
		logThis(" TASK=INSERTING_ACCOUNTS STATUS=FINISHED COUNT=" + updatedCount);
		
	}
	
	private void updateDepts(){
		logThis(" TASK=UPDATING_ACCOUNTS STATUS=STARTING");
		int updatedCount = 0;
		try{
			PreparedStatement updatePS = conn.prepareStatement(UPDATE_ACCOUNT_SQL);
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
					logThis(thisDept," TASK=UPDATING_ACCOUNT STATUS=SUCCESS " );
				}else{
					logThis(thisDept," TASK=UPDATING_ACCOUNT STATUS=ERROR UPDATE_COUNT=" + numUpdated);
				}
			}
			updatePS.close();
		}catch(SQLException ex){
			logThis(" TASK=UPDATING_ACCOUNTS STATUS=EXCEPTION");
			ex.printStackTrace();
		}
		logThis(" TASK=UPDATING_ACCOUNTS STATUS=FINISHED COUNT=" + updatedCount);
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
	
	private void trimLogs(){
		logThis(" TASK=TRIMMING_LOGS STATUS=STARTING");
		//first how many days are we going back
		Integer daysBack = null;
		try{
			daysBack = Integer.parseInt((String) this.props.get("log_archive_days__c"));
		}catch(Exception ex){
			logThis(" TASK=TRIMMING_LOGS STATUS=NOT_TRIMMING REASON=NO_VALID_VALUE");
			//didn't get a good number back, let's just go home now
			return;
		}
		
		if(daysBack == null || daysBack < 0){
			logThis(" TASK=TRIMMING_LOGS STATUS=NOT_TRIMMING");
			//negative number or null, no trimming
			logThis(" TASK=TRIMMING_LOGS STATUS=COMPLETING REASON=NO_VALID_VALUE");
			return;
		}
		
		try{
			PreparedStatement trimPS = conn.prepareStatement(TRIM_LOGS_SQL);
			int numTrimmed = trimPS.executeUpdate();
			trimPS.close();
			logThis(" TASK=TRIMMING_LOGS STATUS=COMPLETE UPDATED=" + numTrimmed);
			
		}catch(SQLException ex){
			logThis(" TASK=TRIMMING_LOGS STATUS=ERROR");
			ex.printStackTrace();
		}
		
	}//end of trimLogs
	
	
	private void writeLog(){
		logThis(" TASK=WRITING_LOG STATUS=STARTING");
		//just in case we get a ginormous log
		String toWrite = getMyLog(MAX_LOG_SIZE);
		String logObject = (String) props.get("log_object_name__c");
		if (logObject == null || logObject.equals("") ){
			logThis(" TASK=WRITING_LOG STATUS=SKIPPING");
			return;
		}
		
		try{
			PreparedStatement writePS = conn.prepareStatement(INSERT_LOG_SQL);
			writePS.setString(1, toWrite);
			int numInserted = writePS.executeUpdate();
			if(numInserted == 1){
				logThis(" TASK=WRITING_LOG SUCCESS=TRUE");
			}else{
				logThis(" TASK=WRITING_LOG SUCCESS=FALSE");
			}
			writePS.close();
		}catch(SQLException ex){
			logThis( " TASK=WRITING_LOG STATUS=EXCEPTION");
		}
		
	}//end of writeLog
	
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
	
	
	
	public String getMyLog() {
		return myLog;
	}

	public String getMyLog(int sizeLimit){
		String result = myLog;
		if(result.length() > MAX_LOG_SIZE){
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
