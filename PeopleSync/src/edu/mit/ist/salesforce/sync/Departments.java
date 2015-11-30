package edu.mit.ist.salesforce.sync;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

public class Departments {
	
	static final Logger logger = LogManager.getLogger(Departments.class);
	
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
	
	private int apiDeptCount = 0;
	private int deptsCreated = 0;
	private int deptsUpdated = 0;
	private int deptsDeactivated = 0;
	
	
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
		
		
		logger.info( " STATUS=STARTING_SCHEMA " + Main.getEnvInfo() );
		this.props = props;
		this.schema = this.props.getProperty("name");
		this.conn = conn;
		
		
		//let's populate updateSF
		String updateSFtext = props.getProperty("update_salesforce__c");
		updateSF = Main.readSFcheckbox(updateSFtext);
		
		
		this.apiMap  = apiMap;
		apiDeptCount = this.apiMap.size();
		logger.trace(" API_DEPT_MAP_SIZE=" + apiDeptCount);
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
	
	/**
	 * This replaces all the variables in each SQL statement with schema specific values
	 * @param input is the base SQL statement
	 * @return returns the SQL statement specific to this schema
	 */
	private  String replaceProps(String input){
		String output = input;
		for(String eachProp:Main.DEPT_SYNC_PROPERTIES){
			output = output.replace("<" + eachProp + ">", this.props.getProperty(eachProp));
		}
		return output;
		
	}
	
	/**
	 * Loads the Salesforce data into the departments object
	 */
	public void loadData(){
		
		loadSFdepts();
	}
	
	/**
	 * Compares SF depts to API depts, and if updateSF=true, then deactivates/inserts/updates
	 */
	public void CompareUpdate(){
		compareMaps();
		trimLogs();
		if(updateSF){
			deactivateDepts();
			insertDepts();
			updateDepts();
			
			
		}else{
			logger.info(" STATUS=NOT_UPDATING_SF");
		}
		
		
	}
		

	
	
	
	private  void loadSFdepts(){
		try {
			logger.info(" TASK=LOAD_SF_DEPTS STATUS=STARTING");
			String OrgIDField = props.getProperty("orgunitid_field__c");
			String nameField = props.getProperty("name_field__c");
			
			PreparedStatement readPS = conn.prepareStatement(SF_ACCOUNTS_SQL);
			ResultSet readRS = readPS.executeQuery();
			while(readRS.next()){
				//System.out.println(readRS.getString("sfid") + "  " + readRS.getString("name"));
				String thisOrgunitID = readRS.getString(OrgIDField);
				String thisSFID = readRS.getString("sfid");
				String thisName = readRS.getString(nameField);
				logger.trace(" TASK=LOAD_SF_DEPTS ORGUNITID=" + thisOrgunitID + " SFID=" + thisSFID + " NAME=" + thisName);
				sfMap.put(thisOrgunitID, new Department(thisOrgunitID, readRS.getString(nameField), thisSFID) );
			}
			
			readRS.close();
			readPS.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info(" TASK=LOAD_SF_DATA STATUS=FINISHED COUNT=" + sfMap.size());
	}
	
	
	private void compareMaps(){
		logger.info(" TASK=COMPARE_DEPT_DATA STATUS=STARTING");
		Iterator<String> it = apiMap.keySet().iterator();
		while(it.hasNext()){
			String apiID = it.next();
			Department apiDept = apiMap.get(apiID);
			
			if(sfMap.containsKey(apiID)){
				//there is an entry in SF with the orgunitid
				Department sfDept = sfMap.get(apiID);
				if(sfDept.getName().equals(apiDept.getName())){
					//everything matches, remove from both maps
					logger.trace(" TASK=COMPARE_DEPT_DATA STATUS=DEPT_EQUAL ORGUNITID=" + apiID + " SFID=" + sfDept.getSfID());
					it.remove();
					sfMap.remove(apiID);
				}else{
					//set sfID for the dept so we can use it to update later
					logger.info( " STATUS=DIFFERENCE_FOUND ORGUNITID=" + apiID + " SFID=" + sfDept.getSfID() +  " API_NAME=\"" + apiDept.getName() + "\" SF_NAME=\"" + sfDept.getName() + "\"" );
					apiDept.setSfID(sfMap.get(apiID).getSfID());
					sfMap.remove(apiID);
				}
			}else{
				//there is not an entry in SF, NEW ACCOUNT
				logger.info( " STATUS=NEW_ITEM_FOUND  " + apiDept.quickinfo());
				addMap.put(apiID, apiDept);
				it.remove();
			}
			
		}
		logger.info(" TASK=COMPARE_DATA STATUS=FINISHED ITEMS_TO_ADD=" + addMap.size() + " ITEMS_TO_DEACTIVATE=" + sfMap.size() + " ITEMS_TO_UPDATE=" + apiMap.size());
	}//end of compareMaps
	
	private void deactivateDepts() {
		logger.info(" TASK=DEACTIVATING_ACCOUNTS STATUS=STARTING");
		try {
			PreparedStatement removePS = conn.prepareStatement(SF_DEACTIVEATE_ACCOUNT_SQL);
			Iterator<String> it = sfMap.keySet().iterator();
			
			while(it.hasNext()){
				deptsUpdated++;
				String thisID = it.next();
				Department thisDept = sfMap.get(thisID);
				removePS.clearParameters();
				removePS.setString(1, thisDept.getSfID());
				int numUpdated = removePS.executeUpdate();
				if(numUpdated == 1){
					logger.info(" TASK=DEACTIVATING_ACCOUNT STATUS=SUCCESS " + thisDept.quickinfo());
				}else{
					logger.error(" TASK=DEACTIVATING_ACCOUNT STATUS=ERROR UPDATE_COUNT=" + numUpdated + thisDept.quickinfo());
				}
				
			}
			removePS.close();
			
			logger.info(" TASK=DEACTIVATING_ACCOUNTS STATUS=FINISHED COUNT=" + deptsUpdated);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(" TASK=DEACTIVATING_ACCOUNTS STATUS=EXCEPTION",e);
			
		}
		
	}//end of deactivateDepts
	
	private void insertDepts(){
		logger.info(" TASK=INSERTING_DEPTS STATUS=STARTING");
		
		try{
			PreparedStatement insertPS = conn.prepareStatement(INSERT_ACCOUNT_SQL);
			Iterator<String> it = addMap.keySet().iterator();
			while(it.hasNext()){
				deptsCreated ++;
				String thisID = it.next();
				Department thisDept = addMap.get(thisID);
				String thisName = thisDept.getName();
				insertPS.clearParameters();
				int c =1;
				insertPS.setString(c++, thisName);
				insertPS.setString(c++, thisID);
				int numUpdated = insertPS.executeUpdate();
				if(numUpdated == 1){
					logger.info( " TASK=INSERTING_DEPTS STATUS=SUCCESS " + thisDept.quickinfo());
				}else{
					logger.error( " TASK=INSERTING_DEPTS STATUS=ERROR UPDATE_COUNT=" + numUpdated + thisDept.quickinfo() );
				}
			}
			insertPS.close();
			
		}catch(SQLException ex){
			logger.error(" TASK=INSERTING_DEPTS STATUS=EXCEPTION",ex);
			
		}
		logger.info(" TASK=INSERTING_DEPTS STATUS=FINISHED COUNT=" + deptsCreated);
		
	}
	
	private void updateDepts(){
		logger.info(" TASK=UPDATING_DEPTS STATUS=STARTING");
		
		try{
			PreparedStatement updatePS = conn.prepareStatement(UPDATE_ACCOUNT_SQL);
			Iterator<String> it = apiMap.keySet().iterator();
			
			while(it.hasNext()){
				deptsUpdated ++;
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
					logger.info(" TASK=UPDATING_DEPTS STATUS=SUCCESS " + thisDept.quickinfo() );
				}else{
					logger.error(" TASK=UPDATING_DEPTS STATUS=ERROR UPDATE_COUNT=" + numUpdated + thisDept.quickinfo());
				}
			}
			updatePS.close();
		}catch(SQLException ex){
			logger.error(" TASK=UPDATING_DEPTS STATUS=EXCEPTION",ex);
			ex.printStackTrace();
		}
		logger.info(" TASK=UPDATING_DEPTS STATUS=FINISHED COUNT=" + deptsUpdated);
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
	
	/**
	 * trims logs on the target schema. deleting logs more than X days old (set in the SF object holding schema info(located in home schema))
	 */
	private void trimLogs(){
		logger.info(" TASK=TRIMMING_LOGS STATUS=STARTING");
		//first how many days are we going back
		Integer daysBack = null;
		try{
			daysBack = Integer.parseInt((String) this.props.get("log_archive_days__c"));
		}catch(Exception ex){
			logger.error(" TASK=TRIMMING_LOGS STATUS=NOT_TRIMMING REASON=NO_VALID_VALUE", ex);
			//didn't get a good number back, let's just go home now
			return;
		}
		
		if(daysBack == null || daysBack < 0){
			logger.info(" TASK=TRIMMING_LOGS STATUS=NOT_TRIMMING");
			//negative number or null, no trimming
			
			return;
		}
		
		try{
			PreparedStatement trimPS = conn.prepareStatement(TRIM_LOGS_SQL);
			int numTrimmed = trimPS.executeUpdate();
			trimPS.close();
			logger.info(" TASK=TRIMMING_LOGS STATUS=COMPLETE UPDATED=" + numTrimmed);
			
		}catch(SQLException ex){
			logger.error(" TASK=TRIMMING_LOGS STATUS=ERROR",ex);
			
		}
		
	}//end of trimLogs
	
	/**
	 * Writes to the Department Log object for each schema. it puts some general info, then inlcudes logText
	 * @param logText is the text to put along with some general information
	 */
	public void writeLog(String logText){
		logger.info(" TASK=WRITING_LOG STATUS=STARTING");
		
		
		
		String toWrite = getRunInfo() + "-----FULL LOG-----\n"  + logText;
		toWrite = Main.trimLongString(toWrite);
		String logObject = (String) props.get("log_object_name__c");
		if (logObject == null || logObject.equals("") ){
			logger.info(" TASK=WRITING_LOG STATUS=SKIPPING");
			return;
		}
		
		try{
			PreparedStatement writePS = conn.prepareStatement(INSERT_LOG_SQL);
			writePS.setString(1, toWrite);
			int numInserted = writePS.executeUpdate();
			if(numInserted == 1){
				logger.debug(" TASK=WRITING_LOG SUCCESS=TRUE");
			}else{
				logger.error(" TASK=WRITING_LOG SUCCESS=FALSE");
			}
			writePS.close();
		}catch(SQLException ex){
			logger.error( " TASK=WRITING_LOG STATUS=EXCEPTION",ex);
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
	
	
	public String getRunInfo(){
		String cr = "\n";
		String result = "";
		result += Main.getEnvInfo() + cr;
		result += "Schema=" + Main.current_schema + cr;
		result += "----Department Object=" + props.getProperty("object_name__c") + cr;
		result += "Departments created=" + deptsCreated + cr;
		result += "Departments Updated=" + deptsUpdated + cr;
		result += "Departments Deactivated=" + deptsDeactivated + cr;
		result += "API Dept Count=" + apiDeptCount + cr;
		result += "--------------------------------" + cr;
		
		return result;
	}
	
	

}
