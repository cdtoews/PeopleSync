package edu.mit.ist.salesforce.sync;

import edu.mit.ist.logging.StringAppender;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.json.JSONArray;
import org.json.JSONObject;


import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;


public class Main {

	static final Logger logger = LogManager.getLogger(Main.class);
	
	Connection conn;
	public static String current_schema = "INITIALIZING";
	public String home_schema = "INITIALIZING";
	public static final String APP_NAME = "PEOPLE_SYNC";
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("M-dd-yyyy HH:mm:ss");
	public static final Integer MAX_LOG_SIZE = 130000;
	private  StringAppender stringAppender;//used to store log as a string to send to Salesforce objects
	public static final String stringAppenderFormat = "[%-5level] %d{yyyy-MM-dd HH:mm:ss.SSS} %c{1}.%M - %msg%n";
	
	public static final String[] DEPT_SYNC_PROPERTIES = new String[]{
																"log_archive_days__c"//days to keep logs before deleting them, -1 to never delete
																,"log_text_field__c"
																,"orgunitid_field__c"
																,"from_api_field__c"
																,"log_object_name__c"
																,"sfid"
																,"name" //schema name
																,"log_datetime_field__c"
																,"name_field__c"
																,"object_name__c"
																,"active_date_field__c"
																,"active_field__c"
																,"inactive_date_field__c"
																,"sync_active__c"
																,"update_salesforce__c"
																};
	
	
	public static final String[] PEOPLE_SYNC_PROPERTIES = new String[]{
		"schema_name__c" //name of schema in Heroku Connect
		,"sfid" //SFID of the row holding this info, so we can update with log info
		,"sync_active__c" //is this item active, when inactive, the item will not even be looked at
		,"update_salesforce__c" // should this item update salesforce
		//------- Log -----------
		,"log_object_name__c" //name of object to hold person sync logs
		,"log_text_field__c"  //text(long) 131,000 field for holding the last sync log
		,"log_datetime_field__c" //datetime field to hold sync datetime
		,"log_archive_days__c" //how long until the logs are deleted (-1 to never delete)
		//------- Person -----------
		,"person_object_name__c" //object name for person object
		,"person_account_lookup_field__c" //lookup to account
		,"person_kerb_id_field__c" 
		,"person_first_name_field__c"
		,"person_middle_name_field__c"
		,"person_last_name_field__c"
		,"person_display_name_field__c"
		,"person_email_field__c"
		,"person_phone_number_field__c"
		,"person_website_field__c"
		,"person_log_changes__c" //should the sync update field on person object of changes, not currently in use
		,"person_update_notes_field__c"//optional field for log of person changes, not currently in use
		,"person_from_api_field__c" //checkbox used to designate if 'person' is from API
		,"person_active_field__c" //checkbox marked false when person not active in API
		,"person_active_date_field__c" //date field populated when inserted using apex insertion. not used if sync not populating SF
		,"person_inactive_date_field__c" //date field populated when kerbID not active in API
		//------- affiliation -----------
		,"affiliation_object_name__c"
		,"affiliation_type_field__c"
		,"affiliation_office_field__c"
		,"affiliation_title_field__c"
		,"affiliation_person_lookup_field__c"
		,"affiliation_from_api_field__c"
		,"affiliation_active_field__c"
		,"affiliation_active_date_field__c"
		,"affiliation_inactive_date_field__c"
		//-------- Dept_Affiliation -----------
		,"deptaff_object_name__c"
		,"deptaff_orgunit_id_field__c"
		,"deptaff_affiliation_lookup_field__c"
		,"deptaff_department_lookup_field__c"
		,"deptaff_from_api_field__c"
		,"deptaff_active_field__c"
		,"deptaff_active_date_field__c"
		,"deptaff_inactive_date_field__c"
		//--------- Department ---------
		,"department_object_name__c"
		,"department_from_api_field__c"
		,"department_active_field__c"
		,"department_orgunitid_field__c"	
		};
	
	public static final String RECORD_DEPT_LOG_SQL = "update <home_schema>.dept_sync__c set last_sync_date__c = current_timestamp, last_sync_log__c = ? where sfid = ?";
	public static final String RECORD_PEOPLE_LOG_SQL = "update <home_schema>.people_sync__c set last_sync_date__c = current_timestamp, last_sync_log__c = ? where sfid = ?";
	
	
	
	
	
	public Main() throws URISyntaxException, SQLException{
		//try to get logging level
		Level level;
		String levelString = System.getenv("LOG_LEVEL");
		if("TRACE".equalsIgnoreCase(levelString)){
			level = Level.TRACE;
		}else if("DEBUG".equalsIgnoreCase(levelString)){
			level = Level.DEBUG;
		}else if("WARN".equalsIgnoreCase(levelString)){
			level = Level.WARN;
		}else if("INFO".equalsIgnoreCase(levelString)){
			level = Level.INFO;
		}else if("ERROR".equalsIgnoreCase(levelString)){
			level = Level.ERROR;
		}else if("FATAL".equalsIgnoreCase(levelString)){
			level = Level.FATAL;
		}else{
			//default level
			level = Level.INFO;
		}
		
		
		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME); 
		loggerConfig.setLevel(level);
		ctx.updateLoggers();  // This causes all Loggers to refetch information from their LoggerConfig.
		
		logger.fatal("LOG_LEVEL=" + level.name());

		// Create a String Appender to capture log output
		stringAppender = StringAppender.createStringAppender(stringAppenderFormat);
		stringAppender.addToLogger(logger.getName(), Level.INFO);
		stringAppender.start();
		
		conn = getConnection();
		//let's get our home schema
		home_schema = System.getenv("HOME_SCHEMA");
	}
	
	
	public static void main(String[] args) throws URISyntaxException, SQLException{
		//TestThis.main(null);
		//System.exit(0);
		
		Main me = new Main();
		me.run();
	}
	
	public  void run() throws URISyntaxException, SQLException{
		ThreadContext.put("id", UUID.randomUUID().toString()); // Add the fishtag;
		ThreadContext.put("current_schema", current_schema); //used in console logging to know what schema we are in

		

		
		//each sync instance is a different hashset of properties
		HashSet<Properties> deptPropSet = new HashSet<Properties>();
		
		//first let's iterate over dept_sync__c table
		PreparedStatement syncListPS = conn.prepareStatement("select * from " + home_schema + ".dept_sync__c where sync_active__c = true");
		ResultSet syncListRS = syncListPS.executeQuery();
		while(syncListRS.next()){
			String thisSchema = syncListRS.getString("name");
			ThreadContext.put("current_schema", thisSchema); 
			//let's load up a properties object for each record
			Properties thisProp = new Properties();
			for(String syncProp:DEPT_SYNC_PROPERTIES){
				String thisValue = syncListRS.getString(syncProp);
				logger.trace("TASK=LOADING_DEPT_VALUES " + syncProp + "=" + thisValue );
				thisProp.put(syncProp, thisValue);
			}
			deptPropSet.add(thisProp);
		}//end of while syncListRS.next
		
		syncListRS.close();
		syncListPS.close();
		//load API data once
		TreeMap<String, Department> apiMap = getAPIdepts();
		ThreadContext.put("comparing", "Departments"); 
		
//		logger.fatal("test message");
//		System.out.println("\n\n---------------------------\n" + stringAppender.getOutput());
		
		//-------------------------------------------
		//---------- Main Dept work loop ------------
		//-------------------------------------------
		if(apiMap != null){
			for(Properties eachProp:deptPropSet){
				
				resetLog();//reset the stringAppender
				current_schema = eachProp.getProperty("name");
				ThreadContext.put("current_schema", current_schema); 
				//resetLog();
				logger.info(" TASK=READING_DEPARTMENTS STATUS=STARTING_SCHEMA SCHEMA=\"" + current_schema + "\"");
				//writeLog(" STATUS=STARTING_SCHEMA");
				Departments depts = new Departments(eachProp,new TreeMap<String,Department>(apiMap), conn);//passing a shallow copy of apiMap.(so we don't have to poll API ever schema
				depts.loadData();
				depts.CompareUpdate();
				depts.writeLog(getLog());
				recordDeptLog( depts.getRunInfo() + getLog(true),eachProp.getProperty("sfid"));
				logger.info(" STATUS=FINISHED_SCHEMA");
				
			}//end of looping through properties (sync sets)
		}//end of if apiMap not null
		
		
		//-------- People ------------
		ThreadContext.put("current_schema", "INITIALIZING_PEOPLE"); 
		logger.info(" TASK=LOADING_PEOPLE_PROPERTIES");
		//each sync instance is a different hashset of properties
		HashSet<Properties> peoplePropSet = new HashSet<Properties>();
		
		//first let's iterate over dept_sync__c table
		PreparedStatement psyncListPS = conn.prepareStatement("select * from " + home_schema + ".people_sync__c where sync_active__c = true");
		ResultSet psyncListRS = psyncListPS.executeQuery();
		while(psyncListRS.next()){
			//let's load up a properties object for each record
			String thisSchema = psyncListRS.getString("schema_name__c");
			ThreadContext.put("current_schema", thisSchema); 
			Properties thisProp = new Properties();
			for(String syncProp:PEOPLE_SYNC_PROPERTIES){
				String thisValue = psyncListRS.getString(syncProp);
				logger.trace("TASK=LOADING_PERSON_VALUES " + syncProp + "=" + thisValue );
				thisProp.put(syncProp, thisValue);
			}
			peoplePropSet.add(thisProp);
		}//end of while syncListRS.next
		
		psyncListRS.close();
		psyncListPS.close();
		
		ThreadContext.put("comparing", "People"); 
		//-------------------------------------------
		//---------- Main People work loop ----------
		//-------------------------------------------
		for(Properties eachProp:peoplePropSet){
			resetLog();
			current_schema = eachProp.getProperty("schema_name__c");
			//put the current schema where the logger can get it
			ThreadContext.put("current_schema", current_schema); 
			Persons persons = new Persons(conn, eachProp);
			//persons.clearOrphans();//orphans get cleared during comparisons
			persons.loadSFpeople();
			persons.compareUpdatePersons();
			persons.writeLog(getLog());
			recordPeopleLog(persons.getRunInfo() + getLog(true),eachProp.getProperty("sfid"));
		}


		conn.close();
		
	}//end of run
	
	
	public static  TreeMap<String, Department> getAPIdepts(){
		TreeMap<String, Department> apiMap = new TreeMap<String, Department>();
		logger.info(" TASK=LOAD_DEPT_API_DATA STATUS=STARTING");
		try {
			HttpResponse<String> response = Unirest.get("https://mit-public.cloudhub.io/departments/v1/departments")
					  .header("authorization", "Basic NmJmMjhjMGZlN2Y3NGEzYWJlZmRkZWYyYzQ5ZDljMzc6OWQxYjA0ZDgwNDczNDEzMzgxMEZEQTA2Q0M0MUMxNjM=")
					  .header("client_id", "6bf28c0fe7f74a3abefddef2c49d9c37")
					  .header("client_secret", "9d1b04d804734133810FDA06CC41C163")
					  .header("cache-control", "no-cache")
					  //.header("postman-token", "e52a374d-1022-49d3-6f1a-ed7bcb6aa4e1")
					  .asString();
			logger.trace(" TASK=LOAD_DEPT_API_DATA  STATUS=RECEIVED_JSON RAW_JASON=" + response.getBody());
			JSONObject responeJson = new JSONObject(response.getBody());
            int listSize = responeJson.getJSONObject("metadata").getInt("size");
			logger.trace(" TASK=LOAD_DEPT_API_DATA STATUS=RECEIVED_DATA LIST_SIZE=" + listSize);
           
            JSONArray jsonArray = responeJson.getJSONArray("items");

            
            for (int i=0;i<jsonArray.length();i++){
            	String thisOrgUnitID = jsonArray.getJSONObject(i).getString("orgUnitId");
            	String thisName = jsonArray.getJSONObject(i).getString("name");
            	apiMap.put(thisOrgUnitID, 	new Department(thisOrgUnitID,thisName )	);
            	logger.trace(" TASK=LOAD_DEPT_API_DATA  STATUS=LOADING_DEPTS ORGUNITID=" + thisOrgUnitID + " NAME=\"" + thisName + "\"");
            	//System.out.println("orgUnitId : "+jsonArray.getJSONObject(i).getString("orgUnitId"));
                //System.out.println("name : "+jsonArray.getJSONObject(i).getString("name"));
            }
            logger.info(" TASK=LOAD_DEPT_API_DATA COUNT=" + listSize);
            if(apiMap.size() != listSize){
            	logger.info(" TASK=LOAD_DEPT_API_DATA STATUS=ERROR NOTE=WRONG_SIZE METADATA_SIZE=" + listSize + " ITEMS_RECEIVED=" + apiMap.size());
            	System.exit(1);
            }
			
		} catch (UnirestException e) {
			
			logger.error("TASK=LOAD_DEPT_API_DATA STATUS=EXCEPTION " , e);
			return null;
		}catch(Exception ex){
			logger.error("TASK=LOAD_DEPT_API_DATA STATUS=EXCEPTION ",ex);
			return null;
		}
		logger.info(" TASK=LOAD_DEPT_API_DATA STATUS=FINISHED COUNT=" + apiMap.size());
		//if we received less than 300? results, return null
		if(apiMap.size() < 300 ){
			logger.error("TASK=LOAD_DEPT_API_DATA STATUS=ERROR NOTE=TOO_FEW_DEPTS");
			return null;
		}
		return apiMap;
	}
	
	/**
	 * records the log to the DEPT_SYNC object in the home schema
	 * @param logText is the text to put in the log
	 * @param sfID is the SFID of the dept_sync object to update
	 */
	private void recordDeptLog(String logText, String sfID){
		logger.info(" TASK=RECORDING_DEPT_LOG STATUS=STARTING");
		try{
			PreparedStatement recordPS = conn.prepareStatement(RECORD_DEPT_LOG_SQL.replace("<home_schema>", home_schema));
			int c=1;
			recordPS.setString(c++, logText);
			recordPS.setString(c++, sfID);
			Integer numUpdated = recordPS.executeUpdate();
			if(numUpdated != null && numUpdated == 1){
				logger.info(" TASK=RECORDING_DEPT_LOG STATUS=SUCCESS");
			}else{
				logger.info(" TASK=RECORDING_DEPT_LOG STATUS=FAILURE NUMUPDATED=" + numUpdated);
			}
		}catch(SQLException ex){
			logger.info(" TASK=RECORDING_DEPT_LOG STATUS=EXCEPTION");
		}
		
		logger.info(" TASK=RECORDING_DEPT_LOG STATUS=FINISHED");
		
	}
	
	/**
	 * records log info to the home schema people_sync object
	 * @param logText  log to update the object with
	 * @param sfID is the salesforce ID of the object on the home schema
	 */
	private void recordPeopleLog(String logText, String sfID){
		logger.info(" TASK=RECORDING_PEOPLE_LOG STATUS=STARTING");
		try{
			PreparedStatement recordPS = conn.prepareStatement(RECORD_PEOPLE_LOG_SQL.replace("<home_schema>", home_schema));
			int c=1;
			recordPS.setString(c++, logText);
			recordPS.setString(c++, sfID);
			Integer numUpdated = recordPS.executeUpdate();
			if(numUpdated != null && numUpdated == 1){
				logger.info(" TASK=RECORDING_PEOPLE_LOG STATUS=SUCCESS");
			}else{
				logger.info(" TASK=RECORDING_PEOPLE_LOG STATUS=FAILURE NUMUPDATED=" + numUpdated);
			}
		}catch(SQLException ex){
			logger.info(" TASK=RECORDING_PEOPLE_LOG STATUS=EXCEPTION");
		}
		
		logger.info(" TASK=RECORDING_PEOPLE_LOG STATUS=FINISHED");
		
	}
	
	
	public static void emailMe() {
		 		
		
		
		final String finalUser = "herokumailforme@gmail.com";
		final String finalPass = "Te3tL@b1";
		
		Properties props = new Properties();
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.smtp.port", "587");

		Session session = Session.getInstance(props,
		  new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(finalUser, finalPass);
			}
		  });

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress("herokumailforme@gmail.com"));
			message.setRecipients(Message.RecipientType.TO,
				InternetAddress.parse("ctoews@mit.edu"));
			message.setSubject("FunMail");
			message.setText("Spam Spam Spam Spam");

			Transport.send(message);

			

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}//end of main
	
	public static String readTempLog(){
		//we are assuming log is /logs/temp.log
		if(true) return "logs coming soon";
		String result = "";
		try{
			byte[] encoded = Files.readAllBytes(Paths.get("logs/temp.log"));
			  result = new String(encoded, "UTF-8");
				if(result.length() > MAX_LOG_SIZE){
					result = result.substring(0, MAX_LOG_SIZE);
				}
		}catch(Exception ex){
			ex.printStackTrace();
			result = "unable to read file";
		}
		return result;
	}//end of 
	
	
//	public static void resetTempLog(){
//		
//		try {
//			
//			java.nio.file.Files.deleteIfExists(Paths.get("logs/temp.log"));
//			java.nio.file.Files.createFile(Paths.get("logs/temp.log"));
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			logger.error("can't delete log file",e);
//			
//		}
//	}
	
	public static Connection getConnection() throws URISyntaxException, SQLException {
	    URI dbUri = new URI(System.getenv("DATABASE_URL"));

	    String username = dbUri.getUserInfo().split(":")[0];
	    String password = dbUri.getUserInfo().split(":")[1];
	    String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + ':' + dbUri.getPort() + dbUri.getPath() + "?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
	    //System.out.println(dbUrl);
	    return DriverManager.getConnection(dbUrl, username, password);
	}
	
	/**
	 * gets the log output from a stringAppender without resetting string appender
	 * @return
	 */
	public String getLog(){
		return getLog(false);
	}
	
	/**
	 * gets the log output from a string appender, 
	 * @param resetLog whether to reset the log, clearing it
	 * @return
	 */
	public String getLog(boolean resetLog){
		String result = stringAppender.getOutput();
		if(resetLog){
			resetLog();
		}
		return result;
	}
	
	/**
	 * resets the stringAppender (used for gathering a log per schema), emptying the string
	 */
	public void resetLog(){
		//Logger Stringlogger = LogManager.getRootLogger();
		try{
			stringAppender.removeFromLogger(LogManager.getRootLogger().getName());
		}catch(NullPointerException ex){
			logger.warn("couldn't remove stringAppender from logger");
		}
		
		stringAppender = StringAppender.createStringAppender(stringAppenderFormat);
		stringAppender.addToLogger(logger.getName(), Level.INFO);
		stringAppender.start();
	}


	/**
	 * trims log files that might be too long for SF fields
	 * @param input String to look at
	 * @return String to return, possibly truncated
	 */
	public static String trimLongString(String input){
		return trimLongString(input,MAX_LOG_SIZE);
	}
	
	/**
	 * Trims log files that might be too long for SF fields
	 * @param input  String to look at
	 * @param maxSize Maximum size of the String, leave some wiggle room for the "...more..."
	 * @return returns string, possibly truncated
	 */
	public static String trimLongString(String input, Integer maxSize){
		String result;
		if(input.length() > maxSize){
			result = input.substring(0, maxSize) + "\n...more...";
		}else{
			result = input;
		}
		
		
		return result;
	}
	
	public static String getEnvInfo(){
		String result = "";
		result += " JAVA_VERSION=\"" + System.getProperty("java.version") + "\" ";
		result += " DYNO=" + System.getenv("DYNO");
		try {
			result += " HOSTNAME=" + InetAddress.getLocalHost().getHostName() + " ";
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			result += " HOSTNAME=UNKNOWN ";
		}
		return result;
	}
	
	public static boolean readSFcheckbox(String field){
		if(field != null && field.toLowerCase().equals("t")){
			return true;
		}else{
			return false;
		}
	}
	
}//end of class
