package edu.mit.ist.salesforce.sync;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.TreeMap;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.json.JSONArray;
import org.json.JSONObject;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;


public class Main {

	Connection conn;
	public static String current_schema = "INITIALIZING";
	public String home_schema = "INITIALIZING";
	public static final String APP_NAME = "PEOPLE_SYNC";
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("M-dd-yyyy HH:mm:ss");
	
	public static final String[] SYNC_PROPERTIES = new String[]{
																"log_archive_days__c"
																,"log_text_field__c"
																,"orgunitid_field__c"
																,"from_api_field__c"
																,"log_object_name__c"
																,"sfid"
																,"name"
																,"log_datetime_field__c"
																,"name_field__c"
																,"object_name__c"
																,"active_date_field__c"
																,"active_field__c"
																,"inactive_date_field__c"
																,"sync_active__c"
																,"update_salesforce__c"
																};
	
	
	public static final String RECORD_LOG_SQL = "update <home_schema>.dept_sync__c set last_sync_date__c = current_timestamp, last_sync_log__c = ? where sfid = ?";
	
	public Main() throws URISyntaxException, SQLException{
		conn = getConnection();
		//let's get our home schema
		home_schema = System.getenv("HOME_SCHEMA");
	}
	
	
	public static void main(String[] args) throws URISyntaxException, SQLException{
		Main me = new Main();
		me.run();
	}
	
	public  void run() throws URISyntaxException, SQLException{
		//emailMe();
		//Small change
		//added in dummy1
		
		
		HashSet<Properties> propSet = new HashSet<Properties>();
		
		
		
		//first let's iterate over dept_sync__c table
		PreparedStatement syncListPS = conn.prepareStatement("select * from " + home_schema + ".dept_sync__c where sync_active__c = true");
		ResultSet syncListRS = syncListPS.executeQuery();
		while(syncListRS.next()){
			//let's load up a properties object for each record
			Properties thisProp = new Properties();
			for(String syncProp:SYNC_PROPERTIES){
				
				thisProp.put(syncProp, syncListRS.getString(syncProp));
			}
			propSet.add(thisProp);
		}//end of while syncListRS.next
		
		syncListRS.close();
		syncListPS.close();
		//load API data once
		TreeMap<String, Department> apiMap = getAPIdepts();
		
		
		//-------------------------------------------
		//------------ Main work loop ---------------
		//-------------------------------------------
		for(Properties eachProp:propSet){
			//System.out.println("---------start properties---------------");
			//for(String syncProp:SYNC_PROPERTIES){
			//	System.out.println(syncProp + "\t " + eachProp.getProperty(syncProp));
			//}
			current_schema = eachProp.getProperty("name");
			writeLog(" STATUS=STARTING_SCHEMA");
			Departments depts = new Departments(eachProp,new TreeMap<String,Department>(apiMap), conn);//passing a shallow copy of apiMap. 
			depts.loadData();
			depts.CompareUpdate();
			recordLog(depts.getMyLog(Departments.MAX_LOG_SIZE),eachProp.getProperty("sfid"));
			writeLog(" STATUS=FINISHED_SCHEMA");
			
		}//end of looping through properties (sync sets)
		
		
		

		conn.close();
		
	}//end of run
	
	
	public static  TreeMap<String, Department> getAPIdepts(){
		TreeMap<String, Department> apiMap = new TreeMap<String, Department>();
		writeLog(" TASK=LOAD_API_DATA STATUS=STARTING");
		try {
			HttpResponse<String> response = Unirest.get("https://mit-public.cloudhub.io/departments/v1/departments")
					  .header("authorization", "Basic NmJmMjhjMGZlN2Y3NGEzYWJlZmRkZWYyYzQ5ZDljMzc6OWQxYjA0ZDgwNDczNDEzMzgxMEZEQTA2Q0M0MUMxNjM=")
					  .header("client_id", "6bf28c0fe7f74a3abefddef2c49d9c37")
					  .header("client_secret", "9d1b04d804734133810FDA06CC41C163")
					  .header("cache-control", "no-cache")
					  //.header("postman-token", "e52a374d-1022-49d3-6f1a-ed7bcb6aa4e1")
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
            writeLog(" TASK=LOAD_API_DATA COUNT=" + listSize);
            if(apiMap.size() != listSize){
            	writeLog(" TASK=LOAD_API_DATA STATUS=ERROR NOTE=WRONG_SIZE METADATA_SIZE=" + listSize + " ITEMS_RECEIVED=" + apiMap.size());
            	System.exit(1);
            }
			
		} catch (UnirestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		writeLog(" TASK=LOAD_API_DATA STATUS=FINISHED COUNT=" + apiMap.size());
		return apiMap;
	}
	
	private void recordLog(String logText, String sfID){
		writeLog(" TASK=RECORDING_LOG STATUS=STARTING");
		try{
			PreparedStatement recordPS = conn.prepareStatement(RECORD_LOG_SQL.replace("<home_schema>", home_schema));
			int c=1;
			recordPS.setString(c++, logText);
			recordPS.setString(c++, sfID);
			Integer numUpdated = recordPS.executeUpdate();
			if(numUpdated != null && numUpdated == 1){
				writeLog(" TASK=RECORDING_LOG STATUS=SUCCESS");
			}else{
				writeLog(" TASK=RECORDING_LOG STATUS=FAILURE NUMUPDATED=" + numUpdated);
			}
		}catch(SQLException ex){
			writeLog(" TASK=RECORDING_LOG STATUS=EXCEPTION");
		}
		
		writeLog(" TASK=RECORDING_LOG STATUS=FINISHED");
		
	}
	
	
	public static void emailMe() {
		 		
		String username = System.getenv("smtpuser");
		String password = System.getenv("smtppass");
		
		System.out.println("user:" + username);
		
		final String finalUser = username;
		final String finalPass = password;
		
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
			message.setText("Dear Mail Crawler,"
				+ "\n\n No spam to my email, please!");

			Transport.send(message);

			System.out.println("Done");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}//end of main
	
	
	public static Connection getConnection() throws URISyntaxException, SQLException {
	    URI dbUri = new URI(System.getenv("DATABASE_URL"));

	    String username = dbUri.getUserInfo().split(":")[0];
	    String password = dbUri.getUserInfo().split(":")[1];
	    String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + ':' + dbUri.getPort() + dbUri.getPath() + "?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
	    //System.out.println(dbUrl);
	    return DriverManager.getConnection(dbUrl, username, password);
	}
	
	
	public static String writeLog(String whatToWrite){
		String  result = DATE_FORMAT.format(new Date())  + " APP=" + APP_NAME + " SCHEMA=" + current_schema + " " + whatToWrite;
		System.out.println(result);
		return result;
	}
	
	public static String writeLog(Department dept, String whatToWrite){
		String orgUnitID = dept.getOrgUnitID();
		String Name = dept.getName();
		String sfID = dept.getSfID();
		String result = DATE_FORMAT.format(new Date())  + " APP=" + APP_NAME + " SCHEMA=" + current_schema + " ORGUNITID=" + orgUnitID + " NAME=\"" + Name + "\" SFID=" + sfID + " " + whatToWrite;
		System.out.println(result);
		return result;
		
	}
	
}//end of class
