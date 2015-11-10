package edu.mit.ist.salesforce.sync;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

	public static String current_schema = "INITIALIZING";
	public static final String APP_NAME = "PEOPLE_SYNC";
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("M-dd-yyyy HH:mm:ss");
	
	public static void main(String[] args) throws URISyntaxException, SQLException{
		emailMe();
		if(true){
			System.exit(0);
		}
		Connection conn = getConnection();
		String[] schemas = new String[]{"sftest1","sfprod"};
		TreeMap<String, Department> apiMap = getAPIdepts();
		for(String schema: schemas){
			current_schema = schema;
			writeLog(" STATUS=STARTING_SCHEMA");
			Departments depts = new Departments(schema,new TreeMap<String,Department>(apiMap), conn);//passing a shallow copy of apiMap. 
			depts.loadData();
			depts.CompareUpdate();
			writeLog(" STATUS=FINISHED_SCHEMA");
		}
		
		conn.close();
		
	}
	
	
	public static  TreeMap<String, Department> getAPIdepts(){
		TreeMap<String, Department> apiMap = new TreeMap<String, Department>();
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
            	Main.writeLog(" TASK=LOAD_API_DATA STATUS=ERROR NOTE=WRONG_SIZE METADATA_SIZE=" + listSize + " ITEMS_RECEIVED=" + apiMap.size());
            	System.exit(1);
            }
			
		} catch (UnirestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Main.writeLog(" TASK=LOAD_API_DATA STATUS=FINISHED COUNT=" + apiMap.size());
		return apiMap;
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
	
	
	public static void writeLog(String whatToWrite){
		System.out.println(DATE_FORMAT.format(new Date())  + " APP=" + APP_NAME + " SCHEMA=" + current_schema + " " + whatToWrite);
	}
	
	public static void writeLog(Department dept, String whatToWrite){
		String orgUnitID = dept.getOrgUnitID();
		String Name = dept.getName();
		String sfID = dept.getSfID();
		
		System.out.println(DATE_FORMAT.format(new Date())  + " APP=" + APP_NAME + " SCHEMA=" + current_schema + " ORGUNITID=" + orgUnitID + " NAME=\"" + Name + "\" SFID=" + sfID + " " + whatToWrite);
		
		
	}
	
}//end of class
