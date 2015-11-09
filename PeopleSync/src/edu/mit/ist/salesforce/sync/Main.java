package edu.mit.ist.salesforce.sync;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;


public class Main {

	
	public static void main(String[] args) throws URISyntaxException, SQLException{
		emailMe();
		Connection conn = getConnection();
		PreparedStatement readPS = conn.prepareStatement("select * from sftest1.account");
		ResultSet readRS = readPS.executeQuery();
		while(readRS.next()){
			System.out.println(readRS.getString("sfid") + "  " + readRS.getString("name"));
		}
		System.out.println("done");
		readRS.close();
		readPS.close();
		conn.close();
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
	
	
	private static Connection getConnection() throws URISyntaxException, SQLException {
	    URI dbUri = new URI(System.getenv("DATABASE_URL"));

	    String username = dbUri.getUserInfo().split(":")[0];
	    String password = dbUri.getUserInfo().split(":")[1];
	    String dbUrl = "jdbc:postgresql://" + dbUri.getHost() + ':' + dbUri.getPort() + dbUri.getPath() + "?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
	    System.out.println(dbUrl);
	    return DriverManager.getConnection(dbUrl, username, password);
	}
	
}//end of class
