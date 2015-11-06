package edu.mit.ist.salesforce.sync;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;


public class Main {

	public static void notmain(String[] args){
		System.out.println("You are inside Main");
	}
	
	public static void main(String[] args) {
		 String username ;
		 String password;

		
		Properties getProp = new Properties();
		InputStream input = null;
		
		try{
			input = new FileInputStream("config.properties");

			// load a properties file
			getProp.load(input);
			username = getProp.getProperty("smtpuser");
			password = getProp.getProperty("smtppass");
		}catch(Exception ex){
			System.out.println("didn't find prop file, trying environmental variables");
			username = System.getenv("smtpuser");
			password = System.getenv("smtppass");
		}
		
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
	
}//end of class
