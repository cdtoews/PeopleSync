package edu.mit.ist.salesforce.sync;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;


public class Main {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("mail.smtp.host", "outgoing.mit.edu");
        Session session = Session.getDefaultInstance(props, null);

        String msgBody = "...";

        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress("ctoews@mit.edu", "Chris Toews"));
            msg.addRecipient(Message.RecipientType.TO,
                             new InternetAddress("ctoews@mit.edu", "Mr. User"));
            msg.setSubject("Your Example.com account has been activated");
            msg.setText(msgBody);
            Transport.send(msg);

        } catch (AddressException e) {
            e.printStackTrace();
        } catch (MessagingException e) {
            e.printStackTrace();
        }catch(Exception ex){
        	ex.printStackTrace();
        }
	}
	
}
