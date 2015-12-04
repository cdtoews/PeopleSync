package edu.mit.ist.salesforce.sync;

import java.sql.Connection;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Person {

	static final Logger logger = LogManager.getLogger(Person.class);
	
	private String kerbID;
	private String firstName;
	private String middleName;
	private String lastName;
	private String displayName;
	private String email;
	private String phoneNumber;
	private String website;
	private String sfID;
	private HashSet<Affiliation> affs;
	private boolean duplicate;
	private boolean isMissing;
	
	/**
	 * used to signify when a response is valid from the API, but the person is missing
	 * @param myNull, must be null to be used
	 * @throws Exception 
	 */
	public Person(String myNull){
		if(myNull == null){
			isMissing = true;
		}else{
			isMissing = false;
			logger.error("TASK=CREATE_PERSON STATUS=ERROR NOTE=BAD_INVOCATION");
		}
	}
	
	public Person(String kerbID, String firstName, String middleName,
			String lastName, String displayName, String email,
			String phoneNumber, String website) {
		this.kerbID = kerbID;
		this.firstName = firstName;
		this.middleName = middleName;
		this.lastName = lastName;
		this.displayName = displayName;
		this.email = email;
		this.phoneNumber = phoneNumber;
		this.website = website;
		affs = new HashSet<Affiliation>();
		sfID = null;
		duplicate = false;
		isMissing = false;
	}

	
	
	
	public Person(String kerbID, String firstName, String middleName,
			String lastName, String displayName, String email,
			String phoneNumber, String website, String sfID) {
		this.kerbID = kerbID;
		this.firstName = firstName;
		this.middleName = middleName;
		this.lastName = lastName;
		this.displayName = displayName;
		this.email = email;
		this.phoneNumber = phoneNumber;
		this.website = website;
		affs = new HashSet<Affiliation>();
		this.sfID = sfID;
		duplicate = false;
		isMissing = false;
	}


	


	public boolean isMissing() {
		return isMissing;
	}

	public String getSfID() {
		return sfID;
	}




	public void setSfID(String sfID) {
		this.sfID = sfID;
	}




	public void addAffiliation(Affiliation aff){
		affs.add(aff);
	}

	public String getKerbID() {
		return kerbID;
	}


	public String getFirstName() {
		return firstName;
	}


	public String getMiddleName() {
		return middleName;
	}


	public String getLastName() {
		return lastName;
	}


	public String getDisplayName() {
		return displayName;
	}


	public String getEmail() {
		return email;
	}


	public String getPhoneNumber() {
		return phoneNumber;
	}


	public String getWebsite() {
		return website;
	}


	public HashSet<Affiliation> getAffs() {
		return affs;
	}
	
	

	public boolean isDuplicate() {
		return duplicate;
	}




	public void setDuplicate(boolean duplicate) {
		this.duplicate = duplicate;
	}




	@Override
	public String toString(){
		String cr = "\n";
		String result = "--------------------" + cr;
		result += "----- Person -------" + cr;
		result += "--------------------" + cr;
		result += "kerbID = " + kerbID + cr;
		result += "sfID = " + sfID + cr;
		result += "firstName = " + firstName + cr;
		result += "middleName = " + middleName + cr;
		result += "lastName = " + lastName + cr;
		result += "displayName = " + displayName + cr;
		result += "email = " + email + cr;
		result += "phoneNumber = " + phoneNumber + cr;
		result += "website = " + website + cr;
		Iterator<Affiliation> it = affs.iterator();
		while(it.hasNext()){
			result += it.next().toString();
		}
		
		
		
		return result;
		
	}
	
	public boolean deactivateSFperson(Connection conn,String updateNote){
		
		
		
		return false;
	}
	
	public Boolean personFieldsEqual(Person otherPerson){

		//probably need to change this to output the difference to log
		if(
				//either one of the SFID's is null, meaning we are going to ignore it for this comparison, or they match
				((this.sfID == null || otherPerson.getSfID() == null) || this.sfID.equals(otherPerson.getSfID()))
				&& areStringsEqual(this.kerbID , otherPerson.getKerbID())
				&& areStringsEqual(this.firstName,otherPerson.getFirstName())
				&& areStringsEqual(this.middleName , otherPerson.getMiddleName())
				&& areStringsEqual(this.lastName, otherPerson.getLastName())
				&& areStringsEqual(this.displayName,otherPerson.getDisplayName())
				&& areStringsEqual(this.email,otherPerson.getEmail())
				&& areStringsEqual(this.phoneNumber,otherPerson.getPhoneNumber())
				&& areStringsEqual(this.website,otherPerson.getWebsite())
				
				){
			return true;
		}else{
			return false;
		}
			

	}
	
	public static boolean areStringsEqual(String s1, String s2){
		s1 = (s1 == null) ? "" : s1;
		s2 = (s2 == null) ? "" : s2;
		return s1.equals(s2);
	}
	
}
