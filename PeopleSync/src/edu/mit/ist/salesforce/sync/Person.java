package edu.mit.ist.salesforce.sync;

import java.util.HashSet;
import java.util.Iterator;

public class Person {

	String kerbID;
	String firstName;
	String middleName;
	String lastName;
	String displayName;
	String email;
	String phoneNumber;
	String website;
	String sfID;
	HashSet<Affiliation> affs;
	
	
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
		this.sfID = sfID;
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
	
	
}
