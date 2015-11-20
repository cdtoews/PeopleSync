package edu.mit.ist.salesforce.sync;

import java.util.Iterator;

//class for holding department info

public class Department {

	private String orgUnitID;
	private String Name;
	private String sfID;
	
	
	public Department(String orgUnitID, String name) {
		super();
		this.orgUnitID = orgUnitID;
		this.Name = name;
		this.sfID = null;
		
	}
	
	
	
	public Department(String orgUnitID, String name, String sfID) {
		super();
		this.orgUnitID = orgUnitID;
		Name = name;
		this.sfID = sfID;
	}



	public String getOrgUnitID() {
		return orgUnitID;
	}
	
	public String getName() {
		return Name;
	}



	public String getSfID() {
		return sfID;
	}



	public void setSfID(String sfID) {
		this.sfID = sfID;
	}
	
	public String toString(){
		String cr = "\n";
		String prefix = "\t\t";
		String result = prefix + "----- Department -----" + cr;
		result += prefix + "sfID = " + sfID + cr;
		result += prefix + "orgUnitID = " + orgUnitID + cr;
		result += prefix + "Name = " + Name + cr;
		
		
		
		return result;
	}
	
	public String quickinfo(){
		return  " ORGUNITID=" + orgUnitID + " NAME=\"" + Name + "\" SFID=" + sfID + " ";
	}
}
