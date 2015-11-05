package edu.mit.ist.salesforce.sync;

//class for holding department info

public class Department {

	private Integer orgUnitID;
	private String Name;
	public Department(Integer orgUnitID, String name) {
		super();
		this.orgUnitID = orgUnitID;
		Name = name;
	}
	
	public Integer getOrgUnitID() {
		return orgUnitID;
	}
	
	public String getName() {
		return Name;
	}
	
	
	
}
