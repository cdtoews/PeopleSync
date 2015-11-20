package edu.mit.ist.salesforce.sync;

import java.util.Comparator;

public class DeptAff implements Comparator  {

	private String orgUnitID;
	private String sfID;
	private String affiliationSFID;
	private String departmentSFID;
	private String name;
	
	public DeptAff(String orgUnitID,  String sfID) {
		this.orgUnitID = orgUnitID;
		this.sfID = sfID;
	}

	public DeptAff(String orgUnitID) {
		this.orgUnitID = orgUnitID;
		this.sfID = null;
	}

	public DeptAff withAffiliationSFID(String affiliationSFID){
		this.affiliationSFID = affiliationSFID;
		return this;
	}
	
	public DeptAff withDepartmentSFID(String departmentSFID){
		this.departmentSFID = departmentSFID;
		return this;
	}
	
	public DeptAff withName(String name){
		this.name = name;
		return this;
	}
	
	public String getAffiliationSFID() {
		return affiliationSFID;
	}

	public String getDepartmentSFID() {
		return departmentSFID;
	}

	public String getOrgUnitID() {
		return orgUnitID;
	}

	public String getSfID() {
		return sfID;
	}
	
	
	
	public void setSfID(String sfID) {
		this.sfID = sfID;
	}

	public void setAffiliationSFID(String affiliationSFID) {
		this.affiliationSFID = affiliationSFID;
	}

	public void setDepartmentSFID(String departmentSFID) {
		this.departmentSFID = departmentSFID;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String toString(){
		String cr = "\n";
		String prefix = "\t\t";
		String result = prefix + "----- Department -----" + cr;
		result += prefix + "sfID = " + sfID + cr;
		result += prefix + "orgUnitID = " + orgUnitID + cr;
		result += prefix + "name = " + name  + cr;
		result += prefix + "DepartmentSFID = " + departmentSFID + cr;
		result += prefix + "AffiliationSFID = " + affiliationSFID + cr;
		
		
		return result;
	}

	@Override
	public int compare(Object o1, Object o2) {
		DeptAff d1 = (DeptAff) o1;
		DeptAff d2 = (DeptAff) o2;
		if(d1.getOrgUnitID().equals(d2.getOrgUnitID())){
			return 0;
		}else{
			return compare(d1.getOrgUnitID(), d2.getOrgUnitID());
		}
		
	}
	
	@Override
	public boolean equals(Object o){
		if(!( o instanceof DeptAff)){
			return false;
		}else{
			DeptAff otherDeptAff = (DeptAff) o;
			return this.getOrgUnitID().equals(otherDeptAff.getOrgUnitID());
		}
		
	}
}
