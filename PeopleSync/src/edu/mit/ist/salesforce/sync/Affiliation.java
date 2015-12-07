package edu.mit.ist.salesforce.sync;

import java.util.Iterator;
import java.util.TreeMap;

public class Affiliation {

	private String type;
	private String title;
	private String office;
	private String sfID;
	private TreeMap<String, DeptAff> depts;//orgunitid,deptaff
	
	
	public Affiliation(String type, String title, String office) {
		this.type = type;
		this.title = title;
		this.office = office;
		this.sfID = null;
		depts = new TreeMap<String, DeptAff>();
	}

	

	
	public Affiliation(String type, String title, String office, String sfID) {
		this.type = type;
		this.title = title;
		this.office = office;
		this.sfID = sfID;
		depts = new TreeMap<String, DeptAff>();//orgunitID, deptAff
	}




	public String getSfID() {
		return sfID;
	}




	public void setSfID(String sfID) {
		this.sfID = sfID;
	}




	public void addDept(DeptAff deptAff){
		if(deptAff != null){
			String thisOUID = deptAff.getOrgUnitID();
			depts.put(thisOUID, deptAff);
		}
		
	}
	

	public String getType() {
		if(type == null){
			return "";
		}else{
			return type;
		}
		
	}


	public String getTitle() {
		if(title == null){
			return "";
		}else{
			return title;
		}
	}


	public String getOffice() {
		if(office == null){
			return "";
		}else{
			return office;
		}
	}


	public TreeMap<String, DeptAff> getDeptAffs() {
		return depts;
	}
	
	
	public String toString(){
		String cr = "\n";
		String prefix = "\t";
		String result = prefix + "----- Affiliation -----" + cr;
		result += prefix + "sfID = " + sfID + cr;
		result += prefix + "type = " + type + cr;
		result += prefix + "title = " + title + cr;
		result += prefix + "office = " + office + cr;
		Iterator<String> it  = depts.keySet().iterator();
		while(it.hasNext()){
			String orgUnitID = it.next();
			DeptAff eachDept = depts.get(orgUnitID);
			result += eachDept.toString();
		}
		
		
		return result;
	}
	
	public boolean equalValues(Affiliation other){
		if(
				this.getType().equals(other.getType())
				&& this.getTitle().equals(other.getTitle())
				&& this.getOffice().equals(other.getOffice())
				&& this.getDeptAffs().equals(other.getDeptAffs())
				){
			return true;
		}else{
			return false;
		}
		
	}//end of equalValues
	
	public boolean almostEqualValues(Affiliation other){
		if(
				this.getType().equals(other.getType())
				&& this.getTitle().equals(other.getTitle())
				// && this.getOffice().equals(other.getOffice())
				&& this.getDeptAffs().equals(other.getDeptAffs())
				){
			return true;
		}else{
			return false;
		}
		
	}//end of equalValues
	
	
}
