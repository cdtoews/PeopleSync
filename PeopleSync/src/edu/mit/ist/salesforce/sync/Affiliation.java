package edu.mit.ist.salesforce.sync;

import java.util.Iterator;
import java.util.TreeMap;

public class Affiliation {

	String type;
	String title;
	String office;
	String sfID;
	TreeMap<String, Department> depts;
	
	
	public Affiliation(String type, String title, String office) {
		this.type = type;
		this.title = title;
		this.office = office;
		this.sfID = null;
		depts = new TreeMap<String, Department>();
	}

	

	
	public Affiliation(String type, String title, String office, String sfID) {
		this.type = type;
		this.title = title;
		this.office = office;
		this.sfID = sfID;
		depts = new TreeMap<String, Department>();
	}




	public String getSfID() {
		return sfID;
	}




	public void setSfID(String sfID) {
		this.sfID = sfID;
	}




	public void addDept(Department dept){
		if(dept != null){
			depts.put(dept.getOrgUnitID(), dept);
		}
		
	}
	

	public String getType() {
		return type;
	}


	public String getTitle() {
		return title;
	}


	public String getOffice() {
		return office;
	}


	public TreeMap<String, Department> getDepts() {
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
			Department eachDept = depts.get(orgUnitID);
			result += eachDept.toString();
		}
		
		
		return result;
	}
	
	
}
