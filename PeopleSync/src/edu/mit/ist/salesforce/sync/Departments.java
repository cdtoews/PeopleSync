package edu.mit.ist.salesforce.sync;

import java.util.Iterator;
import java.util.TreeMap;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class Departments {

	TreeMap<Integer, Department> deptMap ;
	
	public static void main(String[] args){
		Departments me = new Departments();
		me.loadDepts();
		System.out.print(me.listDepts());
	}
	
	
	public Departments(){
		deptMap = new TreeMap<Integer, Department>();
	}
	
	public void loadDepts(){

		try {
			HttpResponse<JsonNode> response = Unirest.get("https://mit-public-dev.cloudhub.io/departments/v1/departments")
					  .header("authorization", "Basic NmJmMjhjMGZlN2Y3NGEzYWJlZmRkZWYyYzQ5ZDljMzc6OWQxYjA0ZDgwNDczNDEzMzgxMEZEQTA2Q0M0MUMxNjM=")
					  .header("client_id", "6bf28c0fe7f74a3abefddef2c49d9c37")
					  .header("client_secret", "9d1b04d804734133810FDA06CC41C163")
					  .header("cache-control", "no-cache")
					  .header("postman-token", "e52a374d-1022-49d3-6f1a-ed7bcb6aa4e1")
					  .asJson();
			
			
		} catch (UnirestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String listDepts(){
		return listDepts("\n");
	}
	
	public String listDepts(String cr){
		String result = "";
		Iterator<Integer> it = deptMap.keySet().iterator();
		
		while(it.hasNext()){
			Integer thisID = it.next();
			String thisDept = deptMap.get(thisID).getName();
			result += thisID + " : " + thisDept + cr;
		}
		result += "end of list" + cr;
		return result;
	}
	
}
