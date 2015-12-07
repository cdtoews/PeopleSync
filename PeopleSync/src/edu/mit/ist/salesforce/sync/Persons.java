package edu.mit.ist.salesforce.sync;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

public class Persons {
	static final Logger logger = LogManager.getLogger(Persons.class);
	
	private Connection conn;

	private HashMap<String, Person> sfPeople; //<SFID,Person>
	private  boolean updateSF;
	private Properties props; // names of fields for each SF org
	private HashMap<String,String> deptIDs;
	private int personsCreated = 0;
	private int personsUpdated = 0;
	private int personsDeactivated = 0;
	private int affsCreated = 0;
	private int affsUpdated = 0;
	private int affsDeactivated = 0;
	private int deptAffsCreated = 0;
	private int deptAffsUpdated = 0;
	private int deptAffsDeactivated = 0;
	
	
	
	
	
	public static final String SF_PEOPLE_SQL_BASE = 
			  	"  select   \n " 
			  + " 	-- Person  \n " 
			  + " 	person.sfid as person_sfid,  \n " 
			  + " 	person.<person_account_lookup_field__c> as person_account_lookup_field__c,  \n " 
			  + " 	person.<person_kerb_id_field__c> as person_kerb_id_field__c ,  \n " 
			  + " 	person.<person_first_name_field__c> as person_first_name_field__c,  \n " 
			  + " 	person.<person_middle_name_field__c> as person_middle_name_field__c,  \n " 
			  + " 	person.<person_last_name_field__c> as person_last_name_field__c,  \n " 
			  + " 	person.<person_display_name_field__c> as person_display_name_field__c,  \n " 
			  + " 	person.<person_email_field__c> as person_email_field__c,  \n " 
			  + " 	person.<person_phone_number_field__c> as person_phone_number_field__c,  \n " 
			  + " 	person.<person_website_field__c> as person_website_field__c,  \n " 
			  + " 	person.<person_from_api_field__c> as person_from_api_field__c ,  \n " 
			  + " 	person.<person_active_field__c> as person_active_field__c,  \n " 
			  + " 	person.<person_active_date_field__c> as person_active_date_field__c ,  \n " 
			  + " 	person.<person_inactive_date_field__c> as person_inactive_date_field__c ,  \n " 
			  + " 	-- Affiliation  \n " 
			  + " 	aff.sfid as aff_sfid,  \n " 
			  + " 	aff.<affiliation_type_field__c> as affiliation_type_field__c,  \n " 
			  + " 	aff.<affiliation_title_field__c> as affiliation_title_field__c,  \n " 
			  + " 	aff.<affiliation_office_field__c> as affiliation_office_field__c,  \n " 
			  + " 	aff.<affiliation_person_lookup_field__c> as affiliation_person_lookup_field__c,  \n " 
			  + " 	aff.<affiliation_from_api_field__c> as affiliation_from_api_field__c,  \n " 
			  + " 	aff.<affiliation_active_field__c> as affiliation_active_field__c,  \n " 
			  + " 	aff.<affiliation_active_date_field__c> as affiliation_active_date_field__c,  \n " 
			  + " 	aff.<affiliation_inactive_date_field__c> as affiliation_inactive_date_field__c,  \n " 
			  + " 	-- DepartmentAffiliation  \n " 
			  + " 	deptaff.sfid as deptaff_sfid,  \n " 
			  + " 	COALESCE(deptaff.<deptaff_orgunit_id_field__c>, '') as deptaff_orgunit_id_field__c,  \n " 
			  + " 	deptaff.<deptaff_affiliation_lookup_field__c> as deptaff_affiliation_lookup_field__c,  \n " 
			  + " 	deptaff.<deptaff_department_lookup_field__c> as deptaff_department_lookup_field__c,  \n " 
			  + " 	deptaff.<deptaff_from_api_field__c> as deptaff_from_api_field__c,  \n " 
			  + " 	deptaff.<deptaff_active_field__c> as deptaff_active_field__c,  \n " 
			  + " 	deptaff.<deptaff_active_date_field__c> as deptaff_active_date_field__c,  \n " 
			  + " 	deptaff.<deptaff_inactive_date_field__c> as deptaff_inactive_date_field__c  \n " 
			  + " from     \n " 
			  + " 	<schema_name__c>.<person_object_name__c> as person     \n " 
			  + " left join      \n " 
			  + " 	<schema_name__c>.<affiliation_object_name__c> as aff     \n " 
			  + " 	on     \n " 
			  + " 		aff.<affiliation_person_lookup_field__c> = person.sfid     \n " 
			  + " 		and aff.<affiliation_active_field__c> = true	\n "
			  + "		and aff.<affiliation_from_api_field__c> = true	\n "
			  + " left join     \n " 
			  + " 	<schema_name__c>.<deptaff_object_name__c> as deptaff     \n " 
			  + " 	on     \n " 
			  + " 		deptaff.<deptaff_affiliation_lookup_field__c> = aff.sfid  \n "
			  + " 		and deptaff.<deptaff_active_field__c> = true	\n "
			  + "		and deptaff.<deptaff_from_api_field__c> = true	\n "
			  + " where 	\n "
			  + " 		person.<person_active_field__c> = true	\n "
			  + "		and person.<person_from_api_field__c> = true	\n "
			  + " order by person.createddate asc	\n ";
			  
	String SF_PEOPLE_SQL;
	
	public static final String SF_DEACTIVEATE_PERSON_SQL_BASE = "update <schema_name__c>.<person_object_name__c> set <person_active_field__c> = false, <person_inactive_date_field__c> = current_date where sfid = ?";
	String SF_DEACTIVEATE_PERSON_SQL;
	PreparedStatement deactivatePersonPS;
	
	public static final String SF_DEACTIVEATE_AFF_SQL_BASE = "update <schema_name__c>.<affiliation_object_name__c> set <affiliation_active_field__c> = false, <affiliation_inactive_date_field__c> = current_date where sfid = ?";
	String SF_DEACTIVEATE_AFF_SQL;
	PreparedStatement deactivateAffPS;
	
	public static final String SF_DEACTIVEATE_DEPTAFF_SQL_BASE = "update <schema_name__c>.<deptaff_object_name__c> set <deptaff_active_field__c> = false, <deptaff_inactive_date_field__c> = current_date where sfid = ?";
	String SF_DEACTIVEATE_DEPTAFF_SQL;
	PreparedStatement deactivateDeptAffPS;
	
	public static final String SF_UPDATE_AFF_SQL_BASE = "update <schema_name__c>.<affiliation_object_name__c> set <affiliation_office_field__c> = ? where sfid = ?";
	String SF_UPDATE_AFF_SQL;
	PreparedStatement updateAffPS;
	
	public static final String SF_DEPT_LIST_SQL_BASE = "select <department_orgunitid_field__c> department_orgunitid_field__c, sfid from <schema_name__c>.<department_object_name__c>  where <department_from_api_field__c> = true and <department_active_field__c> = true;";
	String SF_DEPT_LIST_SQL;
	
	public static final String SF_GET_AFFILIATION_SFID_SQL_BASE = "select sfid from <schema_name__c>.<affiliation_object_name__c> where id = ?";
	String SF_GET_AFFILIATION_SFID_SQL;
	PreparedStatement getAffsfidPS;
	
	public static final String INSERT_LOG_SQL_BASE = "insert into <schema_name__c>.<log_object_name__c> (  <log_datetime_field__c> , <log_text_field__c>) values (current_timestamp, ?)";
	String INSERT_LOG_SQL;
	
	public static final String TRIM_LOGS_SQL_BASE = "delete from <schema_name__c>.<log_object_name__c> where <log_datetime_field__c> < current_date - <log_archive_days__c> ";
	String TRIM_LOGS_SQL;
	
	public static final String SF_UPDATE_PERSON_SQL_BASE = 
			  			" update <schema_name__c>.<person_object_name__c>  \n " 
					  + " 	set  \n " 
					  + " 	-- <person_account_lookup_field__c> = ?,  \n " 
					  + " 	<person_first_name_field__c> = ?,  \n " 
					  + " 	<person_middle_name_field__c> = ?,  \n " 
					  + " 	<person_last_name_field__c> = ?,  \n " 
					  + " 	<person_display_name_field__c> = ?,  \n " 
					  + " 	<person_email_field__c> = ?,  \n " 
					  + " 	<person_phone_number_field__c> = ?,  \n " 
					  + " 	<person_website_field__c> = ?  \n " 
					  + " where  \n " 
					  + " 	sfid = ? \n ";
	String SF_UPDATE_PERSON_SQL;
	PreparedStatement updatePersonPS;
	
	public static final String SF_INSERT_AFFILIATION_BASE = 
			  			" insert into <schema_name__c>.<affiliation_object_name__c>  \n " 
					  + " 	(  \n " 
					  + " 		<affiliation_active_date_field__c>,   \n " 
					  + " 		<affiliation_active_field__c>,  \n " 
					  + " 		affiliation_type__c,  \n " 
					  + " 		<affiliation_person_lookup_field__c>,  \n " 
					  + " 		<affiliation_from_api_field__c>,  \n " 
					  + " 		<affiliation_office_field__c>,  \n " 
					  + " 		<affiliation_title_field__c>  \n " 
					  + " 	)  \n " 
					  + " values  \n " 
					  + " 	(  current_date, true , ?, ?, true, ?, ? ) \n " ;
	String SF_INSERT_AFFILIATION;				 
	PreparedStatement insertAffPS;
	
	public static final String SF_INSERT_DEPTAFF_SQL_BASE =
						"  insert into <schema_name__c>.<deptaff_object_name__c>  \n " 
					  + " 	(  \n " 
					  + " 		<deptaff_active_date_field__c>,   \n " 
					  + " 		<deptaff_active_field__c>,  \n " 
					  + " 		<deptaff_from_api_field__c>,  \n " 
					  + " 		<deptaff_affiliation_lookup_field__c>,  \n " 
					  + " 		<deptaff_department_lookup_field__c>,  \n " 
					  + " 		<deptaff_orgunit_id_field__c>  \n " 
					  + " 	)  \n " 
					  + " values  \n " 
					  + " 	(  current_date, true, true, ?, ?, ?) \n " ;
	String SF_INSERT_DEPTAFF_SQL;
	PreparedStatement insertDeptAff;
	
	public static final String ORPHANED_AFF_SQL_BASE = 
			  			" select   \n " 
					  + " 	aff.sfid  \n " 
					  + " from   \n " 
					  + " 	<schema_name__c>.<affiliation_object_name__c> aff   \n " 
					  + " left join   \n " 
					  + " 	<schema_name__c>.<person_object_name__c> person   \n " 
					  + " on   \n " 
					  + " 	aff.<affiliation_person_lookup_field__c> = person.sfid   \n " 
					  + " 	and person.<person_active_field__c> = true  \n " 
					  + " 	and person.<person_from_api_field__c> = true  \n " 
					  + " where   \n " 
					  + " 	person.sfid is null  \n " 
					  + " 	and aff.<affiliation_active_field__c> = true  \n " 
					  + " 	and aff.<affiliation_from_api_field__c> = true \n ";
	String ORPHANED_AFF_SQL;
	
	public static final String ORPHANED_DEPTAFF_SQL_BASE =
			  			" select   \n " 
					  + " 	deptaff.sfid  \n " 
					  + " from   \n " 
					  + " 	<schema_name__c>.<deptaff_object_name__c> deptaff   \n " 
					  + " left join   \n " 
					  + " 	<schema_name__c>.<affiliation_object_name__c> aff   \n " 
					  + " on   \n " 
					  + " 	deptaff.<deptaff_affiliation_lookup_field__c> = aff.sfid   \n " 
					  + " 	and aff.<affiliation_active_field__c> = true  \n " 
					  + " 	and aff.<affiliation_from_api_field__c> = true  \n " 
					  + " where   \n " 
					  + " 	aff.sfid is null  \n " 
					  + " 	and deptaff.<deptaff_active_field__c> = true  \n " 
					  + " 	and deptaff.<deptaff_from_api_field__c> = true; \n ";
	String ORPHANED_DEPTAFF_SQL;
	
	
	/*
	 * 
	 * QUERY after re-conflaburaterating it with dev1 sandbox
  select   
  	-- Person  
  	person.sfid as person_sfid,  
  	person.accountid as person_account_lookup_field__c,  
  	person.kerberos_id__c as person_kerb_id_field__c ,  
  	person.firstname as person_first_name_field__c,  
  	person.middle_name__c as person_middle_name_field__c,  
  	person.lastname as person_last_name_field__c,  
  	person.email as person_email_field__c,  
  	person.phone as person_phone_number_field__c,  
  	person.website__c as person_website_field__c,  
  	person.from_api__c as person_from_api_field__c ,  
  	person.active__c as person_active_field__c,  
  	person.active_date__c as person_active_date_field__c ,  
  	person.inactive_date__c as person_inactive_date_field__c ,  
  	-- Affiliation  
  	aff.sfid as aff_sfid,  
  	aff.affiliation_type__c as affiliation_type_field__c,  
  	aff.office__c as affiliation_office_field__c,  
  	aff.contact__c as affiliation_person_lookup_field__c,  
  	aff.from_api__c as affiliation_from_api_field__c,  
  	aff.active__c as affiliation_active_field__c,  
  	aff.active_date__c as affiliation_active_date_field__c,  
  	aff.inactive_date__c as affiliation_inactive_date_field__c,  
  	-- DepartmentAffiliation  
  	deptaff.sfid as deptaff_sfid,  
  	deptaff.orgunitid__c as deptaff_orgunit_id_field__c,  
  	deptaff.affiliation__c as deptaff_affiliation_lookup_field__c,  
  	deptaff.department__c as deptaff_department_lookup_field__c,  
  	deptaff.from_api__c as deptaff_from_api_field__c,  
  	deptaff.active__c as deptaff_active_field__c,  
  	deptaff.active_date__c as deptaff_active_date_field__c,  
  	deptaff.inactive_date__c as deptaff_inactive_date_field__c  
  from     
  	sfdev1.contact as person     
  left join      
  	sfdev1.affiliation__c as aff     
  	on     
  		aff.contact__c = person.sfid     
  left join     
  	sfdev1.department_affiliation__c as deptaff     
  	on     
  		deptaff.affiliation__c = aff.sfid ; 
 

	 * 
	 */
	
	public static final String TEST_JSON_STRING;
	
	static{
		TEST_JSON_STRING =  "{"
				+ "    \"kerberosId\": \"wwhite\","
				+ "    \"givenName\": \"Walter\","
				+ "    \"familyName\": \"White\","
				+ "    \"middleName\": \"Danger\","
				+ "    \"displayName\": \"Walter White\","
				+ "    \"email\": \"wwhite@mit.edu\","
				+ "    \"phoneNumber\": \"617-252-1906\","
				+ "    \"website\": \"http://www.broadinstitute.org\","
				+ "    \"affiliations\": ["
				+ "      {"
				+ "        \"type\": \"staff\","
				+ "        \"title\": \"Professor\","
				+ "        \"office\": \"NE30-6013\","
				+ "            \"Primary\": \"Yes\","
				+ "        \"departments\": ["
				+ "          {"
				+ "            \"orgUnitId\": \"10000429\","
				+ "            \"name\": \"Department of Biology\""
				+ "          },"
				+ "{"
				+ "            \"orgUnitId\": \"10000887\","
				+ "            \"name\": \"Department of Radiology\""
				+ "          }"
				+ ""
				+ "        ]"
				+ "      },"
				+ "                    {"
				+ "        \"type\": \"staff\","
				+ "        \"title\": \"Professor\","
				+ "        \"office\": \"NW30-5548\","
				+ "            \"Primary\": \"No\","
				+ "        \"departments\": ["
				+ "          {"
				+ "            \"orgUnitId\": \"10000129\","
				+ "            \"name\": \"Department of Chemistry\""
				+ "          }"
				+ "        ]"
				+ "      }"
				+ "    ]"
				+ "  }";
	}
	
	public static void main(String[] args) throws UnirestException, URISyntaxException, SQLException{
		//Persons me = new Persons(null,new Properties());
		//me.getAPIperson("elander");
		//me.getAPIperson("ctoews");
		Main main = new Main();
		String[] names = new String[]{"bogus","elander","ctoews"};
		for(String name:names){
			try{
				System.out.print(getAPIperson(name).toString());
			}catch(Exception ex){
				//logger.error("Whoopsie!",ex);
			}
		}
		
		
		//JSONObject testJson = new JSONObject(TEST_JSON_STRING);
		//Person testPerson = parsePersonJson(testJson); 
		//System.out.print(testPerson.toString());
	}
	
	//-------------  CONSTRUCTOR  ----------------------
	public Persons(Connection conn,Properties props) throws SQLException{
		this.conn=conn;
		this.props = props;
		SF_PEOPLE_SQL = replaceProps(SF_PEOPLE_SQL_BASE);
		SF_DEACTIVEATE_PERSON_SQL = replaceProps(SF_DEACTIVEATE_PERSON_SQL_BASE);
		SF_DEACTIVEATE_AFF_SQL = replaceProps(SF_DEACTIVEATE_AFF_SQL_BASE);
		SF_DEACTIVEATE_DEPTAFF_SQL = replaceProps(SF_DEACTIVEATE_DEPTAFF_SQL_BASE);
		SF_UPDATE_PERSON_SQL = replaceProps(SF_UPDATE_PERSON_SQL_BASE);
		SF_UPDATE_AFF_SQL = replaceProps(SF_UPDATE_AFF_SQL_BASE);
		SF_DEPT_LIST_SQL = replaceProps(SF_DEPT_LIST_SQL_BASE);
		SF_INSERT_AFFILIATION = replaceProps(SF_INSERT_AFFILIATION_BASE);
		SF_GET_AFFILIATION_SFID_SQL = replaceProps(SF_GET_AFFILIATION_SFID_SQL_BASE);
		SF_INSERT_DEPTAFF_SQL = replaceProps(SF_INSERT_DEPTAFF_SQL_BASE);
		INSERT_LOG_SQL = replaceProps(INSERT_LOG_SQL_BASE);
		TRIM_LOGS_SQL = replaceProps(TRIM_LOGS_SQL_BASE);
		ORPHANED_AFF_SQL = replaceProps(ORPHANED_AFF_SQL_BASE);
		ORPHANED_DEPTAFF_SQL = replaceProps(ORPHANED_DEPTAFF_SQL_BASE);
		
		sfPeople = new HashMap<String, Person>();  //sfid,Person
		deptIDs = new HashMap<String,String>();
		
		String updateSFtext = props.getProperty("update_salesforce__c");
		updateSF = Main.readSFcheckbox(updateSFtext);
		loadDeptIDs();
		//System.out.println("\n" + SF_PEOPLE_SQL + "\n");
	}//end of constructor
	
	public void loadDeptIDs() throws SQLException{
		logger.info("TASK=LOADING_DEPTS STATUS=STARTING");
		PreparedStatement deptPS = conn.prepareStatement(SF_DEPT_LIST_SQL);
		logger.debug("SF_DEPT_LIST_SQL=" + SF_DEPT_LIST_SQL);
		ResultSet deptRS = deptPS.executeQuery();
		while(deptRS.next()){
			String orgunitID = deptRS.getString("department_orgunitid_field__c");
			String sfid = deptRS.getString("sfid");
			logger.trace("TASK=LOADING_DEPTS ORGUNITID=" + orgunitID + " SFID=" + sfid);
			deptIDs.put(orgunitID, sfid);
		}
		deptRS.close();
		deptPS.close();
		logger.info("TASK=LOADING_DEPTS STATUS=FINISHED");
	}//end loadDeptIDs
	
	/**
	 * deactivates orphaned affiliations, and department-affiliations
	 */
	public void clearOrphans(){
		logger.info("TASK=CLEARING_ORPHAN_AFFILIATES STATUS=STARTING");
		try {
			logger.trace("TASK=CLEARING_ORPHAN_AFFILIATES SQL=" + ORPHANED_AFF_SQL);
			PreparedStatement oaffPS = conn.prepareStatement(ORPHANED_AFF_SQL);
			ResultSet oaffRS = oaffPS.executeQuery();
			while(oaffRS.next()){
				String oSFID = oaffRS.getString("sfid");
				Affiliation thisAff = new Affiliation(null,null,null,oSFID);
				logger.warn(" TASK=CLEARING_ORPHAN_AFFILIATES STATUS=FOUND_ORPHAN SFID=" + oSFID);
				deactivateAff(thisAff);
			}
			oaffRS.close();
			oaffPS.close();
		} catch (SQLException ex) {
			logger.error("TASK=CLEARING_ORPHAN_AFFILIATES STATUS=EXCEPTION",ex);
		}
		logger.info("TASK=CLEARING_ORPHAN_AFFILIATES STATUS=FINISHED");
		
		
		logger.info("TASK=CLEARING_ORPHAN_DEPTAFFS STATUS=STARTED");
		//ORPHANED_DEPTAFF_SQL
		try {
			logger.trace("TASK=CLEARING_ORPHAN_DEPTAFFS SQL=" + ORPHANED_DEPTAFF_SQL);
			PreparedStatement odaffPS = conn.prepareStatement(ORPHANED_DEPTAFF_SQL);
			ResultSet odaffRS = odaffPS.executeQuery();
			while(odaffRS.next()){
				String oSFID = odaffRS.getString("sfid");
				DeptAff thisDeptAff = new DeptAff(null,oSFID);
				logger.warn(" TASK=CLEARING_ORPHAN_DEPTAFFS STATUS=FOUND_ORPHAN SFID=" + oSFID);
				this.deactivateDeptAff(thisDeptAff);
			}
			odaffRS.close();
			odaffPS.close();
		} catch (SQLException ex) {
			logger.error("TASK=CLEARING_ORPHAN_DEPTAFFS STATUS=EXCEPTION",ex);
		}
		logger.info("TASK=CLEARING_ORPHAN_DEPTAFFS STATUS=FINISHED");
		
		
	}
	
	
	/**
	 * Compares and updates Person, Affiliation, and Department-Affiliation object
	 */
	public void compareUpdatePersons(){
		logger.info("TASK=COMPARE_UPDATE_PERSONS STATUS=STARTING");
		try {
			deactivatePersonPS = conn.prepareStatement(SF_DEACTIVEATE_PERSON_SQL);
			deactivateAffPS = conn.prepareStatement(SF_DEACTIVEATE_AFF_SQL);
			deactivateDeptAffPS = conn.prepareStatement(SF_DEACTIVEATE_DEPTAFF_SQL);
			updatePersonPS = conn.prepareStatement(SF_UPDATE_PERSON_SQL);
			insertAffPS = conn.prepareStatement(SF_INSERT_AFFILIATION, Statement.RETURN_GENERATED_KEYS);
			getAffsfidPS = conn.prepareStatement(SF_GET_AFFILIATION_SFID_SQL);
			insertDeptAff = conn.prepareStatement(SF_INSERT_DEPTAFF_SQL);
			updateAffPS = conn.prepareStatement(SF_UPDATE_AFF_SQL);
		} catch (SQLException e) {
			logger.fatal(" TASK=CREATING_PREPARED_STATEMENTS STATUS=EXCEPTION" , e);
			return;
		}
		
		//first remove orphans
		clearOrphans();
		
		//we are going to iterate SFpersons
		Iterator<String> sfIT = sfPeople.keySet().iterator();
		while(sfIT.hasNext()){
			String sfid = sfIT.next();
			Person sfPerson = sfPeople.get(sfid);
			String kerbID = sfPerson.getKerbID();
			Person apiPerson = getAPIperson(kerbID);
			logger.trace("TASK=COMPARE_UPDATE_PERSONS STATUS=LOADING_PEOPLE SFID=" + sfid + " KERBID=" + kerbID);
			try{
				//if apiPerson is null, this person isn't in the system, or he's Neo...
				if(apiPerson == null){
					//apiPerson is null, that means a bad response, or an exception
					logger.error("TASK=COMPARE_UPDATE_PERSONS STATUS=ERROR SFID=" + sfid + " KERBID=" + kerbID);
				}else if(apiPerson.isMissing()){
					//this means a valid response from the api, but not a person
					logger.info("TASK=COMPARE_UPDATE_PERSONS STATUS=DEACTIVATING SFID=" + sfid + " KERBID=" + kerbID);
					deactivatePerson(sfPerson);
				}else if(sfPerson.isDuplicate()){
					logger.warn(" TASK=COMPARE_UPDATE_PERSONS STATUS=FOUND_DUPLICATE_KERBID KERBID=" + sfPerson.getKerbID() + " SFID=" + sfPerson.getSfID());
					deactivatePerson(sfPerson);
				}else if(sfPerson.personFieldsEqual(apiPerson)){
					//all is well, smiles all around
				}else{
					//we have two persons matching kerbID, now we just need to update SF person fields
					logger.trace("TASK=COMPARE_UPDATE_PERSONS STATUS=UPDATING_PERSON SFID=" + sfid + " KERBID=" + kerbID);
					updatePerson(apiPerson,sfPerson.getSfID());
				}//end of if/else for person
			}catch(Exception ex){
				//kicking and error for THIS person
				logger.error(" TASK=COMPARING_PERSON STATUS=EXCEPTION KERBID="  + kerbID,ex);
			}
			if(apiPerson != null){
				compareUpdateAffs(apiPerson.getAffs(),sfPerson.getAffs(),sfPerson.getSfID());
				logger.info("TASK=COMPARE_UPDATE_PERSONS STATUS=FINISHED KERBID=" + kerbID);
			}
			
		}//end of while sfPeople has next
		
		
		
		
		
		
		try {
			deactivatePersonPS.close();
			deactivateAffPS.close();
			deactivateDeptAffPS.close();
			updatePersonPS.close();
			insertAffPS.close();
			getAffsfidPS.close();
			insertDeptAff.close();
			updateAffPS.close();
		} catch (SQLException e) {
			logger.error(" TASK=CLOSING_PREPAREDSTATEMENTS STATUS=EXCEPTION", e);
		}
		if(updateSF){
			trimLogs();
			
		}
		
	}//end of compareUpdate
	
	/**
	 * Compares and updates Affiliations, needs api,SF HashSets, also needs SFID of the person record (possibly a contact) for creating link to person
	 * @param apiAffs Affiliations from API for this person
	 * @param sfAffs Affiliations from Salesforce for this person
	 * @param personSFID  Salesforce 18 character ID of the person object these affilaition are associated with
	 */
	private void compareUpdateAffs(HashSet<Affiliation> apiAffs,HashSet<Affiliation> sfAffs, String personSFID){
		logger.info("TASK=COMPARE_UPDATE_AFFILIATIONS STATUS=STARTING");
//		HashSet<Affiliation> toUpdate = new HashSet<Affiliation>();
//		HashSet<Affiliation> toAdd = new HashSet<Affiliation>();
//		HashSet<Affiliation> toRemove = new HashSet<Affiliation>();
		
		//iterate API Affs
		Iterator<Affiliation> aIT = apiAffs.iterator();
		while(aIT.hasNext()){
			Affiliation apiAff = aIT.next();
			boolean match = false;
			
			//inner loop over SF Affs
			Iterator<Affiliation> sfIT = sfAffs.iterator();
			sfLoop:
			while(sfIT.hasNext()){
				Affiliation sfAff = sfIT.next();
				logger.trace(" TASK=COMPARE_UPDATE_AFFILIATIONS SFID=" + sfAff.getSfID());
				if(sfAff.equalValues(apiAff)){
					logger.trace(" TASK=COMPARE_UPDATE_AFFILIATIONS SFID=" + sfAff.getSfID() + " STATUS=EQUAL_VALUES");
					aIT.remove();
					sfIT.remove();
					match = true;
					break sfLoop;
				}else if(sfAff.almostEqualValues(apiAff)){
					updateAffiliation(apiAff,sfAff.getSfID());
					aIT.remove();
					sfIT.remove();
					match = true;
					break sfLoop;
				}//end of if/else for aff equality
			}//end of inner Sf Aff iterator
			
			if(!match){
				//we didn't find a match, let's add this to SF
				logger.info(" TASK=COMPARE_UPDATE_AFFILIATIONS PERSON_SFID=" + personSFID + " STATUS=NO_MATCH_INSERTING");
				insertAffiliation(apiAff,personSFID);
				
			}
			
		}//end of API Aff iterator
		
		//now we need to deactivate leftover sfAffs
		Iterator<Affiliation> sfIT = sfAffs.iterator();
		logger.info(" TASK=COMPARE_UPDATE_AFFILIATIONS STATUS=REMOVING_OLD_AFFS");
		while(sfIT.hasNext()){
			Affiliation removeAff = sfIT.next();
			logger.debug(" TASK=COMPARE_UPDATE_AFFILIATIONS STATUS=REMOVING_OLD_AFFS SFID=" + removeAff.getSfID() );
			deactivateAff(removeAff);
			
			
		}
		logger.info("TASK=COMPARE_UPDATE_AFFILIATIONS STATUS=FINISHED");
	}//end of compareUpdateAffs
	
	/**
	 * Inserts Affiliation and deptaff hash inside aff
	 * @param aff Affiliation needing to be inserted
	 * @param personSFID 18 character ID of the person associated with Affiliation
	 */
	private void insertAffiliation(Affiliation aff, String personSFID){
		if(!updateSF){
			logger.info(" STATUS=NOT_UPDATING TASK=INSERTING_AFFILIATION PERSON_SFID=" + personSFID);
			return;
		}
		affsCreated++;
		try{
			logger.debug(" TASK=INSERTING_AFFILIATION STATUS=STARTING");
			insertAffPS.clearParameters();
			int c=1;
			insertAffPS.setString(c++, aff.getType());
			insertAffPS.setString(c++, personSFID);
			insertAffPS.setString(c++, aff.getOffice());
			insertAffPS.setString(c++, aff.getTitle());
			insertAffPS.executeUpdate();
			
			ResultSet keyRS = insertAffPS.getGeneratedKeys();
			Integer rowid;
			if(keyRS.next()){
				rowid = keyRS.getInt("id");
			}else{
				logger.error("TASK=INSERTING_AFFILIATION STATUS=ERROR NOTE='didn't get row ID back when inserting'");
				return;
			}
			
			//we have to get the Affiliation SFID
			//prep the statement
			getAffsfidPS.clearParameters();
			getAffsfidPS.setInt(1, rowid);
			logger.trace(" TASK=LOOKING_FOR_AFF_SFID ROW_ID=" + rowid);
			String affSFID = null;
			searchForSfidLoop:
			for(int i = 1;i < 8 ; i++){
				ResultSet sfidRS = getAffsfidPS.executeQuery();
				boolean foundID = false;
				if(sfidRS.next()){
					String idAttempt = sfidRS.getString("sfid");
					if(!sfidRS.wasNull()){
						foundID = true;
						affSFID = idAttempt;
						logger.trace(" TASK=LOOKING_FOR_AFF_SFID ROW_ID=" + rowid + " SFID=" + affSFID);
					}
				}
				if(!foundID){
					logger.debug("TASK=INSERTING_AFFILIATION STATUS=WAITING_ON_AFF_SFID ATTEMPT=" + i + " ROW_ID=" + rowid);
					Thread.sleep((i * i) * 1000);//wait 1,4,9,16,25,36,49,64 seconds for a sfid to show up
				}else{
					//we have a value
					break searchForSfidLoop;
				}
			}//end loop looking for sfid
			
			//did we get an SFID
			if(affSFID == null){
				logger.error("TASK=INSERTING_AFFILIATION STATUS=ERROR NOTE='didn't get a salesforce ID back from inserting Affiliation'");
				return;
			}
			
			//now we go through and insert DeptAffs
			TreeMap<String, DeptAff> deptAffs = aff.getDeptAffs();
			Iterator<String> daIT = deptAffs.keySet().iterator();
			while(daIT.hasNext()){
				String orgunitID = daIT.next();
				DeptAff deptAff = deptAffs.get(orgunitID);
				deptAff.setAffiliationSFID(affSFID);
				//get dept sfid
				String deptID = deptIDs.get(orgunitID);
				deptAff.setDepartmentSFID(deptID);
				if(deptID == null){
					logger.error("TASK=INSERTING_AFFILIATION STATUS=NO_DEPT_FOUND ORDUNITID=" + orgunitID + " AFFILIATE_SFID=" + affSFID);
				}else{
					//insert deptaff
					insertDeptAff(deptAff);
				}
			}
			
			
			
			
		}catch(Exception ex){
			logger.error(" TASK=INSERTING_AFFILIATION STATUS=EXCEPTION PERSON_SFID=" + personSFID,ex);
		}
	}//end of insertAffiliation
	
	
	
	/**
	 * insert a deptAff into Salesforce
	 * @param deptAff deptAff must have affiliationSFID and deptSFID populated, or 
	 */
	private void insertDeptAff(DeptAff deptAff){
		if(!updateSF){
			logger.info(" STATUS=NOT_UPDATING TASK=INSERTING_DEPTAFF DEPT_SFID=" + deptAff.getDepartmentSFID());
			return;
		}
		//let's verify that we have the values we need
		if(!deptAff.isInsertable()){
			logger.error("TASK=INSERTING_DEPTAFF STATUS=DEPTAFF_NOT_INSERTABLE AFFILIATINID=" + deptAff.getAffiliationSFID() + " DEPARTMENTID=" + deptAff.getDepartmentSFID() + " ORGUNITID=" + deptAff.getOrgUnitID() );
			return;
		}
		
		
		deptAffsCreated++;
		try{
			insertDeptAff.clearParameters();
			int c=1;
			insertDeptAff.setString(c++, deptAff.getAffiliationSFID());
			insertDeptAff.setString(c++, deptAff.getDepartmentSFID());
			insertDeptAff.setString(c++, deptAff.getOrgUnitID());
			logger.debug(" TASK=INSERTING_DEPTAFF AFFILIATION_SFID=" + deptAff.getAffiliationSFID() + " DEPARTMENT_SFID=" + deptAff.getDepartmentSFID() + " ORGUNITID=" + deptAff.getOrgUnitID());
			insertDeptAff.executeUpdate();
		}catch(Exception ex){
			logger.error(" TASK=INSERTING_DEPTAFF STATUS=EXCEPTION",ex);
		}
	}
	
	
	/**
	 * Updates Office field only for the Affiliation object
	 * @param aff object holding data to update into SF, normally Affiliation from API
	 * @param sfid Salesforce 18 character ID of object to update
	 */
	private void updateAffiliation(Affiliation aff, String sfid){
		if(!updateSF){
			logger.info(" STATUS=NOT_UPDATING TASK=UPDATING_AFFILIATION AFFILIATION_SFID=" + sfid);
			return;
		}
		affsUpdated++;
		try{
			logger.debug("TASK=UPDATING_AFFILIATION STATUS=STARTING SFID=" + sfid);
			updateAffPS.clearParameters();
			int c=1;
			updateAffPS.setString(c++, aff.getOffice());
			updateAffPS.setString(c++, sfid);
			int numUpdated = updateAffPS.executeUpdate();
			if(numUpdated == 1){
				logger.info(" TASK=UPDATING_AFFILIATION STATUS=SUCCESS  SFID=" + sfid);
			}else{
				logger.error(" TASK=UPDATING_AFFILIATION STATUS=SUCCESS SFID=" + sfid + " NUMUPDATED=" + numUpdated);
			}
		}catch(Exception ex){
			logger.error("TASK=UPDATING_AFFILIATION STATUS=EXCEPTION SFID=" + sfid,ex);
		}
	}
	
	
	/**
	 * @param person object holding data to update into SF, normally person from API
	 * @param sfid Salesforce 18 character ID of object to update
	 */
	private void updatePerson(Person person, String sfid){
		if(!updateSF){
			logger.info(" STATUS=NOT_UPDATING TASK=UPDATE_PERSON PERSON_SFID=" + sfid);
			return;
		}
		personsUpdated++;
		try{
			logger.debug(" TASK=UPDATE_PERSON STATUS=STARTING SFID=" + sfid + " KERBID=" + person.getKerbID());
			updatePersonPS.clearParameters();
			int c=1;
			updatePersonPS.setString(c++, person.getFirstName());
			updatePersonPS.setString(c++, person.getMiddleName());
			updatePersonPS.setString(c++, person.getLastName());
			updatePersonPS.setString(c++, person.getDisplayName());
			updatePersonPS.setString(c++, person.getEmail());
			updatePersonPS.setString(c++, person.getPhoneNumber());
			updatePersonPS.setString(c++, person.getWebsite());
			updatePersonPS.setString(c++, sfid);
			int numUpdated = updatePersonPS.executeUpdate();
			if(numUpdated == 1){
				logger.info(" TASK=UPDATE_PERSON STATUS=SUCCESS KERBID=" + person.getKerbID() + " SFID=" + person.getSfID());
			}else{
				logger.error(" TASK=UPDATE_PERSON STATUS=SUCCESS KERBID=" + person.getKerbID() + " SFID=" + person.getSfID() + " NUMUPDATED=" + numUpdated);
			}
			
			
		}catch(SQLException ex){
			logger.error(" TASK=UPDATING_PERSON STATUS=EXCEPTION SFID=" + sfid + " KERBID=" + person.getKerbID(),ex);
		}
	}//end of updatePerson
	
	/**
	 * deactivates a person, sets active=false, inactive_date = current_date, deactivates associated affiliations
	 * @param person person object, must contain SFID
	 */
	private void deactivatePerson(Person person) {
		if(!updateSF){
			logger.info(" STATUS=NOT_UPDATING TASK=DEACTIVATE_PERSON PERSON_SFID=" + person.getSfID());
			return;
		}
		personsDeactivated++;
		try{
			logger.debug(" TASK=DEACTIVATE_PERSON STATUS=STARTING KERBID=" + person.getKerbID() + " SFID=" + person.getSfID());
			deactivatePersonPS.clearParameters();
			deactivatePersonPS.setString(1, person.getSfID());
			int numUpdated = deactivatePersonPS.executeUpdate();
			if(numUpdated == 1){
				logger.info(" TASK=DEACTIVATE_PERSON STATUS=SUCCESS KERBID=" + person.getKerbID() + " SFID=" + person.getSfID());
			}else{
				logger.error(" TASK=DEACTIVATE_PERSON STATUS=SUCCESS KERBID=" + person.getKerbID() + " SFID=" + person.getSfID() + " NUMUPDATED=" + numUpdated);
			}
		}catch(SQLException ex){
			logger.error(" TASK=DEACTIVATE_PERSON STATUS=EXCEPTION KERBID=" + person.getKerbID() + " SFID=" + person.getSfID(),ex);
		}
		//now we deactivate each Affiliation
		HashSet<Affiliation> affs = person.getAffs();
		for(Affiliation aff:affs){
			deactivateAff(aff);
		}
		
	}//end of deactivatePerson
	
	/**
	 * deactivates an Affiliation object in Salesforce, and all child department-affiliation objects 
	 * @param aff Affiliation object, must contain SFID
	 */
	private void deactivateAff( Affiliation aff) {
		if(!updateSF){
			logger.info(" STATUS=NOT_UPDATING TASK=DEACTIVATE_AFFILIATION AFFILIATION_SFID=" + aff.getSfID());
			return;
		}
		affsDeactivated++;
		try{
			logger.debug(" TASK=DEACTIVATE_AFFILIATION STATUS=STARTING  SFID=" + aff.getSfID());
			deactivateAffPS.clearParameters();
			deactivateAffPS.setString(1, aff.getSfID());
			int numUpdated = deactivateAffPS.executeUpdate();
			if(numUpdated == 1){
				logger.info(" TASK=DEACTIVATE_AFFILIATION STATUS=SUCCESS  SFID=" + aff.getSfID());
			}else{
				logger.error(" TASK=DEACTIVATE_AFFILIATION STATUS=SUCCESS  SFID=" + aff.getSfID() + " NUMUPDATED=" + numUpdated);
			}
		}catch(SQLException ex){
			logger.error(" TASK=DEACTIVATE_AFFILIATION STATUS=EXCEPTION  SFID=" + aff.getSfID(),ex);
		}
		
		TreeMap<String, DeptAff> deptAffs = aff.getDeptAffs();
		for(String eachOrg:deptAffs.keySet()){
			DeptAff deptAff = deptAffs.get(eachOrg);
			deactivateDeptAff(deptAff);
		}
		
	}//end of deactivateAff
	
	
	private void deactivateDeptAff(DeptAff deptAff){
		if(!updateSF){
			logger.info(" STATUS=NOT_UPDATING TASK=DEACTIVATE_DEPTAFF DEPTAFF_SFID=" + deptAff.getSfID());
			return;
		}
		deptAffsDeactivated++;
		//deactivateDeptAffPS
		try{
			logger.debug(" TASK=DEACTIVATE_DEPTAFF STATUS=STARTING  SFID=" + deptAff.getSfID());
			deactivateDeptAffPS.clearParameters();
			deactivateDeptAffPS.setString(1, deptAff.getSfID());
			int numUpdated = deactivateDeptAffPS.executeUpdate();
			if(numUpdated == 1){
				logger.info(" TASK=DEACTIVATE_DEPTAFF STATUS=SUCCESS  SFID=" + deptAff.getSfID());
			}else{
				logger.error(" TASK=DEACTIVATE_DEPTAFF STATUS=SUCCESS  SFID=" + deptAff.getSfID() + " NUMUPDATED=" + numUpdated);
			}
		}catch(SQLException ex){
			logger.error(" TASK=DEACTIVATE_DEPTAFF STATUS=EXCEPTION  SFID=" + deptAff.getSfID(),ex);
		}
		
	}//end of deactivateDeptAff
	
	
	/**
	 * as the name implies, it loads Salesforce people that are: active=true, from_api=true
	 */
	public void loadSFpeople(){
		logger.trace("TASK=LOAD_SF_PEOPLE STATUS=STARTING SF_PEOPLE_SQL="  + SF_PEOPLE_SQL );
		HashSet<String> kerbIDs = new HashSet<String>();
		
		 //logThis(" TASK=LOAD_SF_PEOPLE STATUS=STARTING");
		 try{
			 
			 PreparedStatement sfPS = conn.prepareStatement(SF_PEOPLE_SQL);
			 ResultSet sfRS = sfPS.executeQuery();
			 String lastPersonID = "person_initializing";
			 String lastAffID = "aff_initializing";
			 String thisPersonID = "FOO";
			 String thisAffID = "BAR";
			 Person eachPerson = null;
			 Affiliation eachAff = null;
			 DeptAff eachDeptAff = null;
			 while(sfRS.next()){
				 lastAffID = thisAffID;
				lastPersonID = thisPersonID;//reset which was the last one
				thisPersonID = sfRS.getString("person_sfid");
				thisAffID = sfRS.getString("aff_sfid");
				if(thisAffID == null){
					thisAffID = "null";
				}
				
				//----------- take care of last row before loading new row -----------
				//first we always add the deptaff into any existing aff
				if(eachAff != null && eachDeptAff != null ){
					//if there is an existing Affiliation (not first run through), and there is a deptaff (not always have a deptaff)
					eachAff.addDept(eachDeptAff);
				}
				
				//see if we need to rollup aff into person
				if(eachPerson != null && eachAff != null  && !lastAffID.equals(thisAffID)){
					//if there is a person, and an affiliation, and this new row will be a new affiliation
					eachPerson.addAffiliation(eachAff);
				}
				
				
				if(eachPerson != null && !thisPersonID.equals(lastPersonID)){
					//if we have a person (not first pass through loop), and this is a new row for a person
					if(kerbIDs.contains(eachPerson.getKerbID()) ){
						//this is a duplicate
						eachPerson.setDuplicate(true);
					}else{
						//not a duplicate, add kerb id
						kerbIDs.add(eachPerson.getKerbID());
					}
					sfPeople.put(lastPersonID, eachPerson);
					
					
				}
				
				
				
				// ------------------  now load current row  -----------------------
				// == Person ==
				if(!lastPersonID.equals(thisPersonID)){
				//now create new person
					eachPerson = parseRSperson(sfRS);
				}
				
				// == Affiliation ==
				if(!lastAffID.equals(thisAffID)){

					eachAff = parseRSaff(sfRS);
				}
				
				// == Dept/Aff ==
				//doesn't always have a deptAff...
				eachDeptAff = parseRSdeptAff(sfRS);
				
				
				
			 }//end of while(sfRS.next
			
			 //clean up from last row
			//first we always add the deptaff into any existing aff
				if(eachAff != null && eachDeptAff != null ){
					//if there is an existing Affiliation (not first run through), and there is a deptaff (not always have a deptaff)
					eachAff.addDept(eachDeptAff);
				}
				
				//see if we need to rollup aff into person
				if(eachPerson != null && eachAff != null ){
					//if there is a person, and an affiliation, and this new row will be a new affiliation
					eachPerson.addAffiliation(eachAff);
				}
				
				
				if(eachPerson != null ){
					if(kerbIDs.contains(eachPerson.getKerbID()) ){
						//this is a duplicate
						eachPerson.setDuplicate(true);
					}else{
						//not a duplicate, add kerb id
						kerbIDs.add(eachPerson.getKerbID());
					}
					//if we have a person (not first pass through loop), and this is a new row for a person
					sfPeople.put(thisPersonID, eachPerson);
				}
				
			 
			 
			 
			 
			 sfRS.close();
			 sfPS.close();
		 }catch(SQLException ex){
			 ex.printStackTrace();
		 }
		
		
		 logger.info(" TASK=LOAD_SF_PEOPLE STATUS=FINISHED");
		 logger.trace("----------------OUTPUT OF SFPEOPLE SIZE=" + sfPeople.size() + " ------------------");
		 Iterator<String> pIT = sfPeople.keySet().iterator();
		 while(pIT.hasNext()){
			 String pID = pIT.next();
			 Person thisPerson = sfPeople.get(pID);
			 logger.trace(thisPerson.toString());
		 }
		 logger.trace("----------------END OF OUTPUT OF SFPEOPLE------------------");
	}//end of loadSFpeople
	
	private DeptAff parseRSdeptAff(ResultSet rs){
		
		try {
			String sfID = rs.getString("deptaff_sfid");
			if(rs.wasNull()){
				return null;
			}
			String orgunitID = rs.getString("deptaff_orgunit_id_field__c");
			String deptSFID = rs.getString("deptaff_department_lookup_field__c");
			String affiliationSFID = rs.getString("deptaff_department_lookup_field__c");
			DeptAff deptAff = new DeptAff(orgunitID,sfID).withAffiliationSFID(affiliationSFID).withDepartmentSFID(deptSFID);
			return deptAff;
		} catch (SQLException e) {
			// TODO Auto-generated catch block:
			e.printStackTrace();
			return null;
		}
		
		
	}
	
	private Affiliation parseRSaff(ResultSet rs){
		//Affiliation(String type, String title, String office, String sfID)
		try {
			String type = rs.getString("affiliation_type_field__c");
			String title = rs.getString("affiliation_title_field__c");
			String office = rs.getString("affiliation_office_field__c");
			String sfID = rs.getString("aff_sfid");
			if(rs.wasNull()){
				//there is no SFID
				return null;
			}
			Affiliation thisAff = new Affiliation(type,title,office,sfID);
			return thisAff;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		
	}//end of parseRSaff
	
	
	private Person parseRSperson(ResultSet rs){
		
		try {
			String kerbID = rs.getString("person_kerb_id_field__c");
			String firstName = rs.getString("person_first_name_field__c");
			String middleName = rs.getString("person_middle_name_field__c");
			String lastName = rs.getString("person_last_name_field__c");
			String displayName = rs.getString("person_display_name_field__c");
			String email = rs.getString("person_email_field__c");
			String phoneNumber = rs.getString("person_phone_number_field__c");
			String website = rs.getString("person_website_field__c");
			String sfID = rs.getString("person_sfid");
			Person thisPerson = new Person(kerbID,firstName,middleName,lastName,displayName,email,phoneNumber,website,sfID);
			return thisPerson;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	
	}//end of parseRSperson
	
	/*
	 * bogus kerbid
	 * getStatus:400
{
  "errorCode" : "400",
  "errorMessage" : "Bad Request",
  "errorDetails" : [ {
    "message" : "kerberosId is not valid"
  } ]
}

bad URL
getStatus: 404
getBody: Resource Not Found

	 */
	
	/**
	 * returns a Person from the Kerbid
	 * @param kerbID
	 * @return returns a Person. if an error happens getting person, returns null. if valid response, but kerbid is not valid (or missing?) returns person with isMissing() = true
	 */
	public static Person getAPIperson(String kerbID) {
		Person person;
		if(kerbID == null){
			return null;
		}
		kerbID = kerbID.toLowerCase();
		try{
			
			HttpResponse<String> response = Unirest.get("https://mit-public.cloudhub.io/people/v3/people/"  + kerbID)
					  //.header("authorization", "Basic NmJmMjhjMGZlN2Y3NGEzYWJlZmRkZWYyYzQ5ZDljMzc6OWQxYjA0ZDgwNDczNDEzMzgxMEZEQTA2Q0M0MUMxNjM=")
					  .header("client_id", Main.api_id)
					  .header("client_secret", Main.api_secret)
					  .header("cache-control", "no-cache")
					  //.header("postman-token", "bbb7ce03-8aee-fef1-97c6-a137796366e4")
					  .asString();
			if(response.getStatus() == 400){
				logger.warn("KERBID=" + kerbID + " " + Main.parseJsonError(response));
				person = new Person(null);
				return person;
			}else if(response.getStatus() == 404){
				logger.warn("KERBID=" + kerbID + " " + Main.parseJsonError(response));
				return null;
			}else if(response.getStatus() != 200){
				logger.error("UNKNOWN_ERROR_TYPE KERBID=" + kerbID + " " +  Main.parseJsonError(response));
				return null;
			}
			logger.trace(" TASK=RECEIVING_JSON KERBID=" + kerbID + " JSON=" + response.getBody());
			JSONObject responeJson = new JSONObject(response.getBody());
			JSONObject personJson = responeJson.getJSONObject("item");
			person = parsePersonJson(personJson);
			//System.out.print(person.toString());
			//System.out.println("\nRESPONSE FOR " + kerbID + "\n" + response.getBody());
		}catch(UnirestException ex){
			logger.error(" TASK=READING_PEOPLE_API STATUS=EXCEPTION EXCEPTION_TYPE=UNIREST KERBID=" + kerbID,ex);
			person = null;
		}catch(JSONException ex){
			logger.error(" TASK=READING_PEOPLE_API STATUS=EXCEPTION EXCEPTION_TYPE=JSONEXCEPTION KERBID=" + kerbID, ex); //(" TASK=READING_PEOPLE_API STATUS=EXCEPTION EXCEPTION_TYPE=JSONEXCEPTION KERBID=" + kerbID);
			person = null;
		}
		

		return person;
	}//end of getPerson
	
	public static Person parsePersonJson(JSONObject personJson){
		if(personJson == null){
			return null;
		}
		
		String kerbID      = personJson.getString("kerberosId");
		String firstName   = personJson.optString("givenName"  , "");
		String lastName    = personJson.optString("familyName" , "");
		String middleName  = personJson.optString("middleName" , "");
		String displayName = personJson.optString("displayName", "");
		String email       = personJson.optString("email"      , "");
		String phoneNumber = personJson.optString("phoneNumber", "");
		String website     = personJson.optString("website"    , "");
		
		Person person = new Person(kerbID,firstName,middleName,lastName,displayName,email,phoneNumber,website);
		
		JSONArray affsJson = personJson.optJSONArray("affiliations");
		if(affsJson == null){
			//if no affiliations, person is complete
			return person;
		}
		
		//we have at least 1 affiliation if we are here. iterate affiliations
		for(int i=0;i<affsJson.length();i++){
			JSONObject affJson = affsJson.getJSONObject(i);
			String type   = affJson.optString("type"  , null);
			String title  = affJson.optString("title" , null);
			String office = affJson.optString("office", null);
			Affiliation aff = new Affiliation(type,title,office);
			JSONArray deptsJson = affJson.optJSONArray("departments");
			if(deptsJson != null){
				//we have at least 1 dept
				for(int j=0;j<deptsJson.length();j++){
					JSONObject deptJson = deptsJson.getJSONObject(j);
					try{
						String orgUnitId = deptJson.getString("orgUnitId");
						String name      = deptJson.optString("name", null);
						DeptAff deptAff  = new DeptAff(orgUnitId).withName(name);
						aff.addDept(deptAff);
					}catch(Exception ex){
						//if orgUnitID didn't have a value, walk away
					}
				}//end of for(deptsJson
			}//end of if(deptsJson not null
			person.addAffiliation(aff);
		}//end of for(affsJson
		
		
		
		return person;
		
	}//end of parsePersonJson
	
	/**
	 * changes Base SQL statements to be schema specific
	 * @param input input the Base SQL statement
	 * @return returns the customized SQL statement for this schema
	 */
	private  String replaceProps(String input){
		String output = input;
		for(String eachProp:Main.PEOPLE_SYNC_PROPERTIES){
			output = output.replace("<" + eachProp + ">", this.props.getProperty(eachProp));
		}
		logger.trace(" TASK=CUSOMIZING SQL INPUT=\"" + input + "\" OUTPUT=\"" + output + "\"");
		return output;
		
	}
	
	private void trimLogs(){
		logger.info(" TASK=TRIMMING_PEOPLE_LOGS STATUS=STARTING");
		//first how many days are we going back
		Integer daysBack = null;
		try{
			daysBack = Integer.parseInt((String) this.props.get("log_archive_days__c"));
		}catch(Exception ex){
			logger.error(" TASK=TRIMMING_PEOPLE_LOGS STATUS=NOT_TRIMMING REASON=NO_VALID_VALUE", ex);
			//didn't get a good number back, let's just go home now
			return;
		}
		
		if(daysBack == null || daysBack < 0){
			logger.info(" TASK=TRIMMING_PEOPLE_LOGS STATUS=NOT_TRIMMING");
			//negative number or null, no trimming
			
			return;
		}
		
		try{
			PreparedStatement trimPS = conn.prepareStatement(TRIM_LOGS_SQL);
			int numTrimmed = trimPS.executeUpdate();
			trimPS.close();
			logger.info(" TASK=TRIMMING_PEOPLE_LOGS STATUS=FINISHED UPDATED=" + numTrimmed);
			
		}catch(SQLException ex){
			logger.error(" TASK=TRIMMING_PEOPLE_LOGS STATUS=ERROR",ex);
			
		}
		
	}//end of trimLogs
	
	/**
	 * writes log information into the person_update_log object on the target schema
	 * @param logText text to include along with some general update info
	 */
	public void writeLog(String logText){
		logger.info(" TASK=WRITING_LOG STATUS=STARTING");
		
		String toWrite = getRunInfo() + "-----FULL LOG-----\n" +  logText;
		toWrite = Main.trimLongString(toWrite);
		
		
		String logObject = (String) props.get("log_object_name__c");
		if (logObject == null || logObject.equals("") ){
			logger.info(" TASK=WRITING_LOG STATUS=SKIPPING");
			return;
		}
		
		try{
			PreparedStatement writePS = conn.prepareStatement(INSERT_LOG_SQL);
			writePS.setString(1, toWrite);
			int numInserted = writePS.executeUpdate();
			if(numInserted == 1){
				logger.debug(" TASK=WRITING_LOG SUCCESS=TRUE");
			}else{
				logger.error(" TASK=WRITING_LOG SUCCESS=FALSE");
			}
			writePS.close();
		}catch(SQLException ex){
			logger.error( " TASK=WRITING_LOG STATUS=EXCEPTION",ex);
		}
		
	}//end of writeLog
	
	public String getRunInfo(){
		String cr = "\n";
		String result = "";
		result += Main.getEnvInfo() + cr;
		result += "Schema=" + Main.current_schema + cr;
		result += "----Person Object=" + props.getProperty("person_object_name__c") + cr;
		result += "Persons created=" + personsCreated + cr;
		result += "Persons Updated=" + personsUpdated + cr;
		result += "Persons Deactivated=" + personsDeactivated + cr;
		result += "----Affiliation Object=" + props.getProperty("affiliation_object_name__c") + cr;
		result += "Affiliations created=" + affsCreated + cr;
		result += "Affiliations Updated=" + affsUpdated + cr;
		result += "Affiliations Deactivated=" + affsDeactivated + cr;
		result += "----DepartmentAffiliation Object=" + props.getProperty("deptaff_object_name__c") + cr;
		result += "Department-Affiliations created=" + deptAffsCreated + cr;
		result += "Department-Affiliations Updated=" + deptAffsUpdated + cr;
		result += "Department-Affiliations Deactivated=" + deptAffsDeactivated + cr;
		
		return result;
	}
	
//	public String getMyLog() {
//		return myLog;
//	}
//
//	public String getMyLog(int sizeLimit){
//		String result = myLog;
//		if(result.length() > Main.MAX_LOG_SIZE){
//			result = result.substring(0, sizeLimit);
//		}
//		return result;
//	}
//
//
//	private void logThis(String whatToWrite){
//		myLog += Main.writeLog(whatToWrite) + "\n";
//	}
//	
//	private void logThis(Person person,String whatToWrite){
//		myLog += Main.writeLog(person, whatToWrite) + "\n";
//	}
	
	
	
	
}
