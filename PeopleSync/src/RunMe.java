import java.net.InetAddress;
import java.net.UnknownHostException;



public class RunMe {

	
	public static void main(String[] args){
		//edu.mit.ist.salesforce.sync.Main.emailMe();
		String result = "";
		result += " JAVA_VERSION=\"" + System.getProperty("java.version") + "\" ";
		try {
			result += " HOSTNAME=" + InetAddress.getLocalHost().getHostName() + " ";
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			result += " HOSTNAME=UNKNOWN ";
		}
		
		
		System.out.println(result);
	}
	
}
