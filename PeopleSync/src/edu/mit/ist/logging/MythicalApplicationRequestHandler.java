package edu.mit.ist.logging;

/**
 * Created on 15/2/20.
 *
 * @author andrew.flower
 */
public class MythicalApplicationRequestHandler {
	private static MythicalApplicationRequestHandler instance = null;

	public synchronized static MythicalApplicationRequestHandler getInstance() {
		if (instance == null) {
			instance = new MythicalApplicationRequestHandler();
		}
		return instance;
	}

	public synchronized static void setInstance(MythicalApplicationRequestHandler instance) {
		MythicalApplicationRequestHandler.instance = instance;
	}

	public String getCurrentRequestId() {
		return "abcd-efgh-1234-5678";
	}
}
