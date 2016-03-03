package eu.dnetlib.iis.common;

import java.io.IOException;

import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

/**
 * Helper util class for working with ssh execution 
 * 
 * @author madryk
 *
 */
public class SshExecUtils {
	
	
	//------------------------ CONSTRUCTORS --------------------------
	
	private SshExecUtils() {
		throw new IllegalStateException("Can't instantiate util class " + getClass());
	}
	
	
	//------------------------ LOGIC --------------------------
	
	/**
	 * Reads output of executed ssh command
	 */
	public static String readCommandOutput(Command cmd) {
		try {
			return IOUtils.readFully(cmd.getInputStream()).toString();
		} catch (IOException e) {
			throw new RuntimeException("Unable to read command output", e);
		}
	}
	
	/**
	 * Reads error of executed ssh command
	 */
	public static String readCommandError(Command cmd) {
		try {
			return IOUtils.readFully(cmd.getErrorStream()).toString();
		} catch (IOException e) {
			throw new RuntimeException("Unable to read command error", e);
		}
	}
}
