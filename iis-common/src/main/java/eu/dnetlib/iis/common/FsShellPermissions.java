package eu.dnetlib.iis.common;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.springframework.beans.BeanUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

/**
 * Extracted from: 
 * https://github.com/spring-projects/spring-hadoop/blob/master/spring-hadoop-core/src/main/java/org/springframework/data/hadoop/fs/FsShellPermissions.java
 * 
 * Utility class for accessing Hadoop FsShellPermissions (which is not public) 
 * without having to duplicate its code. 
 * @author Costin Leau
 *
 */
public class FsShellPermissions {

	private static boolean IS_HADOOP_20X = ClassUtils.isPresent("org.apache.hadoop.fs.FsShellPermissions$Chmod",
			FsShellPermissions.class.getClassLoader());

	public enum Op {
		CHOWN("-chown"), CHMOD("-chmod"), CHGRP("-chgrp");

		private final String cmd;

		Op(String cmd) {
			this.cmd = cmd;
		}

		public String getCmd() {
			return cmd;
		}
	}

	// TODO: move this into Spring Core (but add JDK 1.5 compatibility first)
	@SafeVarargs
    static <T> T[] concatAll(T[] first, T[]... rest) {
		// can add some sanity checks
		int totalLength = first.length;
		for (T[] array : rest) {
			totalLength += array.length;
		}
		T[] result = Arrays.copyOf(first, totalLength);
		int offset = first.length;
		for (T[] array : rest) {
			System.arraycopy(array, 0, result, offset, array.length);
			offset += array.length;
		}
		return result;
	}

	public static void changePermissions(FileSystem fs, Configuration config, 
			Op op, boolean recursive, String group, String uri) {
		changePermissions(fs, config, op, recursive, group, new String[] {uri});
	}
	
	public static void changePermissions(FileSystem fs, Configuration config, 
			Op op, boolean recursive, String group, String... uris) {
		String[] argvs;
		if (recursive) {
			argvs = new String[1];
			argvs[0] = "-R";
		} else {
			argvs = new String[0];
		}
		argvs = concatAll(argvs, new String[] { group }, uris);

		// Hadoop 1.0.x
		if (!IS_HADOOP_20X) {
			Class<?> cls = ClassUtils.resolveClassName("org.apache.hadoop.fs.FsShellPermissions", config.getClass().getClassLoader());
			Object[] args = new Object[] { fs, op.getCmd(), argvs, 0, new FsShell(config) };

			Method m = ReflectionUtils.findMethod(cls, "changePermissions", FileSystem.class, String.class, String[].class, int.class, FsShell.class);
			ReflectionUtils.makeAccessible(m);
			ReflectionUtils.invokeMethod(m, null, args);
		}
		// Hadoop 2.x
		else {
			Class<?> cmd = ClassUtils.resolveClassName("org.apache.hadoop.fs.shell.Command", config.getClass().getClassLoader());
			Class<?> targetClz = ClassUtils.resolveClassName("org.apache.hadoop.fs.FsShellPermissions$Chmod", config.getClass().getClassLoader());
			Configurable target = (Configurable) BeanUtils.instantiate(targetClz);
			target.setConf(config);
			// run(String...) swallows the exceptions - re-implement it here
			//
			LinkedList<String> args = new LinkedList<String>(Arrays.asList(argvs));
			try {
				Method m = ReflectionUtils.findMethod(cmd, "processOptions", LinkedList.class);
				ReflectionUtils.makeAccessible(m);
				ReflectionUtils.invokeMethod(m, target, args);
				m = ReflectionUtils.findMethod(cmd, "processRawArguments", LinkedList.class);
				ReflectionUtils.makeAccessible(m);
				ReflectionUtils.invokeMethod(m, target, args);
			} catch (IllegalStateException ex){
				throw new RuntimeException("Cannot change permissions/ownership " + ex);
			}
		}
	}
}
