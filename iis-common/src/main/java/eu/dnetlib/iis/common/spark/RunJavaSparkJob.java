package eu.dnetlib.iis.common.spark;

import org.apache.spark.SparkConf;

import java.lang.reflect.Method;

public class RunJavaSparkJob {

	public static void main(String[] args) throws Exception {

		String className = args[0];
		String[] mainArgs = new String[args.length - 1];
		System.arraycopy(args, 1, mainArgs, 0, mainArgs.length);

		SparkConf conf = new SparkConf();

        SparkSessionSupport.runWithHiveEnabledSparkSession(conf, false, sparkSession -> {

            Class<?> clazz = Class.forName(className);

            Method mainMethod = clazz.getMethod("main", String[].class);

            if (!java.lang.reflect.Modifier.isStatic(mainMethod.getModifiers())) {
                System.err.println("Error: The main method of " + className + " must be static.");
                System.exit(1);
            }

            mainMethod.invoke(null, (Object) mainArgs);
        });
	}
}
