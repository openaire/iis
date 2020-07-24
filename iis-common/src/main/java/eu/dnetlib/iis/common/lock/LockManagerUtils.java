package eu.dnetlib.iis.common.lock;

import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;

public class LockManagerUtils {

    private LockManagerUtils() {
    }

    public static LockManager instantiateLockManager(String lockManagerFactoryClassName,
                                                     Configuration config) throws Exception {
        Class<?> clazz = Class.forName(lockManagerFactoryClassName);
        Constructor<?> constructor = clazz.getConstructor();
        LockManagerFactory lockManagerFactory = (LockManagerFactory) constructor.newInstance();
        return lockManagerFactory.instantiate(config);
    }
}
