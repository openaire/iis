package eu.dnetlib.iis.common.cache;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.OOZIE_ACTION_OUTPUT_FILENAME;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonWriter;

import eu.dnetlib.iis.common.FsShellPermissions;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * CacheMetadata managing process.
 * 
 * @author mhorst
 *
 */
public class CacheMetadataManagingProcess implements eu.dnetlib.iis.common.java.Process {

	public static final String OUTPUT_PROPERTY_CACHE_ID = "cache_id";
	
	public static final String OUTPUT_PROPERTY_NEXT_RECORD_INDEX = "next_record_index";
	
	public static final String PARAM_CACHE_DIR = "cache_location";
	
	public static final String PARAM_MODE = "mode";
	
	public static final String PARAM_ID = "id";
	
	public static final String PARAM_NEXT_RECORD_INDEX = "next_record_index";
	
	public static final String MODE_READ_CURRENT_ID = "read_current_id";
	
	public static final String MODE_GENERATE_NEW_ID = "generate_new_id";
	
	public static final String MODE_WRITE_ID = "write_id";
	
	public static final String MODE_READ_META = "read_meta";
	
	public static final String MODE_WRITE_META = "write_meta";
	
	public static final String DEFAULT_METAFILE_NAME = "meta.json";
	
	public static final int CACHE_ID_PADDING_LENGTH = 6;
	
	public static final String UNDEFINED = "$UNDEFINED$"; 

	public static final String DEFAULT_ENCODING = "UTF-8";
	
	public static class CacheMeta {

	    private String currentCacheId;
	    
	    /**
	     * Optional field used only in incrementally indexed cache implementations. 
	     */
	    private String nextRecordIndex;

	    
        public String getCurrentCacheId() {
			return currentCacheId;
		}

		public void setCurrentCacheId(String currentCacheId) {
			this.currentCacheId = currentCacheId;
		}
		
        public String getNextRecordIndex() {
            return nextRecordIndex;
        }

        public void setNextRecordIndex(String nextRecordIndex) {
            this.nextRecordIndex = nextRecordIndex;
        }

	}
	
	/**
	 * Underlying file system facade factory.
	 */
	private final FileSystemFacadeFactory fsFacadeFactory;

    // ---------------------------- CONSTRUCTORS --------------------------
	
	
	/**
	 * Default constructor instantiating process with hadoop file system.
	 */
	public CacheMetadataManagingProcess() {
        this((conf) -> {
            return new HadoopFileSystemFacade(FileSystem.get(conf));
        });
	}
	
	/**
	 * Constructor instantiating process with custom file system facade.
	 */
	public CacheMetadataManagingProcess(FileSystemFacadeFactory fsFacadeFactory) {
	    this.fsFacadeFactory = fsFacadeFactory;
	}
	
	// ---------------------------- LOGIC ---------------------------------
	
	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return Collections.emptyMap();
	}

    @Override
    public void run(PortBindings portBindings, Configuration conf,
            Map<String, String> parameters) throws Exception {
        String mode = parameters.get(PARAM_MODE);
        Properties props = new Properties();
        
        switch(mode) {
            case MODE_READ_CURRENT_ID: {
                props.setProperty(OUTPUT_PROPERTY_CACHE_ID, getExistingCacheId(conf, parameters.get(PARAM_CACHE_DIR)));
                break;
            }
            case MODE_GENERATE_NEW_ID: {
                props.setProperty(OUTPUT_PROPERTY_CACHE_ID, generateNewCacheId(conf, parameters.get(PARAM_CACHE_DIR)));
                break;
            }
            case MODE_WRITE_ID: {
                writeCacheId(conf, parameters.get(PARAM_CACHE_DIR), parameters.get(PARAM_ID));
                break;
            }
            case MODE_READ_META: {
                readMeta(conf, parameters.get(PARAM_CACHE_DIR), props);
                break;
            }
            case MODE_WRITE_META: {
                writeMeta(conf, parameters.get(PARAM_CACHE_DIR), parameters.get(PARAM_ID), parameters.get(PARAM_NEXT_RECORD_INDEX));
                break;
            }
            default: {
                throw new RuntimeException("unsupported mode: " + mode);
            }
        
        }
        
        File file = new File(System.getProperty(OOZIE_ACTION_OUTPUT_FILENAME));
        OutputStream os = new FileOutputStream(file);
        try {
            props.store(os, "");    
        } finally {
            os.close(); 
        }   
    }

    public String getExistingCacheId(Configuration conf, String cacheDir) throws IOException {
        if (StringUtils.isNotBlank(cacheDir)) {
            CacheMeta cacheMeta = readCacheMeta(fsFacadeFactory.create(conf), 
                    new Path(cacheDir, DEFAULT_METAFILE_NAME));
            if (cacheMeta != null) {
                return cacheMeta.getCurrentCacheId();
            } else {
                return UNDEFINED;
            }
        } else {
            throw new RuntimeException("cache directory location not provided! "
                    + "'" + PARAM_CACHE_DIR + "' parameter is missing!");
        }
    }
    
    public Properties readMeta(Configuration conf, String cacheDir, Properties props) throws IOException {
        if (StringUtils.isNotBlank(cacheDir)) {
            CacheMeta cacheMeta = readCacheMeta(fsFacadeFactory.create(conf), 
                    new Path(cacheDir, DEFAULT_METAFILE_NAME));
            if (cacheMeta != null) {
                props.setProperty(OUTPUT_PROPERTY_CACHE_ID, cacheMeta.getCurrentCacheId());
                props.setProperty(OUTPUT_PROPERTY_NEXT_RECORD_INDEX, cacheMeta.getNextRecordIndex());
            } else {
                props.setProperty(OUTPUT_PROPERTY_CACHE_ID, UNDEFINED);
                props.setProperty(OUTPUT_PROPERTY_NEXT_RECORD_INDEX, UNDEFINED);
            }
            return props;
        } else {
            throw new RuntimeException("cache directory location not provided! "
                    + "'" + PARAM_CACHE_DIR + "' parameter is missing!");
        }
    }
    
    public String generateNewCacheId(Configuration conf, String cacheDir) throws IOException {
        if (StringUtils.isNotBlank(cacheDir)) {
            CacheMeta cachedMeta = readCacheMeta(fsFacadeFactory.create(conf), 
                    new Path(cacheDir, DEFAULT_METAFILE_NAME));
            if (cachedMeta != null) {
                int currentIndex = convertCacheIdToInt(cachedMeta.getCurrentCacheId());
                return convertIntToCacheId(currentIndex+1);
            } else {
//              initializing cache meta
                return convertIntToCacheId(1);
            }
        } else {
            throw new RuntimeException("cache directory location not provided! "
                    + "'" + PARAM_CACHE_DIR + "' parameter is missing!");
        }
    }
    
    public void writeCacheId(Configuration conf, String cacheDir, String cacheId) throws IOException {
        writeMeta(conf, cacheDir, cacheId, null);
    }
    
    public void writeMeta(Configuration conf, String cacheDir, String cacheId, String nextRecordIndex) throws IOException {
        if (StringUtils.isNotBlank(cacheDir)) {
            if (StringUtils.isNotBlank(cacheId)) {

                FileSystemFacade fs = fsFacadeFactory.create(conf);
                
                Path cacheFilePath = new Path(cacheDir, DEFAULT_METAFILE_NAME);
                
                CacheMeta cachedMeta = getExistingCacheMetaOrCreateNewOne(cacheId, fs, cacheFilePath);
                if (cacheId != null) {
                    cachedMeta.setCurrentCacheId(cacheId);    
                }
                if (nextRecordIndex != null) {
                    cachedMeta.setNextRecordIndex(nextRecordIndex); 
                }
                
                Gson gson = new Gson();
                OutputStream outputStream = fs.create(cacheFilePath, true);
                JsonWriter writer = new JsonWriter(new OutputStreamWriter(outputStream, DEFAULT_ENCODING));
                try {
                    gson.toJson(cachedMeta, CacheMeta.class, writer);
                } finally {
                    writer.close();
                    outputStream.close();
//                  changing file permission to +rw to allow writing for different users
                    fs.changePermissions(conf, FsShellPermissions.Op.CHMOD, 
                            false, "0666", cacheFilePath.toString());
                }
            } else {
                throw new RuntimeException("unable to write new cache id in meta.json file, "
                        + "no '" + PARAM_ID + "' input parameter provied!");
            }
        } else {
            throw new RuntimeException("cache directory location not provided! "
                    + "'" + PARAM_CACHE_DIR + "' parameter is missing!");
        }
    }
    
    
    // ---------------------------- PRIVATE ---------------------------------
	
	
	/**
	 * Reads or creates new cache metadata record.
	 */
    private static CacheMeta getExistingCacheMetaOrCreateNewOne(String cacheId, FileSystemFacade fs, Path cacheFilePath)
            throws JsonSyntaxException, JsonIOException, UnsupportedEncodingException, IOException {
        CacheMeta cachedMeta = readCacheMeta(fs, cacheFilePath);
//      writing new id
        if (cachedMeta==null) {
            cachedMeta = new CacheMeta();
        }
        return cachedMeta;
	}
	
    private static CacheMeta readCacheMeta(FileSystemFacade fs, Path cacheFilePath) throws JsonSyntaxException, JsonIOException, UnsupportedEncodingException, IOException {
        if (fs.exists(cacheFilePath)) {
            InputStream inputStream = fs.open(cacheFilePath);
            InputStreamReader reader = new InputStreamReader(
                    inputStream, DEFAULT_ENCODING);
            try {
                Gson gson = new Gson();
                return gson.fromJson(reader, CacheMeta.class);
            } finally {
                reader.close();
                inputStream.close();
            }
        } else {
            return null;
        }
    }
    
	private static int convertCacheIdToInt(String cacheId) {
		StringBuffer strBuff = new StringBuffer(cacheId);
		while (true) {
			if (strBuff.charAt(0)=='0') {
				strBuff.deleteCharAt(0);
			} else {
				break;
			}
		}
		return Integer.parseInt(strBuff.toString());
	}
	
	private static String convertIntToCacheId(int cacheIndex) {
		StringBuffer strBuff = new StringBuffer(String.valueOf(cacheIndex));
		while(strBuff.length()<CACHE_ID_PADDING_LENGTH) {
			strBuff.insert(0, '0');
		}
		return strBuff.toString();
	}
	
}
