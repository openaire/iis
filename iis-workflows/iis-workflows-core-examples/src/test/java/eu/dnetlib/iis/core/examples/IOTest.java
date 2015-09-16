package eu.dnetlib.iis.core.examples;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.MiniOozieTestCase;
import org.apache.oozie.util.XLog;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.io.Files;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.TestsIOUtils;


/**
 * Tests of basic IO operations when using Oozie testing facilities
 * @author Mateusz Kobos
 */
@Category(IntegrationTest.class)
public class IOTest extends MiniOozieTestCase {
	
	@Override
	protected void setUp() throws Exception {
		System.setProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
		log = new XLog(LogFactory.getLog(getClass()));
		super.setUp();
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testCopyingFile() throws IOException {
		File tempDir = Files.createTempDir();
		File inFile = null;
		File outFile = null;
	
		try{
			inFile = new File(tempDir, "in_file.bin");
			
			Path hdfsFile = new Path(getFsTestCaseDir().toUri().getPath(), 
					"my_file.bin");
			
			byte[] contents = new byte[]{1, 2, 3, 1, 2};
			
			OutputStream out = 
					new BufferedOutputStream(new FileOutputStream(inFile));
			out.write(contents);
			out.close();
			getFileSystem().copyFromLocalFile(
					new Path(inFile.getAbsolutePath()), hdfsFile);
			outFile = new File(tempDir, "out_file.bin");
			getFileSystem().copyToLocalFile(hdfsFile, new 
					Path(outFile.getAbsolutePath()));
			checkFileContents(outFile, contents);
		} finally {
			if(inFile != null){
				inFile.delete();
			}
			if(outFile != null){
				outFile.delete();
			}
			
		}
		
	}
	
	public void testCopyingDirectoryStructure() throws IOException{
		File tempDir = new File(System.getProperty("java.io.tmpdir"));
		File inDir = null;
		File outDir = null;
		
		try{
			inDir = new File(tempDir, "in_dir");
			File inFile = new File(inDir, "file.bin");
			File inDirInternal = new File(inDir, "internal_dir");
			File inFileInternal = new File(inDirInternal, "internal.bin");
			
			inDir.mkdirs();
			inDirInternal.mkdirs();
			
			byte[] contents = new byte[]{1, 2, 3, 1, 2};
			write(inFile, contents);
			byte[] contentsInternal = new byte[]{3, 2, 5, 1};
			write(inFileInternal, contentsInternal);
	
			Path hdfsDir = new Path(getFsTestCaseDir().toUri().getPath(), 
					"my_dir");
			
			getFileSystem().copyFromLocalFile(
					new Path(inDir.getAbsolutePath()), hdfsDir);
			outDir = new File(tempDir, "out_dir");
			getFileSystem().copyToLocalFile(hdfsDir, 
					new	Path(outDir.getAbsolutePath()));
			
			checkFileContents(new File(outDir, "file.bin"), contents);
			checkFileContents(
					new File(inDirInternal, "internal.bin"), contentsInternal);
		} finally {
			if(inDir != null){
				FileUtils.deleteDirectory(inDir);
			}
			if(outDir != null){
				FileUtils.deleteDirectory(outDir);
			}
		}
	}
	
	private static void write(File file, byte[] contents) throws IOException{
		OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
		out.write(contents);
		out.close();		
	}
	
	private void checkFileContents(File file, byte[] expected) 
			throws IOException{
		final int buffSize = 256;
		if(expected.length > buffSize) {
			throw new RuntimeException("expected array too large");
		}
		InputStream in =
				new BufferedInputStream(new FileInputStream(file));
		try {
			byte[] buffer = new byte[buffSize];
			int bytesRead = in.read(buffer);
			
			assertEquals(expected.length, bytesRead);
			byte[] readData = new byte[expected.length];
			for(int i = 0; i < readData.length; i++){
				readData[i] = buffer[i];
			}
			assertTrue(Arrays.equals(expected, readData));
		} finally {
			in.close();
		}
	}
	
	protected void assertContentsEqual(String resourcePath, File otherFile) 
			throws FileNotFoundException, IOException{
		TestsIOUtils.assertEqual(
				Thread.currentThread().getContextClassLoader().getResourceAsStream(
						resourcePath), 
				new FileInputStream(otherFile));
	}
}
