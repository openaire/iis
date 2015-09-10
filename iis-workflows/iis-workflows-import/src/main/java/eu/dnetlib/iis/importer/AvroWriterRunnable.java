package eu.dnetlib.iis.importer;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import eu.dnetlib.iis.core.java.io.DataStore;
import eu.dnetlib.iis.core.java.io.FileSystemPath;


/**
 * Avro writer runnable.
 * @author mhorst
 *
 */
public class AvroWriterRunnable implements Runnable {
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	private final FileSystem fs;
	private final BlockingQueue<ObjectWithPath> protoBuffQueue;
	private volatile boolean wasInterrupted = false;
	private final int autoFlushInterval;// 0 - disabled
	
	public AvroWriterRunnable(FileSystem fs, 
			BlockingQueue<ObjectWithPath> protoBuffQueue,
			int autoFlushInterval) {
		this.autoFlushInterval = autoFlushInterval;
		this.fs = fs;
		this.protoBuffQueue = protoBuffQueue;
	}
	
	public AvroWriterRunnable(FileSystem fs, 
			BlockingQueue<ObjectWithPath> protoBuffQueue) {
		this(fs, protoBuffQueue, 0);
	}
	
	@Override
	public void run() {
		try {
			log.debug("starting SequenceFileWriterRunnable thread");
			ObjectWithPath currentObj = null;
			ObjectWithPath lastObj = null;
			DataFileWriter<SpecificRecord> writer = null;
			try {
				int counter=0; 
				while (!((currentObj = protoBuffQueue.take()) instanceof Poison)) {
					if (lastObj==null || !currentObj.getPath().equals(lastObj.getPath())) {
//						closing previously opened stream
						if (writer!=null) {
							writer.close();	
						}
						log.debug("creating writer on path: " + currentObj.getPath().toUri());
						counter=0;
						writer = DataStore.create(
								new FileSystemPath(fs, currentObj.getPath()), 
								currentObj.getObject().getSchema());
					}
					try {
						writer.append(currentObj.getObject());
						if (autoFlushInterval>0) {
							counter++;
							if (counter%autoFlushInterval==0) {
								writer.flush();
								counter=0;	
							}
						}
					} finally {
						lastObj = currentObj;
					}
				}
			} finally {
//				always closing last file
				if (writer!=null) {
					writer.close();
				}
			}
		} catch (InterruptedException e) {
			wasInterrupted = true;
			log.error("got interrupted exception, shutting down...", e);
		} catch (IOException e) {
			wasInterrupted = true;
			log.error("exception occurred when writing file", e);
		}
		log.debug("exitting writer thread");
	}

	public boolean isWasInterrupted() {
		return wasInterrupted;
	}
}
