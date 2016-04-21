package eu.dnetlib.iis.common.java.io;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/**
 * Iterator that extracts sequence file's consecutive {@link Text} values. 
 * 
 * @author mhorst
 */
public class SequenceFileTextValueReader implements CloseableIterator<Text> {

        private final Logger log = Logger.getLogger(this.getClass());

        private SequenceFile.Reader sequenceReader;

        private final RemoteIterator<LocatedFileStatus> fileIt;

        private final FileSystem fs;

        /** Ignore file starting with underscore. Such files are also ignored by
         * default by map-reduce jobs.
         */
        private final static String whitelistRegexp = "^(?!_).*";

        private Text toBeReturned;

        /**
         * Default constructor.
         * 
         * @param fs hadoop filesystem
         * @param seqPath sequence file path
         * @throws IOException
         */
        public SequenceFileTextValueReader(final FileSystemPath path) 
                        throws IOException {
                this.fs = path.getFileSystem();
                if (fs.isDirectory(path.getPath())) {
                        fileIt = fs.listFiles(path.getPath(), false);
                        sequenceReader = getNextSequenceReader();
                } else {
                        fileIt = null;
                        sequenceReader = new Reader(fs.getConf(),
                                        SequenceFile.Reader.file(path.getPath()));
                }
        }

        final Reader getNextSequenceReader() throws IOException {
                while (fileIt != null && fileIt.hasNext()) {
                        LocatedFileStatus currentFileStatus = fileIt.next();
                        if (isValidFile(currentFileStatus)) {
                                return new Reader(this.fs.getConf(),
                                                SequenceFile.Reader.file(currentFileStatus.getPath()));
                        }
                }
//              fallback
                return null;
        }

        /**
         * Checks whether file is valid candidate.
         * @param fileStatus
         * @return true when valid, false otherwise
         */
        private final boolean isValidFile(LocatedFileStatus fileStatus) {
                if (fileStatus.isFile()) {
                        if (whitelistRegexp!=null) {
                                return Pattern.matches(whitelistRegexp, 
                                                fileStatus.getPath().getName());
                        } else {
                                return true;
                        }
                }
//              fallback
                return false;
        }

        /**
         * Returns next data package.
         * @return next data package
         * @throws IOException
         */
        protected Text getNext() {
                try {
                        if (sequenceReader==null) {
                                return null;
                        }
                        Writable key = (Writable) ReflectionUtils.newInstance(
                                        sequenceReader.getKeyClass(), fs.getConf());
                        Writable value = (Writable) ReflectionUtils.newInstance(
                                        sequenceReader.getValueClass(), fs.getConf());
                        if (sequenceReader.next(key, value)) {
                        return (Text) value;
                      } else {
                          sequenceReader.close();
                          sequenceReader = getNextSequenceReader();
                          if (sequenceReader!=null) {
                                  return getNext();
                          }
                      }
//                      fallback
                        return null;
                } catch (IOException e) {
                        throw new RuntimeException(e);
                }
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#hasNext()
         */
        @Override
        public boolean hasNext() {
//              check and provide next when already returned
                if (toBeReturned==null) {
                        toBeReturned = getNext();
                } 
                return toBeReturned!=null;
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#next()
         */
        @Override
        public Text next() {
                if (toBeReturned!=null) {
//                      element fetched while executing hasNext()
                        Text result = toBeReturned;
                        toBeReturned = null;
                        return result;
                } else {
                        return getNext();
                }
        }

        /* (non-Javadoc)
         * @see eu.dnetlib.iis.exp.iterator.ClosableIterator#close()
         */
        @Override
        public void close() {
                if (sequenceReader!=null) {
                        try {
                                sequenceReader.close();
                        } catch (IOException e) {
                                log.error("error occurred when closing sequence reader", e);
                        }
                }
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.Iterator#remove()
         */
        @Override
        public void remove() {
                throw new UnsupportedOperationException();
        }
}
