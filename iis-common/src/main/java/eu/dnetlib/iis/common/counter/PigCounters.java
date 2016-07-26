package eu.dnetlib.iis.common.counter;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Class that simplifies use of pig counters.
 * 
 * @author madryk
 */
public class PigCounters {

    private Map<String, JobCounters> jobCountersMap;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    /**
     * Default constructor
     * 
     * @param jobCountersList - counters of map-reduce jobs run inside pig job
     */
    public PigCounters(List<JobCounters> jobCountersList) {
        Preconditions.checkNotNull(jobCountersList);
        Preconditions.checkArgument(jobCountersList.size() > 0);
        
        this.jobCountersMap = Maps.newHashMap();
        
        for(JobCounters jobCounters : jobCountersList) {
            this.jobCountersMap.put(jobCounters.getJobId(), jobCounters);
        }
    }
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns list of job ids
     */
    public List<String> getJobIds() {
        return Lists.newArrayList(jobCountersMap.keySet());
    }
    
    /**
     * Returns {@link JobCounters} with provided job id
     * or <code>null</code> if job does not exists.
     */
    public JobCounters getJobCounters(String jobId) {
        return jobCountersMap.get(jobId);
    }
    
    /**
     * Returns job id with provided alias
     * or <code>null</code> if job does not exists.
     */
    public String getJobIdByAlias(String jobAlias) {
        for (Map.Entry<String, JobCounters> jobCountersEntry : jobCountersMap.entrySet()) {
            
            if (jobCountersEntry.getValue().getAliases().contains(jobAlias)) {
                return jobCountersEntry.getValue().getJobId();
            }
        }
        return null;
    }
    
    
    //------------------------ INNER CLASSES --------------------------
    
    /**
     * Representation of single map-reduce job counters that were run inside pig job
     */
    public static class JobCounters {
        
        private String jobId;
        private List<String> aliases;
        private Map<String, String> counters;
        
        
        //------------------------ CONSTRUCTORS --------------------------
        
        /**
         * Constructs object with empty counters and no aliases.
         */
        public JobCounters(String jobId) {
            this(jobId, Lists.newArrayList(), Maps.newHashMap());
        }
        
        /**
         * Constructs object with filled counters and aliases.
         */
        public JobCounters(String jobId, List<String> aliases, Map<String, String> counters) {
            this.jobId = jobId;
            this.aliases = aliases;
            this.counters = counters;
        }

        //------------------------ GETTERS --------------------------
        
        /**
         * Returns job id
         */
        public String getJobId() {
            return jobId;
        }

        /**
         * Returns aliases of job
         */
        public List<String> getAliases() {
            return aliases;
        }
        
        
        //------------------------ LOGIC --------------------------
        
        public void addAlias(String alias) {
            aliases.add(alias);
        }

        /**
         * Adds a counter
         */
        public void addCounter(String counterName, String counterValue) {
            counters.put(counterName, counterValue);
        }
        
        /**
         * Returns a number of all counters for the job
         */
        public int getCountersCount() {
            return counters.size();
        }
        
        /**
         * Returns value of a counter with provided name
         */
        public String getCounter(String counterName) {
            return counters.get(counterName);
        }
        
    }
}
