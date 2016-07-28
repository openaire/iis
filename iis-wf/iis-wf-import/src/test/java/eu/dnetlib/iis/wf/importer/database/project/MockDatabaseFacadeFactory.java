package eu.dnetlib.iis.wf.importer.database.project;

import java.util.Map;

import eu.dnetlib.iis.wf.importer.StaticResourcesProvider;
import eu.dnetlib.iis.wf.importer.facade.DatabaseFacade;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeException;
import eu.dnetlib.iis.wf.importer.facade.ServiceFacadeFactory;

/**
 * Factory for building Database facade mocks.
 * @author mhorst
 *
 */
public class MockDatabaseFacadeFactory implements ServiceFacadeFactory<DatabaseFacade> {

    
    //------------------------ LOGIC --------------------------
    
    @Override
    public DatabaseFacade instantiate(Map<String, String> parameters) {
        return new MockDatabaseFacade();
    }
    
    //--------------- INNER CLASS -------------------

    /**
     * Database mock providing static records.
     *
     */
    private static class MockDatabaseFacade implements DatabaseFacade {
        
        private static final String[] profileLocations = new String[] {
                "/eu/dnetlib/iis/wf/importer/database/project/data/input/project_with_optional.xml",
                "/eu/dnetlib/iis/wf/importer/database/project/data/input/project_no_optional_no_fundingtree.xml", 
                "/eu/dnetlib/iis/wf/importer/database/project/data/input/project_null_acronym_null_optional.xml" };

        //------------------------ LOGIC --------------------------

        @Override
        public Iterable<String> searchSQL(String databaseName, String query) throws ServiceFacadeException {
            return StaticResourcesProvider.getResources(profileLocations);
        }

    }
}

