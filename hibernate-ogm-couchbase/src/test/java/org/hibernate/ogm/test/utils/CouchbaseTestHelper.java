/*
 * Copyright (C) 2012 Tugdual Grall (tugdual@gmail.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hibernate.ogm.test.utils;


import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.datastore.couchbase.impl.CouchbaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CouchbaseTestHelper implements TestableGridDialect {

    private static final Log logger = LoggerFactory.make();


    @Override
    public boolean assertNumberOfEntities(int numberOfEntities, SessionFactory sessionFactory) {
        return true;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean assertNumberOfAssociations(int numberOfAssociations, SessionFactory sessionFactory) {
        return true;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Object> extractEntityTuple(SessionFactory sessionFactory, EntityKey key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean backendSupportsTransactions() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
        try {
            CouchbaseDatastoreProvider provider = this.getProvider(sessionFactory);
            // provider.getClusterManager().deleteBucket( provider.getCouchbaseBucket() );
        }  catch (Exception e)  {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, String> getEnvironmentProperties() {
        Map<String,String> envProps = new HashMap<String, String>(2);
        copyFromSystemPropertiesToLocalEnvironment( "hibernate.ogm.couchbase.test.dropBucket", envProps );
        copyFromSystemPropertiesToLocalEnvironment( "hibernate.ogm.couchbase.test.flushBucket", envProps );
        System.out.println(envProps);
        return envProps;
    }


    /**
     * Get the DataProvider for the test
     * @param sessionFactory
     * @return
     */
    private CouchbaseDatastoreProvider getProvider(SessionFactory sessionFactory) {
        DatastoreProvider provider = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService(DatastoreProvider.class );
        if ( !( CouchbaseDatastoreProvider.class.isInstance(provider) ) ) {
            throw new RuntimeException( "Not testing with Couchbase, cannot extract underlying cache" );
        }
        return CouchbaseDatastoreProvider.class.cast(provider);
    }


    private void copyFromSystemPropertiesToLocalEnvironment(String environmentVariableName, Map<String, String> envProps) {
        String value = System.getProperties().getProperty( environmentVariableName );
        if ( value != null && value.length() > 0 ) {
            envProps.put( environmentVariableName, value );
        }
    }
}
