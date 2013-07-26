/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.datastore.couchbase.impl;

import com.couchbase.client.ClusterManager;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.clustermanager.BucketType;
import org.hibernate.ogm.datastore.dialect.couchbase.CouchbaseDialect;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;


/**
 * Created with IntelliJ IDEA.
 * User: tgrall
 * Date: 10/5/12
 * Time: 1:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class CouchbaseDatastoreProvider implements DatastoreProvider, Startable, Stoppable, Configurable {

    private CouchbaseClient client = null;
    private ClusterManager clusterManager = null;
    private final ConcurrentMap<AssociationKey, Map<RowKey, Map<String, Object>>> associationsKeyValueStorage = new ConcurrentHashMap<AssociationKey, Map<RowKey, Map<String, Object>>>();


    private String[] couchbaseServerUrls = null;
    private List<URI> couchbaseServerUrisList = null;
    private String couchbaseBucket = null;
    private String couchbaseAdministrator = null;
    private String couchbaseAdminPassword= null;
    private String couchbasePassword = null;
    private boolean couchbaseCreateBucket = false;
    private String couchbaseStorageType = Environment.COUCHBASE_STORAGE_JSON; // SIMPLE : K,V  | JSON : JSON Documents


    @Override
    public Class<? extends GridDialect> getDefaultDialect() {


        return CouchbaseDialect.class;
    }


    @Override
    public void configure(Map configurationValues) {
        String urls = (String)configurationValues.get(Environment.COUCHBASE_URLS);
        if (urls == null) {
            couchbaseServerUrls = new String[1];
            couchbaseServerUrls[0] = Environment.COUCHBASE_DEFAULT_URL;
        } else {
            couchbaseServerUrls = urls.split(",");
        }
        couchbaseBucket = (String)configurationValues.get(Environment.COUCHBASE_BUCKET);
        if ( couchbaseBucket == null) { couchbaseBucket = Environment.COUCHBASE_DEFAULT_BUCKET; }

        couchbasePassword = (String)configurationValues.get(Environment.COUCHBASE_PASSWORD);
        if (couchbasePassword == null) { couchbasePassword = ""; }

        if ( ((String)configurationValues.get(Environment.COUCHBASE_STORAGE_TYPE)).equals(Environment.COUCHBASE_STORAGE_SIMPLE) ) {
            couchbaseStorageType = Environment.COUCHBASE_STORAGE_SIMPLE;
        } else if ( ((String)configurationValues.get(Environment.COUCHBASE_STORAGE_TYPE)).equals(Environment.COUCHBASE_STORAGE_JSON) ) {
            couchbaseStorageType = Environment.COUCHBASE_STORAGE_SIMPLE;
        } else {
            System.out.println("====== USING JSON ");
        }

        couchbaseAdministrator = (String)configurationValues.get(Environment.COUCHBASE_ADMINISTRATOR);

        couchbaseAdminPassword = (String)configurationValues.get(Environment.COUCHBASE_ADMINISTRATOR_PASSWORD);


        if ( configurationValues.containsKey(Environment.COUCHBASE_CREATE_BUCKET) ) {
            couchbaseCreateBucket =  "true".equalsIgnoreCase((String) configurationValues.get(Environment.COUCHBASE_CREATE_BUCKET));
        }
    }

    @Override
    public void start() {
        try {
            // prepare the URI
            couchbaseServerUrisList = new ArrayList<URI>();
            for (String url : couchbaseServerUrls) {
                if (!(url.endsWith("pools") || url.endsWith("pools/") ) ) {
                    if (!url.endsWith("/")) { url = url +"/"; }
                    url = url + "pools";
                }
                couchbaseServerUrisList.add(new URI(url));
            }

            // TODO review this part
            if (!couchbaseCreateBucket) {
                // bucket is supposed to be present so just connect to the cluster
                connectToCluster();
            } else {
                clusterManager =  new ClusterManager(couchbaseServerUrisList, couchbaseAdministrator, couchbaseAdminPassword);
                if (!clusterManager.listBuckets().contains( couchbaseBucket  )  ) {
                    clusterManager.createNamedBucket(BucketType.COUCHBASE, couchbaseBucket, 100, 0, couchbasePassword, true);
                    // TODO : fix this!
                    Thread.sleep(5000);
                    connectToCluster();
                } else {
                    connectToCluster();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            //TODO : manage logging
        }
    }


    private void connectToCluster() throws IOException {
        CouchbaseConnectionFactory cbConnectionFactory = new CouchbaseConnectionFactory(couchbaseServerUrisList,couchbaseBucket,couchbasePassword);
        client = new CouchbaseClient(cbConnectionFactory);
    }


    public void putAssociation(AssociationKey key, Map<RowKey, Map<String, Object>> associationMap) {
        associationsKeyValueStorage.put( key, associationMap );
    }

    public Map<RowKey, Map<String, Object>> getAssociation(AssociationKey key) {
        return associationsKeyValueStorage.get( key );
    }

    public void removeAssociation(AssociationKey key) {
        associationsKeyValueStorage.remove( key );
    }

    @Override
    public void stop() {
        clusterManager.shutdown();
        client.shutdown(1, TimeUnit.SECONDS);
    }

    public CouchbaseClient getCouchbaseClient() {
        return client;
    }

    public String getCouchbaseStorageType() {
        return couchbaseStorageType;
    }

    public void setCouchbaseStorageType(String couchbaseStorageType) {
        this.couchbaseStorageType = couchbaseStorageType;
    }

    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    public String getCouchbaseBucket() {
        return couchbaseBucket;
    }
}
