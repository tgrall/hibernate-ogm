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

package org.hibernate.ogm.datastore.dialect.couchbase;

import com.couchbase.client.CouchbaseClient;
import org.hibernate.LockMode;
import org.hibernate.dialect.lock.LockingStrategy;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.ogm.datastore.couchbase.impl.CouchbaseDatastoreProvider;
import org.hibernate.ogm.datastore.impl.EmptyTupleSnapshot;
import org.hibernate.ogm.datastore.impl.MapHelpers;
import org.hibernate.ogm.datastore.impl.MapTupleSnapshot;
import org.hibernate.ogm.datastore.map.impl.MapAssociationSnapshot;
import org.hibernate.ogm.datastore.spi.*;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.EntityKeyMetadata;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.massindex.batchindexing.Consumer;
import org.hibernate.ogm.type.GridType;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Since Couchbase is a document "key/document" nosql engine it is possible to store each Tuple entry
 * as part of a Property in a Document (JSON)    or when simple K,V
 * <p/>
 * TODO : reduce the number of calls to the client.get->JSON->Hashmap
 * <p/>
 * The Couchbase dialect will work with Map... and then when ready persist the map as a JSON object
 * So when creating association let's work with map first then push the JSON
 *
 * mvn test -Dtest=ManyToOneTest#testBidirectionalManyToOneRegular -Dmaven.surefire.debug
 *
 * @author Tugdual Grall <tugdual@grallandco.com>
 */
public class CouchbaseDialect implements GridDialect {

    private static final Log logger = LoggerFactory.make();

    private final CouchbaseDatastoreProvider datastoreProvider;
    private final CouchbaseClient client;

    public CouchbaseDialect(CouchbaseDatastoreProvider datastoreProvider) {
        this.datastoreProvider = datastoreProvider;
        this.client = datastoreProvider.getCouchbaseClient();
    }

    @Override
    public LockingStrategy getLockingStrategy(Lockable lockable, LockMode lockMode) {
        throw new UnsupportedOperationException("The Couchbase GridDialect does not support locking.");
    }

    @Override
    public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		final Object object = this.getObjectFromCouchbase(key);
        if (object != null) {
            return new Tuple(new MapTupleSnapshot(CouchbaseDialectUtil.parse((String) object)));
        } else {
            return null;
        }
    }

    @Override
    public Tuple createTuple(EntityKey key) {
        final HashMap<String, Object> tuple = new HashMap<String, Object>();
        String json = CouchbaseDialectUtil.getJson(tuple);
        client.set(CouchbaseDialectUtil.getKeyAsString(key), 0, json);
        return new Tuple(new MapTupleSnapshot(tuple));
    }

    @Override
    public void updateTuple(Tuple tuple, EntityKey key) {
        Map<String, Object> entityRecord = ((MapTupleSnapshot) tuple.getSnapshot()).getMap();
        MapHelpers.applyTupleOpsOnMap(tuple, entityRecord);
        String json = CouchbaseDialectUtil.getJson(entityRecord);
        client.set(CouchbaseDialectUtil.getKeyAsString(key), 0, json);
    }

    @Override
    public void removeTuple(EntityKey key) {
        client.delete(CouchbaseDialectUtil.getKeyAsString(key));
    }

    @Override
    public Association getAssociation(AssociationKey key, AssociationContext associationContext) {

        if (associationExists(key, associationContext)) {
            Association ass = new Association(new CouchbaseAssociationSnapshot(datastoreProvider, key, associationContext));
			return  ass;
        } else {
            return null;
        }
    }

    @Override
    public Association createAssociation(AssociationKey key) {

        String parentKeyAsString = CouchbaseDialectUtil.getKeyAsString(key.getEntityKey());
        //get the Document from Couchbase
        String entityAsJsonString = (String) client.get(parentKeyAsString);
        HashMap<String, Object> entityAsMap = CouchbaseDialectUtil.parse(entityAsJsonString);
        String[] path = key.getCollectionRole().split("\\.");
        int size = path.length;
        for (int index = 0; index < size; index++) {
            entityAsMap.put(path[index], Collections.EMPTY_LIST);
        }
        client.set(parentKeyAsString, 0, CouchbaseDialectUtil.getJson(entityAsMap));

        // TODO : Check what should really be returned
        Map<RowKey, Map<String, Object>> map = new HashMap<RowKey, Map<String, Object>>();
        Association association = new Association(new MapAssociationSnapshot(map));
        return association;
    }

    @Override
    public void updateAssociation(Association association, AssociationKey key) {
        MapHelpers.updateAssociation(association, key);
        String associationField = key.getCollectionRole();
        String parentKeyAsString = CouchbaseDialectUtil.getKeyAsString(key.getEntityKey());
        String entityAsJsonString = (String) client.get(parentKeyAsString);
        HashMap<String, Object> entityAsMap = CouchbaseDialectUtil.parse(entityAsJsonString);
        for (AssociationOperation action : association.getOperations()) {
            RowKey rowKey = action.getKey();
            Tuple rowValue = action.getValue();
            switch (action.getType()) {
                case CLEAR:
                    System.out.println("\t CLEAR" + rowKey);
                    break;
                case PUT_NULL:
                case PUT:
                    for (String valueKeyName : rowValue.getColumnNames()) {
                        boolean add = true;
                        //exclude columns from the associationKey
                        for (String assocColumn : key.getColumnNames()) {
                            if (valueKeyName.equals(assocColumn)) {
                                add = false;
                                break;
                            }
                        }
                        if (add) {
                            Map<String, Object> keyAsMap = new HashMap<String, Object>();
                            keyAsMap.put(valueKeyName, rowValue.get(valueKeyName));
                            ((ArrayList) entityAsMap.get(associationField)).add(keyAsMap);

                        }
                    }
                    client.set(parentKeyAsString, 0, CouchbaseDialectUtil.getJson(entityAsMap));
                    //update = putAssociationRowKey( rowValue, associationField, key );
                    break;
                case REMOVE:
                    System.out.println("\t REMOVE");
                    //update = removeAssociationRowKey( assocSnapshot, rowKey, associationField );
                    break;
            }
        }
    }

    @Override
    public void removeAssociation(AssociationKey key) {
        System.out.println("Remove Association :\n\t" + key);
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Tuple createTupleAssociation(AssociationKey associationKey, RowKey rowKey) {
        return new Tuple(EmptyTupleSnapshot.SINGLETON);
    }

    @Override
    public void nextValue(RowKey key, IntegralDataTypeHolder value, int increment, int initialValue) {
        // create the key for the sequence
        // no need to use a document here, use the native increment function of Couchbase
        long newValue = client.incr(CouchbaseDialectUtil.getSequenceKey(key), increment, initialValue);
        value.initialize(newValue);
    }

    @Override
    public GridType overrideType(Type type) {
        return null; // all other types handled as in hibernate-ogm-core
    }

    @Override
    public void forEachTuple(Consumer consumer, EntityKeyMetadata... entityKeyMetadatas) {

        //TODO :IMPLEMENT
        System.out.println("TODO : IMPLEMENT forEachTuple");

        //To change body of implemented methods use File | Settings | File Templates.
    }

    private CouchbaseClient getCouchbaseClient() {
        return datastoreProvider.getCouchbaseClient();
    }


    /**
     * This method is used to check if the association field is present in the entity
     *
     * @param key
     * @param associationContext
     * @return true if the association is present
     */
    private boolean associationExists(AssociationKey key, AssociationContext associationContext) {
        String parentKeyAsString = CouchbaseDialectUtil.getKeyAsString(key.getEntityKey());
        String entityAsJsonString = (String) client.get(parentKeyAsString);
        HashMap<String, Object> entityAsMap = CouchbaseDialectUtil.parse(entityAsJsonString);
        return entityAsMap.containsKey(key.getCollectionRole());
    }


    // TODO: generic access for "all key types"
    private Object getObjectFromCouchbase(EntityKey key) {
        String cacheKey = CouchbaseDialectUtil.getKeyAsString(key);
		Object entity = client.get(cacheKey);
        return entity;
    }

    private Object getObjectFromCouchbase(AssociationKey key) {
        String cacheKey = CouchbaseDialectUtil.getKeyAsString(key);
		Object entity = client.get(cacheKey);
		return entity;
    }


}
