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

import org.hibernate.ogm.datastore.spi.TupleSnapshot;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: tgrall
 * Date: 10/5/12
 * Time: 8:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class CouchbaseTupleSnapshot implements TupleSnapshot{

    private final Object couchbaseDocument;
    private final Map<String, Object> objectAsMap;
    private final EntityKey entityKey;
    private final RowKey rowKey;
    private final List<String> columnNames;

    public CouchbaseTupleSnapshot(RowKey key, Map<String, Object> objectAsMap) {
		this.couchbaseDocument = null;
        this.entityKey = null;
        this.rowKey = key;
        this.columnNames = Arrays.asList( key.getColumnNames() );
        this.objectAsMap = objectAsMap;
    }


    public CouchbaseTupleSnapshot(EntityKey key, String keyAsString) {
        this.couchbaseDocument = null;
        this.rowKey = null;
        this.entityKey = key;
        this.columnNames = Arrays.asList( key.getColumnNames() );
        this.objectAsMap = new HashMap<String, Object>();
    }

    public CouchbaseTupleSnapshot(Object document, EntityKey key) {
        this.couchbaseDocument = document;
        this.rowKey = null;
        this.entityKey = key;
        this.columnNames = Arrays.asList( key.getColumnNames() );
        this.objectAsMap = CouchbaseDialectUtil.parse((String) this.couchbaseDocument);
    }

    @Override
    public Object get(String column) {
        if (objectAsMap != null && objectAsMap.containsKey(column)) {
            return objectAsMap.get(column);
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;  //TODO: change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<String> getColumnNames() {
        Set<String> returnValue = null;
        if (rowKey != null ) {
            List<String> list = Arrays.asList( rowKey.getColumnNames() );
            returnValue = new HashSet<String>( list );
        }
        return returnValue;
    }


}
