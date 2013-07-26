package org.hibernate.ogm.datastore.dialect.couchbase;

import org.hibernate.ogm.datastore.couchbase.impl.CouchbaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.AssociationContext;
import org.hibernate.ogm.datastore.spi.AssociationSnapshot;
import org.hibernate.ogm.datastore.spi.Tuple;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.RowKey;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: tgrall
 * Date: 10/11/12
 * Time: 9:07 AM
 * To change this template use File | Settings | File Templates.
 */
public class CouchbaseAssociationSnapshot implements AssociationSnapshot {

    private Map<RowKey, Map<String,Object>> associationMap = null;
    private Map<String, Object> entityAsMap = null;
    private AssociationKey associationKey;


    public CouchbaseAssociationSnapshot(CouchbaseDatastoreProvider provider, AssociationKey key, AssociationContext context) {
        this.associationMap = new LinkedHashMap<RowKey, Map<String,Object> >();
        this.associationKey = key;

        // get the association fields
        String associationField = key.getCollectionRole();
        String parentKeyAsString = CouchbaseDialectUtil.getKeyAsString(key.getEntityKey());
        //get the Document from Couchbase
        String entityAsJsonString = (String) provider.getCouchbaseClient().get(parentKeyAsString);
        entityAsMap = CouchbaseDialectUtil.parse(entityAsJsonString);
        // check if the association exist already
        if (entityAsMap.containsKey(associationField)) {
            // association exists, so let's see if linked object exits
            Collection<String> columnNames = Arrays.asList(key.getRowKeyColumnNames());

            // get the children and add tuple for each one
            List<Map<String, Object>> children = (ArrayList<Map<String, Object>>) entityAsMap.get(associationField);
            for (Map<String, Object> el : children) {

                // get the name and value of each columns
                // some will come from the "association key" (parent), some from the embedded value
                List<Object> columnValues = new ArrayList<Object>();
                for (String columnKey : columnNames) {
                    boolean getFromDatabase = true;
                    int length = key.getColumnNames().length;
                    // try and find the value in the key metadata
                    for (int index = 0; index < length; index++) {
                        String assocColumn = key.getColumnNames()[index];
                        if (assocColumn.equals(columnKey)) {
                            columnValues.add(associationKey.getColumnValues()[index]);
                            getFromDatabase = false;
                            break;
                        }
                    }
                    //otherwise read it from the database structure
                    if (getFromDatabase == true) {
                        columnValues.add(el.get(columnKey));
                    }
                }
                RowKey rowKey = new RowKey(
                        key.getTable(),
                        columnNames.toArray(new String[columnNames.size()]),
                        columnValues.toArray());
                //Stock database structure per RowKey
                this.associationMap.put( rowKey,  el  );
            }

        } else {
            System.out.println("\t ASSO FIELD DOES NOT EXIST");

        }

    }


    @Override
    public Tuple get(RowKey column) {
		Tuple t = new Tuple(new CouchbaseTupleSnapshot(column, entityAsMap));
        return t;
    }

    @Override
    public boolean containsKey(RowKey column) {
        return associationMap.containsKey(column);
    }

    @Override
    public int size() {
        return associationMap.size();
    }

    @Override
    public Set<RowKey> getRowKeys() {
        return associationMap.keySet();
    }

	@Override
	public String toString() {
		return "CouchbaseAssociationSnapshot{" +
				"associationMap=" + associationMap +
				", entityAsMap=" + entityAsMap +
				", associationKey=" + associationKey +
				'}';
	}
}

