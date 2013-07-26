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
 * v.2.1 along with this distribution; if not, writFrançois Molins.e to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.datastore.dialect.couchbase;


//TODO : rewrite the Key stringifier we need a single method fot that for all KEYs


import com.google.gson.*;
import org.hibernate.ogm.grid.AssociationKey;
import org.hibernate.ogm.grid.EntityKey;
import org.hibernate.ogm.grid.RowKey;

import java.util.*;


/**
 * Simple Util class to manage generic String and JSON manipulation
 *
 * @author Tugdual Grall <tugdual@grallandco.com>
 */
public class CouchbaseDialectUtil {

    private final static String KEY_TABLE_PREFIX = "tbl";
    private final static String KEY_SEQUENCE_PREFIX = "seq";
    private final static String KEY_ASSOCIATION_PREFIX = "ass";


    /**
     * As a keyvalue store Couchbase needs a UTF8 string for the key.
     * All the entity are store in a single bucket, so the key needs to contains multiple informations
     * The current format is:
     *  tbl_[tablename],[columnid_name],[id_value]
     *
     * @param key
     * @return the string that could be used as Couchbase key
     */
    public static String getKeyAsString(EntityKey key) {

        return "{"+ KEY_TABLE_PREFIX +":"+ key.getTable() +","+ key.getColumnNames()[0] +":"+ key.getColumnValues()[0] +"}";
    }

    /**
     * As a keyvalue store Couchbase needs a UTF8 string for the key.
     * All the entity are store in a single bucket, so the key needs to contains multiple informations
     * The current format is:
     *  tbl_[tablename],[columnid_name],[id_value]
     *
     * @param key
     * @return the string that could be used as Couchbase key
     */
    public static String getKeyAsString(AssociationKey key) {
        return "{"+ KEY_TABLE_PREFIX +":"+ key.getTable() +","+ key.getColumnNames()[0] +":"+ key.getColumnValues()[0] +"}";
    }

	/**
	 * As a keyvalue store Couchbase needs a UTF8 string for the key.
	 * All the entity are store in a single bucket, so the key needs to contains multiple informations
	 * The current format is:
	 *  tbl_[tablename],[columnid_name],[id_value]
	 *
	 * @param key
	 * @return the string that could be used as Couchbase key
	 */
	public static String getKeyAsString(RowKey key) {
		return "{"+ KEY_TABLE_PREFIX +":"+ key.getTable() +","+ key.getColumnNames()[0] +":"+ key.getColumnValues()[0] +"}";
	}

    /**
     * Create a key for Couchbase to manage sequences. The key contains the name of the "table" and the sequence value
     * @param key
     * @return
     */
    public static String getSequenceKey(RowKey key) {
        return "{"+  KEY_SEQUENCE_PREFIX +"_"+ key.getTable()  +":"+ key.getColumnValues()[0] +"}";
    }


    /**
     * Convert the JSON string into a map. The JSON String is coming most of the time from a Document stored into
     * Couchbase
     *
     * @param json as simple string
     * @return a generic map
     */
    public static HashMap<String, Object> parse(String json) {
        JsonParser parser = new JsonParser();
        JsonObject object = (JsonObject) parser.parse(json);


        Set<Map.Entry<String, JsonElement>> set = object.entrySet();
        Iterator<Map.Entry<String, JsonElement>> iterator = set.iterator();
        HashMap<String, Object> map = new HashMap<String, Object>();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonElement> entry = iterator.next();
            String key = entry.getKey();
            JsonElement value = entry.getValue();

            addJsonElement(key, value, map);

        }

        return map;
    }


    private static void addJsonElement(String key, JsonElement value, HashMap<String, Object> map ) {


        if (value.isJsonArray()) {
            JsonArray jsonArray = value.getAsJsonArray();

            ArrayList listAsValue = new ArrayList<HashMap<String,Object>>();

            HashMap<String, Object> listElementMap = new HashMap<String, Object>();

            Iterator<JsonElement> jsonArrayIterator = jsonArray.iterator();
            while (jsonArrayIterator.hasNext()) {
                JsonElement jsonElement = jsonArrayIterator.next();

                listAsValue.add(parse(jsonElement.toString()));

            }


            map.put(key, listAsValue);


        } else if (value.isJsonPrimitive()) {
            // check the type using JsonPrimitive
            // TODO: check all types
            JsonPrimitive jsonPrimitive = value.getAsJsonPrimitive();
            if ( jsonPrimitive.isNumber() ) {
                map.put(key, jsonPrimitive.getAsDouble());
//                map.put(key, jsonPrimitive.getAsInt());
            }  else {
                map.put(key, jsonPrimitive.getAsString());
            }
        } else {
            map.put(key, parse(value.toString()));
        }

    }


    /**
     * Create a JSON String from a map
     *
     * @param document
     * @return JSON String
     */
    public static String getJson(Map<String,Object> document) {

        //TODO : Manage embeddable object as JSON embeddable
        //       Provide configuration option to select the type of JSON (embedded/properties with prefix)
        Gson gson = new Gson();
        String json = gson.toJson(document);
        return json;
    }

}
