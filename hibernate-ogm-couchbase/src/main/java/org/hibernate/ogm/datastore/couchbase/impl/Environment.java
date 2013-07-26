/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2010-2012 Red Hat Inc. and/or its affiliates and other contributors
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

/**
 * Configuration options of the Couchbase GridDialect
 *
 * @author Tugdual Grall (tugdual@grallandco.com)
 */
public interface Environment {

    /**
     * List of the couchbase servers to use. The string look like:
     *   http://10.1.1.4:8091/,http://10.1.1.5:8091/
     */
    public static final String COUCHBASE_URLS = "hibernate.ogm.couchbase.urls";

    /**
     * Bucket name
     */
    public static final String COUCHBASE_BUCKET = "hibernate.ogm.couchbase.bucket";

    /**
     * Password used to connect to the bucket. Null value by default
     */
    public static final String COUCHBASE_PASSWORD = "hibernate.ogm.couchbase.bucket.password";

    /**
     * Administrator user name Administrator by default
     */
    public static final String COUCHBASE_ADMINISTRATOR = "hibernate.ogm.couchbase.administrator";

    /**
     * Administrator password user name "" by default
     */
    public static final String COUCHBASE_ADMINISTRATOR_PASSWORD = "hibernate.ogm.couchbase.admin.password";

    /**
     * If this value is present and set to true, the bucket will be created if no present
     */
    public static final String COUCHBASE_CREATE_BUCKET = "hibernate.ogm.couchbase.createBucket";


    /**
     * Type of storage (SIMPLE: K,V or JSON: Document)
     */
    public static final String COUCHBASE_STORAGE_TYPE = "hibernate.ogm.couchbase.storageType";


    /**
     * The default url used to connect to Couchbase: if the {@link #COUCHBASE_URLS}
     * property is not set, we'll attempt to connect to localhost.
     */
    public static final String COUCHBASE_DEFAULT_URL = "http://127.0.0.1:8091/pools";

    /**
     * Default bucket, set to "default". This is used when the {@link #COUCHBASE_BUCKET}
     * is not set
     */
    public static final String COUCHBASE_DEFAULT_BUCKET = "default";
    public static final String COUCHBASE_DEFAULT_ADMINISTRATOR = "Administrator";
    public static final String COUCHBASE_STORAGE_SIMPLE = "SIMPLE";
    public static final String COUCHBASE_STORAGE_JSON = "JSON";






}
