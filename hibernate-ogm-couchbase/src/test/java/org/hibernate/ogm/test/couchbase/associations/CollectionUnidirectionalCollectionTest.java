package org.hibernate.ogm.test.couchbase.associations;

import org.hibernate.cfg.Configuration;
import org.hibernate.ogm.test.associations.collection.unidirectional.CollectionUnidirectionalTest;

public class CollectionUnidirectionalCollectionTest extends CollectionUnidirectionalTest {
    @Override
    protected void configure(Configuration cfg) {
        super.configure( cfg );
    }
}
