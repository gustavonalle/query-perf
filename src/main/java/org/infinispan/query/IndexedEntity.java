package org.infinispan.query;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class IndexedEntity {

   @ProtoField(number = 1)
   @Field(analyze = Analyze.YES, store = Store.YES)
   String name;

   @ProtoField(number = 2)
   @Field(analyze = Analyze.NO, store = Store.YES)
   String description;

   @ProtoFactory
   public IndexedEntity(String name, String description) {
      this.name = name;
      this.description = description;
   }

}
