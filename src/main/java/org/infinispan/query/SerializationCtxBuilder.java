package org.infinispan.query;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
      includeClasses = {IndexedEntity.class},
      schemaFileName = "file.proto"
)
public interface SerializationCtxBuilder extends SerializationContextInitializer {
   SerializationCtxBuilder INSTANCE = new SerializationCtxBuilderImpl();
}
