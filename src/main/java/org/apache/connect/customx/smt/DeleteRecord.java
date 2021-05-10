/*
 * Copyright © 2021 Suraj Pillai (reach.suraj777@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.connect.customx.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class DeleteRecord<R extends ConnectRecord<R>> implements Transformation<R> {

/*
"transforms":"deleterec",
"transforms.deleterec.type":"org.apache.connect.customx.smt.DeleteRecord$Value",
"transforms.deleterec.dr.field.name":"OP_TYPE",
"transforms.deleterec.dr.field.condition":"D"
*/

    public static final String OVERVIEW_DOC =
            "Make value null of a record if condition is met";

    private interface ConfigName {
        String DR_FIELD_NAME = "dr.field.name";
        String DR_FIELD_CONDITION = "dr.field.condition";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.DR_FIELD_NAME, ConfigDef.Type.STRING, "OP_TYPE", ConfigDef.Importance.HIGH,
                    "The field name whose condition needs to be checked")
            .define(ConfigName.DR_FIELD_CONDITION, ConfigDef.Type.STRING, "D", ConfigDef.Importance.HIGH,
                    "The value of the field. If this is true then record value needs to be null");

    private static final String PURPOSE = "Make value null of a record if condition is met";

    private String fieldName;
    private String fieldValue;
    private boolean delRecord;


    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.DR_FIELD_NAME);
        fieldValue = config.getString(ConfigName.DR_FIELD_CONDITION);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }


    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);


        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        Struct updatedValue = new Struct(updatedSchema);
        delRecord = false;
        System.out.println("*************************************");
        System.out.println("name->" + fieldName);
        System.out.println("value->" + fieldValue);
        for (Field field : value.schema().fields()) {
            System.out.println(field.name().toUpperCase());
            //System.out.println(value.get(field).toString());
            if(field.name().toUpperCase().equals(fieldName)) {
                if(value.get(field).toString().toUpperCase().equals(fieldValue)) {
                    System.out.println("inside");
                    delRecord = true;
                    updatedValue = null;
                    break;
                }
                else {
                    updatedValue.put(field.name(), value.get(field));
                }
            }
            else {
                updatedValue.put(field.name(), value.get(field));
            }
        }
        // updatedValue.put(fieldName, "te");
        return newRecord(record, updatedSchema, updatedValue);
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }


    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field: schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        // builder.field(fieldName, Schema.STRING_SCHEMA);

        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends DeleteRecord<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            System.out.println("*******1***********");
            System.out.println(updatedValue);
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends DeleteRecord<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            System.out.println("*******2***********");
            System.out.println(updatedValue);
            System.out.println(updatedSchema);
            System.out.println(updatedValue == null);
            System.out.println(record.keySchema());
            System.out.println(record.key());
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), (updatedValue == null? optionalSchema(updatedSchema): updatedSchema), updatedValue, record.timestamp());
        }

        protected Schema optionalSchema(Schema schema) {
            if (schema.isOptional()) {
                return schema;
            }

            Schema keySchema = null;
            Schema valueSchema = null;
            List<Field> fields = null;

            if (schema.type() == Schema.Type.MAP) {
                keySchema = schema.keySchema();
                valueSchema = schema.valueSchema();
            } else if (schema.type() == Schema.Type.ARRAY) {
                valueSchema = schema.valueSchema();
            } else if (schema.type() == Schema.Type.STRUCT) {
                fields = schema.fields();
            }

            return new ConnectSchema(
                    schema.type(),
                    true,
                    schema.defaultValue(),
                    schema.name(),
                    schema.version(),
                    schema.doc(),
                    schema.parameters(),
                    fields,
                    keySchema,
                    valueSchema
            );
        }

    }
}


