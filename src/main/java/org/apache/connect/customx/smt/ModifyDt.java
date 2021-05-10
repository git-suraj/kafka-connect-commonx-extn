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
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ModifyDt<R extends ConnectRecord<R>> implements Transformation<R> {

/*
"transforms":"updatets",
"transforms.updatets.type":"org.apache.connect.customx.smt.ModifyDt$Value",
"transforms.updatets.ts.field.name":"DT",
"transforms.updatets.ts.field.format":"dd-MM-yy",
"transforms.updatets.ts.field.date.separator":"-",
"transforms.updatets.ts.field.default":"01-01-18"
*/

    public static final String OVERVIEW_DOC =
            "Modify the timestamp field based on given format";

    private interface ConfigName {
        String TS_FIELD_NAME = "ts.field.name";
        String TS_FIELD_FORMAT = "ts.field.format";
        String TS_FIELD_DATE_SEPARATOR = "ts.field.date.separator";
        String TS_FIELD_DEFAULT = "ts.field.default";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TS_FIELD_NAME, ConfigDef.Type.STRING, "ts", ConfigDef.Importance.HIGH,
                    "TS field name to be modified")
            .define(ConfigName.TS_FIELD_FORMAT, ConfigDef.Type.STRING, "dd-MM-yyyy hh:mm:ss.S", ConfigDef.Importance.HIGH,
                    "The format of the the field in source resembling SimpleDateFormat")
            .define(ConfigName.TS_FIELD_DATE_SEPARATOR, ConfigDef.Type.STRING, "-", ConfigDef.Importance.HIGH,
                    "The separator between year, month, date fields")
            .define(ConfigName.TS_FIELD_DEFAULT, ConfigDef.Type.STRING, " ", ConfigDef.Importance.HIGH,
                    "The default date value in dd-MM-yy");

    private static final String PURPOSE = "modifying TS field in record";

    private String fieldName;
    private String fieldFormat;
    private String fieldDtSep;
    private String fieldDefault;


    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.TS_FIELD_NAME);
        fieldFormat = config.getString(ConfigName.TS_FIELD_FORMAT);
        fieldDtSep = config.getString(ConfigName.TS_FIELD_DATE_SEPARATOR);
        fieldDefault = config.getString(ConfigName.TS_FIELD_DEFAULT);

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

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {

            if(field.name().equals(fieldName)) {
                System.out.println("**********************");
                System.out.println(value.get(field).toString());
                System.out.println("**********************");
                //String dateStr = "Thu Nov 15 00:00:00 UTC 2018";
                DateFormat srcDf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");

                // parse the date string into Date object
                Date date = null;
                try {
                    date = srcDf.parse(value.get(field).toString());
                } catch (ParseException ex) {
                    System.out.println(ex.getMessage());
                    System.out.println(ex.getStackTrace());
                }

                DateFormat destDf = new SimpleDateFormat("dd-MM-yy");

                // format the date into another format
                String dateStr = destDf.format(date);
                System.out.println("returnFormattedDate->" + dateStr);
                try {
                    Date dateF = new SimpleDateFormat("dd-MM-yy").parse("15-11-18");
                    System.out.println("here->");
                    updatedValue.put(field.name(), dateF);

                }
                catch (Exception ex){
                    System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                    System.out.println(ex.getMessage());
                    System.out.println(ex.getStackTrace());
                    Date dateF = null;
                    try {
                        dateF = new SimpleDateFormat("dd-MM-yy").parse(fieldDefault);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    updatedValue.put(field.name(), dateF);
                    System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
                }
                /*Date dNow = new Date();
                updatedValue.put(field.name(), dNow);*/
            }
            else {
                updatedValue.put(field.name(), value.get(field));
            }
        }
        // updatedValue.put(fieldName, "te");
        return newRecord(record, updatedSchema, updatedValue);
    }

        /*private String returnFormattedDate(String src){
        String dateStr = "Thu Nov 15 00:00:00 UTC 2018";

        DateFormat srcDf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");

        // parse the date string into Date object
        Date date = srcDf.parse(dateStr);

        DateFormat destDf = new SimpleDateFormat("dd-MM-yy");

        // format the date into another format
        dateStr = destDf.format(date);
        return dateStr;
    }*/


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

    public static class Key<R extends ConnectRecord<R>> extends ModifyDt<R> {

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
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends ModifyDt<R> {

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
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}


