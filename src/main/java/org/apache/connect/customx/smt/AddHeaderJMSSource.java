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
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class AddHeaderJMSSource<R extends ConnectRecord<R>> implements Transformation<R> {

/*
"transforms":"addheader",
"transforms.addHeader.type":"org.apache.connect.customx.smt.AddHeaderJMSSource$Value",
"transforms.addHeader.ts.field.name":"ACT_DATE"
*/

    public static final String OVERVIEW_DOC =
            "Add header to kafka records";

    private interface ConfigName {
        String TS_FIELD_NAME = "ts.field.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TS_FIELD_NAME, ConfigDef.Type.STRING, "ts", ConfigDef.Importance.HIGH,
                    "dummy field");

    private static final String PURPOSE = "Add header to kafka records";

    private String fieldName;
    private String fieldFormat;
    private String fieldDtSep;
    private String fieldTmSep;
    private String fieldDefault;


    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.TS_FIELD_NAME);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }


    @Override
    public R apply(R record) {
        return applySchemaless(record);
//        if (operatingSchema(record) == null) {
//            return applySchemaless(record);
//        } else {
//            return applyWithSchema(record);
//        }
    }

    private R applySchemaless(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        Headers headers = record.headers();
        String txt = "";

        for (Field field : value.schema().fields()) {
           if(!field.name().equals("destination") && !field.name().equals("properties") && !field.name().equals("replyTo") && !field.name().equals("text") ){
               System.out.println(field.name());
               try {
                   Object o = value.get(field);
                   if (o instanceof String){
                       headers.addString(field.name(), value.get(field).toString());
                   }
                   else if (o instanceof Integer){
                       headers.addInt(field.name(), (Integer)value.get(field));
                   }
                   else if (o instanceof Long){
                       headers.addLong(field.name(), (Long)value.get(field));
                   }
                   else if (o instanceof Float){
                       headers.addFloat(field.name(), (Float)value.get(field));
                   }
                   else{
                       headers.addString(field.name(), value.get(field).toString());
                   }
               }catch(Exception e){
                   System.out.println("exception 1");
               }
            }
            if(field.name().equals("text")) {
                txt = value.get(field).toString();
            }
            if((field.name().equals("destination") || field.name().equals("destination")) && value.get(field) != null) {
                final Struct s = requireStruct(value.get(field), PURPOSE);
                try {
                    headers.addString(field.name(), s.get("destinationType").toString() + "[" + s.get("name").toString() + "]");
                }catch (Exception ex){
                    System.out.println("ex");
                }
            }
            if(field.name().equals("properties") && value.get(field) != null) {
                final Map<String, Object> v = requireMap(value.get(field), PURPOSE);
                for (Map.Entry<String, Object> e : v.entrySet()) {
                    final Struct s = requireStruct(e.getValue(), PURPOSE);
                    Object o = s.get(s.get("propertyType").toString());
                    if (o instanceof String){
                        headers.addString(e.getKey(), s.get(s.get("propertyType").toString()).toString());
                    }
                    else if (o instanceof Integer){
                        int x = ((Number)s.get(s.get("propertyType").toString())).intValue();
                        headers.addInt(e.getKey(), x);
                    }
                    else if (o instanceof Long){
                        headers.addLong(e.getKey(), (Long)s.get((String) s.get("propertyType")));
                    }
                    else if (o instanceof Float){
                        headers.addFloat(e.getKey(), (Float)s.get(s.get("propertyType").toString()));
                    }
                    else {
                        headers.addString(e.getKey(), s.get(s.get("propertyType").toString()).toString());
                    }
//                    for (Field sf : s.schema().fields()) {
//                        System.out.println("here7");
//                        try {
//                            System.out.println(sf.name());
//                            System.out.println(s.get(sf).toString());
//                        }catch(Exception ex){
//                            System.out.println("here9");
//                        }
//                        System.out.println("here8");
//                    }
                 }

            }
        }
        return newRecord(record, null, txt);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        System.out.println("====================start1==================");
        for (Field field : value.schema().fields()) {
            System.out.println(field.name());
            if(!field.name().equals("replyTo")){
                System.out.println(value.get(field).getClass());
                try {
                    System.out.println(value.get(field).toString());
                }
                catch (Exception e){
                    System.out.println(value.get(field));
                }
            }

            System.out.println("=============");
        }
        System.out.println("====================end1==================");
        // updatedValue.put(fieldName, "te");
        return newRecord(record, updatedSchema, updatedValue);
    }

    private String returnFormattedDate(String s){
        String src_dtf = fieldFormat;
        String dt_sep = fieldDtSep;
        String tm_sep = fieldTmSep;
        tm_sep = (tm_sep.trim().equals(""))?" ": tm_sep;
        int d_idx = 0;
        int m_idx = 0;
        int yt_idx = 0;
        int idx = 0;

        String[] dtf = src_dtf.split(dt_sep);
        int i = 0;
        for (String part:dtf) {
            if(part.contains("d")) {
                idx = part.indexOf('d');
                d_idx = i;
            }
            else if(part.contains("y")){
                yt_idx = i;
            }
            else if(part.contains("M")){
                m_idx = i;
            }
            i++;
        }
        String[] dt_split = s.split(dt_sep);
       /* System.out.println("date");
        for (String x:dt_split) {
            System.out.println(x);
        }*/
        String[] tm_split = dt_split[yt_idx].split(tm_sep);
        /*System.out.println("time");
        for (String x:tm_split) {
            System.out.println(x);
        }*/
        String yrtm = (tm_split[0].length()<4)? 20+tm_split[0]:tm_split[0];
        //System.out.println(yrtm);
        String newDt = dt_split[d_idx].substring(idx) + "-" + dt_split[m_idx] + "-" + yrtm + " " + tm_split[1];
        //System.out.println(newDt);
        if(tm_split.length > 2){
            newDt = dt_split[d_idx].substring(idx) + "-" + dt_split[m_idx] + "-" + yrtm + " " + tm_split[1] + " " + tm_split[2];
        }
        return newDt;
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

    public static class Key<R extends ConnectRecord<R>> extends AddHeaderJMSSource<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends AddHeaderJMSSource<R> {

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


