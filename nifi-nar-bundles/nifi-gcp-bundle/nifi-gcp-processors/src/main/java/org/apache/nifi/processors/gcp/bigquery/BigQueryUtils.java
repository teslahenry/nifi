/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gcp.bigquery;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


/**
 * Util class for schema manipulation
 */
public class BigQueryUtils {

    private final static Type gsonSchemaType = new TypeToken<List<Map>>() { }.getType();

    private static LegacySQLTypeName getType(String typeStr) {
        LegacySQLTypeName type = null;

        if (typeStr.equals("BOOLEAN")) {
            type = LegacySQLTypeName.BOOLEAN;
        } else if (typeStr.equals("STRING")) {
            type = LegacySQLTypeName.STRING;
        } else if (typeStr.equals("BYTES")) {
            type = LegacySQLTypeName.BYTES;
        } else if (typeStr.equals("INTEGER")) {
            type = LegacySQLTypeName.INTEGER;
        } else if (typeStr.equals("FLOAT")) {
            type = LegacySQLTypeName.FLOAT;
        } else if (typeStr.equals("TIMESTAMP") || typeStr.equals("DATE")
                || typeStr.equals("TIME") || typeStr.equals("DATETIME")) {
            type = LegacySQLTypeName.TIMESTAMP;
        } else if (typeStr.equals("RECORD")) {
            type = LegacySQLTypeName.RECORD;
        }

        return type;
    }

    public static Field mapToField(Map fMap) {
        String typeStr = fMap.get("type").toString();
        String nameStr = fMap.get("name").toString();
        String modeStr = fMap.getOrDefault("mode", "NULLABLE").toString();
        LegacySQLTypeName type = getType(typeStr);


        if (type == LegacySQLTypeName.RECORD) {
            ArrayList<Map> fields = (ArrayList) fMap.get("fields");
            List<Field> subFields = new ArrayList<Field>();
            for (Map fieldMap : fields) {
                subFields.add(mapToField(fieldMap));
            }

            return Field.newBuilder(nameStr, type, FieldList.of(subFields)).setMode(Field.Mode.valueOf(modeStr)).build();
        }

        return Field.newBuilder(nameStr, type).setMode(Field.Mode.valueOf(modeStr)).build();
    }

    public static List<Field> listToFields(List<Map> m_fields) {
        List<Field> fields = new ArrayList(m_fields.size());
        for (Map m : m_fields) {
            fields.add(mapToField(m));
        }

        return fields;
    }

    public static Schema schemaFromString(String schemaStr) {
        if (schemaStr == null) {
            return null;
        } else {
            Gson gson = new Gson();
            List<Map> fields = gson.fromJson(schemaStr, gsonSchemaType);
            return Schema.of(BigQueryUtils.listToFields(fields));
        }
    }

    public static void main(String[] args) {
        Schema schema = schemaFromString("[\n" +
                "      {\n" +
                "        \"name\": \"event_date\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_timestamp\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_previous_timestamp\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_value_in_usd\", \n" +
                "        \"type\": \"FLOAT\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_bundle_sequence_id\", \n" +
                "        \"type\": \"INTEGER\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_server_timestamp_offset\", \n" +
                "        \"type\": \"INTEGER\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_pseudo_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_first_touch_timestamp\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_ltv_revenue\", \n" +
                "        \"type\": \"FLOAT\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_ltv_currency\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_category\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_mobile_brand_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_mobile_model_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_mobile_marketing_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_mobile_os_hardware_model\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_operating_system\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_operating_system_version\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_vendor_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_advertising_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_language\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_is_limited_ad_tracking\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_time_zone_offset_seconds\", \n" +
                "        \"type\": \"INTEGER\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_browser\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_browser_version\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_web_info_browser\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_web_info_browser_version\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"device_web_info_hostname\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"geo_continent\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"geo_country\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"geo_region\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"geo_city\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"geo_sub_continent\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"geo_metro\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"app_info_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"app_info_version\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"app_info_install_store\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"app_info_firebase_app_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"app_info_install_source\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"traffic_source_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"traffic_source_medium\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"traffic_source_source\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"stream_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"platform\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_dimensions_hostname\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_barcode\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_barcode_scanned\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_campaign\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_click_timestamp\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_component\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_content\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_content_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_content_title\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_content_type\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_delivery_option_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_engagement_time_msec\", \n" +
                "        \"type\": \"FLOAT\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_entrances\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_error\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_error_value\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_event_timestamp\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_fatal\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_firebase_conversion\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_firebase_error\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_firebase_event_origin\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_firebase_previous_class\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_firebase_previous_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_firebase_screen_class\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_firebase_screen_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_ga_session_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_ga_session_number\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_gclid\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_latitude\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_longitude\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_medium\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_module\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_payment_option_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_previous_app_version\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_previous_first_open_count\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_previous_os_version\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_product_brand_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_product_brand_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_product_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_product_location_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_product_merchant_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_product_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_product_price_discounted\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_product_time_expire\", \n" +
                "        \"type\": \"DATETIME\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_product_time_purchase\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_purchase_expire\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_purchase_id\", \n" +
                "        \"type\": \"STRING\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_purchase_status\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_purchase_val\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_referral_code\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_screen\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_screen_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_session_engaged\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_source\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_status\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_store_code\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_success\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_system_app\", \n" +
                "        \"type\": \"STRING\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_system_app_update\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_term\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_time_duration\", \n" +
                "        \"type\": \"FLOAT\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_time_end\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_time_start\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_timestamp\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_total_value\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_transaction_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_transaction_point\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_transaction_time\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_transaction_type\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_transaction_value\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_update_with_analytics\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_user_address_district\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_user_address_ward\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_user_info_dob\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_user_info_gender\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_user_info_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_user_tier\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"event_params_user_uid\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_user_address_district\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_user_info_gender\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_user_address_ward\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_user_tier\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_user_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_user_info_name\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_device_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_total_value\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_first_open_time\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_user_info_dob\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_referral_code\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_ga_session_number\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_properties_ga_session_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"user_session_id\", \n" +
                "        \"type\": \"STRING\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"session_start\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"session_end\", \n" +
                "        \"type\": \"DATETIME\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"num_records\", \n" +
                "        \"type\": \"INTEGER\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"total_sessions\", \n" +
                "        \"type\": \"INTEGER\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }, \n" +
                "      {\n" +
                "        \"name\": \"session_length_seconds\", \n" +
                "        \"type\": \"FLOAT\",\n" +
                "\t\t\"mode\": \"NULLABLE\"\n" +
                "      }\n" +
                "    ]");
    }

}
