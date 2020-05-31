/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <vector>
#include <set>

#include "database.hh"
#include "kafka_upload_service.hh"

#include "avro/lang/c++/api/Compiler.hh"
#include "avro/lang/c++/api/Encoder.hh"
#include "avro/lang/c++/api/Decoder.hh"
#include "avro/lang/c++/api/Specific.hh"
#include "avro/lang/c++/api/Generic.hh"

#include "cql3/column_specification.hh"
#include "cql3/column_identifier.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"

namespace cdc::kafka {

using seastar::sstring;
using namespace std::chrono_literals;

std::vector<schema_ptr> kafka_upload_service::get_tables_with_cdc_enabled() {
    auto tables = _proxy.get_db().local().get_column_families();

    std::vector<schema_ptr> tables_with_cdc;
    for (auto& [id, table] : tables) {
        auto schema = table->schema();
        if (schema->cdc_options().enabled()) {
            tables_with_cdc.push_back(schema);
        }
    }

    return tables_with_cdc;
}

timeuuid do_kafka_replicate(schema_ptr table_schema, timeuuid last_seen) {
    return last_seen;
}

void kafka_upload_service::on_timer() {
    arm_timer();

    auto tables_with_cdc_enabled = get_tables_with_cdc_enabled();
    std::set<std::pair<sstring, sstring>> cdc_keyspace_table;

    // Remove all entries not seen in set of CDC enabled tables
    for (auto& table : tables_with_cdc_enabled) {
        cdc_keyspace_table.emplace(table->ks_name(), table->cf_name());
    }

    for (auto it = _last_seen_row_key.cbegin(); it != _last_seen_row_key.cend(); ) {
        auto should_delete = cdc_keyspace_table.count(it->first) == 0;
        if (should_delete) {
            _last_seen_row_key.erase(it++);
        } else {
            ++it;
        }
    }

    for (auto& table : tables_with_cdc_enabled) {
        std::pair<sstring, sstring> entry = {table->ks_name(), table->cf_name()};
        auto has_entry = _last_seen_row_key.count(entry) != 0;
        if (!has_entry) {
            _last_seen_row_key[entry] = utils::UUID();
        }
        // Create Kafka topic and schema
        auto last_seen = _last_seen_row_key[entry];
        _last_seen_row_key[entry] = do_kafka_replicate(table, last_seen);
    }
    select(tables_with_cdc_enabled);
}

sstring kafka_upload_service::kind_to_avro_type(abstract_type::kind kind) {
    switch (kind) {
        //TODO: Complex types + Check if all kinds are translated into appropriate avro types
        case abstract_type::kind::boolean:
            return sstring("boolean");

        case abstract_type::kind::counter:
        case abstract_type::kind::long_kind:
            return sstring("long");

        case abstract_type::kind::decimal:
        case abstract_type::kind::float_kind:
            return sstring("float");

        case abstract_type::kind::double_kind:
            return sstring("double");

        case abstract_type::kind::int32:
        case abstract_type::kind::short_kind:
            return sstring("int");

        case abstract_type::kind::ascii:
        case abstract_type::kind::byte:
        case abstract_type::kind::bytes:
        case abstract_type::kind::date:
        case abstract_type::kind::duration:
        case abstract_type::kind::empty:
        case abstract_type::kind::inet:
        case abstract_type::kind::list:
        case abstract_type::kind::map:
        case abstract_type::kind::reversed:
        case abstract_type::kind::set:
        case abstract_type::kind::simple_date:
        case abstract_type::kind::time:
        case abstract_type::kind::timestamp:
        case abstract_type::kind::timeuuid:
        case abstract_type::kind::tuple:
        case abstract_type::kind::user:
        case abstract_type::kind::utf8:
        case abstract_type::kind::uuid:
        case abstract_type::kind::varint:
        default:
            return sstring("string");
    }
}

seastar::sstring kafka_upload_service::compose_key_schema_for(schema_ptr schema){

    sstring key_schema, key_schema_fields;
    schema::columns_type primary_key_columns;
    for(const column_definition& cdef : schema->all_columns()){
        if(cdef.is_primary_key()){
            primary_key_columns.push_back(cdef);
        }
    }
    key_schema_fields = compose_avro_record_fields(primary_key_columns);
    key_schema = compose_avro_schema("key_schema", schema->ks_name() + "." + schema->cf_name(),
                                     key_schema_fields);
    return key_schema;
}

sstring kafka_upload_service::compose_value_schema_for(schema_ptr schema){

    sstring value_schema, value_schema_fields;
    value_schema_fields = compose_avro_record_fields(schema->all_columns());
    value_schema = compose_avro_schema("value_schema", schema->ks_name() + "." + schema->cf_name(),
                                       value_schema_fields);
    return value_schema;
}

sstring kafka_upload_service::compose_avro_record_fields(const schema::columns_type& columns){
    sstring result = "";
    int n = 0;
    for(const column_definition& cdef : columns){
        if (n++ != 0) {
            result += ",";
        }
        result += "{";
        result += "\"name\":\"" + cdef.name_as_text() + "\"";
        result += ",\"type\":[\"null\",\""  + kind_to_avro_type(cdef.type->get_kind()) + "\"]";
        result += "}";
    }
    return result;
}

sstring kafka_upload_service::compose_avro_schema(sstring avro_name, sstring avro_namespace, sstring avro_fields) {
        sstring result = sstring("{"
                                 "\"type\":\"record\","
                                 "\"name\":\"" + avro_name + "\","
                                 "\"namespace\":\"" + avro_namespace + "\","
                                 "\"fields\":[" + avro_fields + "]"
                                 "}");
        return result;
 }

void kafka_upload_service::select(std::vector<schema_ptr> &tables) {
    for (auto &table : tables) {
        auto key = std::make_pair<sstring, sstring> (sstring(table->ks_name()), sstring(table->cf_name()));
        auto last_seen_key = _last_seen_row_key.at(key);
        std::vector<query::clustering_range> bounds;
        auto ckp = clustering_key_prefix::from_single_value(*table, timeuuid_type->decompose(last_seen_key));
        auto b = range_bound(ckp, false);
        bounds.push_back(query::clustering_range::make_starting_with(b));
        auto selection = cql3::selection::selection::wildcard(table);
        auto opts = selection->get_query_options();
        auto partition_slice = query::partition_slice(std::move(bounds), *table, column_set(), opts);
        // db::timeout_clock::time_point timeout = db::timeout_clock::now + 10s;
        auto timeout = seastar::lowres_clock::now() + std::chrono::seconds(10);
        auto command = make_lw_shared<query::read_command> (
            table->id(),
            table->version(),
            partition_slice);
        dht::partition_range_vector partition_ranges;
        partition_ranges.push_back(query::full_partition_range);
        try {
        auto results = _proxy.query(
            table, 
            command, 
            std::move(partition_ranges), 
            db::consistency_level::QUORUM,
            service::storage_proxy::coordinator_query_options(
                timeout,
                empty_service_permit(),
                _client_state
            )).then([table = table, partition_slice = std::move(partition_slice), selection = std::move(selection)] 
            (service::storage_proxy::coordinator_query_result qr) -> lw_shared_ptr<cql3::untyped_result_set> {
                cql3::selection::result_set_builder builder(*selection, gc_clock::now(), cql_serialization_format::latest());
                query::result_view::consume(*qr.query_result, partition_slice, cql3::selection::result_set_builder::visitor(builder, *table, *selection));
                auto result_set = builder.build();
                if (!result_set || result_set->empty()) {
                    return {};
                }
                return make_lw_shared<cql3::untyped_result_set>(*result_set);
            }).then([this, table](lw_shared_ptr<cql3::untyped_result_set> results){
           for (auto &row : *results) {
                convert(table, row);
            }
        });
        } catch (exceptions::unavailable_exception &e) {
            // handle it
        }
    }
}

void kafka_upload_service::convert(schema_ptr schema, const cql3::untyped_result_set_row &row) {
    auto avro_schema = compose_value_schema_for(schema);
    std::istringstream ifs(avro_schema);
    avro::ValidSchema compiledSchema;
    avro::compileJsonSchema(ifs, compiledSchema);
    avro::OutputStreamPtr out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    avro::GenericDatum datum(compiledSchema);
    if (datum.type() == avro::AVRO_RECORD) {
        avro::GenericRecord &record = datum.value<avro::GenericRecord>();
        auto columns = row.get_columns();
        for (auto &column : columns) {
            auto name = column->name->to_string();
            auto value = row.get_opt<bytes>(name);
            if (value) {
                record.field(name).value<bytes>() = value.value();
            }
        }
    }
    avro::encode(*e,datum);
    uint8_t* tmp;
    size_t length;
    out->next(&tmp, &length);
    std::cout << tmp;
}

} // namespace cdc::kafka
