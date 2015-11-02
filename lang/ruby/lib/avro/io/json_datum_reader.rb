# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'avro/io/datum_reader_base'

module Avro
  module IO
    class JsonDatumReader < DatumReaderBase
      def read(decoder)
        super
        read_data(writers_schema, readers_schema, decoder.data)
      end

      def read_data(writers_schema, readers_schema, data)
        ensure_schema_match(writers_schema, readers_schema)

        rs = union_schema_resolution(writers_schema, readers_schema)
        return read_data(writers_schema, rs, data) if rs

        validate_data(writers_schema, data)

        # function dispatch for reading data based on type of writer's schema
        case writers_schema.type_sym
        when :null;    JsonDecoder.decode_null(data)
        when :boolean; JsonDecoder.decode_boolean(data)
        when :string;  JsonDecoder.decode_string(data)
        when :int;     JsonDecoder.decode_int(data)
        when :long;    JsonDecoder.decode_long(data)
        when :float;   JsonDecoder.decode_float(data)
        when :double;  JsonDecoder.decode_double(data)
        when :bytes;   JsonDecoder.decode_bytes(data)
        when :fixed;   JsonDecoder.decode_fixed(data)
        when :enum;    data
        when :array
          read_items = []
          data.each do |item|
            read_items << read_data(writers_schema.items, readers_schema.items, item)
          end
          return read_items
        when :map
          read_items = {}
          data.each_pair do |key, value|
            read_items[key] = read_data(writers_schema.values, readers_schema.values, value)
          end
          return read_items
        when :union
          read_union(writers_schema, readers_schema, data)
        when :record, :error, :request
          read_record(writers_schema, readers_schema, data)
        else
          raise AvroError, "Cannot read unknown schema type: #{writers_schema.type}"
        end
      end

      def validate_data(schema, data)
        return if [:array, :map, :union, :record, :error, :request].include?(schema.type_sym)

        unless ::Avro::Schema.validate(schema, data)
          raise AvroTypeError.new(schema, data)
        end
      end

      def read_union(writers_schema, readers_schema, data)
        if data.nil?
          schema_type, value = :null, nil
        else
          schema_type, value = data.first
        end

        data_schema = writers_schema.schemas.find do |writer_schema|
          writer_schema.type_sym == schema_type.to_sym ||
                                    (writer_schema.name == schema_type if writer_schema.respond_to?(:name))
        end

        read_data(data_schema, readers_schema, value)
      end

      def read_record(writers_schema, readers_schema, data)
        readers_fields_hash = readers_schema.fields_hash
        read_record = {}
        writers_schema.fields.each do |field|
          if readers_field = readers_fields_hash[field.name]
            field_val = read_data(field.type, readers_field.type, data[field.name])
            read_record[field.name] = field_val
          end
        end

        # fill in the default values
        if readers_fields_hash.size > read_record.size
          writers_fields_hash = writers_schema.fields_hash
          readers_fields_hash.each do |field_name, field|
            unless writers_fields_hash.has_key? field_name
              if !field.default.nil?
                field_val = read_default_value(field.type, field.default)
                read_record[field.name] = field_val
              end
            end
          end
        end

        read_record
      end
    end
  end
end
