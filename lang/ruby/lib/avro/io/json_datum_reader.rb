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

module Avro
  module IO
    class JsonDatumReader
      attr_accessor :writers_schema, :readers_schema

      def initialize(writers_schema=nil, readers_schema=nil)
        @writers_schema = writers_schema
        @readers_schema = readers_schema
      end

      def read(decoder)
        self.readers_schema = writers_schema unless readers_schema
        read_data(writers_schema, readers_schema, decoder, decoder.data)
      end

      private

      def read_data(writers_schema, readers_schema, decoder, data)
        # schema matching
        unless ::Avro::IO.match_schemas(writers_schema, readers_schema)
          raise SchemaMatchException.new(writers_schema, readers_schema)
        end

        # schema resolution: reader's schema is a union, writer's schema is not
        if writers_schema.type_sym != :union && readers_schema.type_sym == :union
          rs = readers_schema.schemas.find{|s|
            ::Avro::IO.match_schemas(writers_schema, s)
          }
          return read_data(writers_schema, rs, decoder, data) if rs
          raise SchemaMatchException.new(writers_schema, readers_schema)
        end

        validate_data(writers_schema, data)

        # function dispatch for reading data based on type of writer's schema
        case writers_schema.type_sym
        when :null;    decoder.decode_null(data)
        when :boolean; decoder.decode_boolean(data)
        when :string;  decoder.decode_string(data)
        when :int;     decoder.decode_int(data)
        when :long;    decoder.decode_long(data)
        when :float;   decoder.decode_float(data)
        when :double;  decoder.decode_double(data)
        when :bytes;   decoder.decode_bytes(data)
        when :fixed;   decoder.decode_fixed(data)
        when :enum;    data
        when :array
          read_items = []
          data.each do |item|
            read_items << read_data(writers_schema.items, readers_schema.items, decoder, item)
          end
          return read_items
        when :map
          read_items = {}
          data.each_pair do |key, value|
            read_items[key] = read_data(writers_schema.values, readers_schema.values, decoder, value)
          end
          return read_items
        when :union
          read_union(writers_schema, readers_schema, decoder, data)
        when :record, :error, :request
          read_record(writers_schema, readers_schema, decoder, data)
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

      def read_union(writers_schema, readers_schema, decoder, data)
        if data.nil?
          schema_type, value = :null, nil
        else
          schema_type, value = data.first
        end

        data_schema = writers_schema.schemas.find do |writer_schema|
          writer_schema.type_sym == schema_type.to_sym ||
                                    (writer_schema.name == schema_type if writer_schema.respond_to?(:name))
        end

        read_data(data_schema, readers_schema, decoder, value)
      end

      def read_record(writers_schema, readers_schema, decoder, data)
        readers_fields_hash = readers_schema.fields_hash
        read_record = {}
        writers_schema.fields.each do |field|
          if readers_field = readers_fields_hash[field.name]
            field_val = read_data(field.type, readers_field.type, decoder, data[field.name])
            read_record[field.name] = field_val
          end
        end

        # fill in the default values
        if readers_fields_hash.size > read_record.size
          writers_fields_hash = writers_schema.fields_hash
          readers_fields_hash.each do |field_name, field|
            unless writers_fields_hash.has_key? field_name
              if !field.default.nil?
                field_val = read_default_value(field.type, decoder, field.default)
                read_record[field.name] = field_val
              end
            end
          end
        end

        read_record
      end

      def read_default_value(field_schema, decoder, default_value)
        case field_schema.type_sym
        when :null;    decoder.decode_null(default_value)
        when :boolean; decoder.decode_boolean(default_value)
        when :string;  decoder.decode_string(default_value)
        when :int;     decoder.decode_int(default_value)
        when :long;    decoder.decode_long(default_value)
        when :float;   decoder.decode_float(default_value)
        when :double;  decoder.decode_double(default_value)
        when :bytes;   decoder.decode_bytes(default_value)
        when :fixed;   decoder.decode_fixed(default_value)
        when :enum;    default_value
        when :array
          read_array = []
          default_value.each do |json_val|
            item_val = read_default_value(field_schema.items, decoder, json_val)
            read_array << item_val
          end
          return read_array
        when :map
          read_map = {}
          default_value.each do |key, json_val|
            map_val = read_default_value(field_schema.values, decoder, json_val)
            read_map[key] = map_val
          end
          return read_map
        when :union
          return read_default_value(field_schema.schemas[0], decoder, default_value)
        when :record, :error
          read_record = {}
          field_schema.fields.each do |field|
            json_val = default_value[field.name]
            json_val = field.default unless json_val
            field_val = read_default_value(field.type, decoder, json_val)
            read_record[field.name] = field_val
          end
          return read_record
        else
          fail_msg = "Unknown type: #{field_schema.type}"
          raise AvroError, fail_msg
        end
      end
    end
  end
end
