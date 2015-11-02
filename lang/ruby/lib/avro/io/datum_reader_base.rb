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
require 'avro/io/json_decoder'

module Avro
  module IO
    class DatumReaderBase
      attr_accessor :writers_schema, :readers_schema

      def initialize(writers_schema=nil, readers_schema=nil)
        @writers_schema = writers_schema
        @readers_schema = readers_schema
      end

      def read(decoder)
        self.readers_schema = writers_schema unless readers_schema
      end

      def read_default_value(field_schema, default_value)
        case field_schema.type_sym
        when :null;    JsonDecoder.decode_null(default_value)
        when :boolean; JsonDecoder.decode_boolean(default_value)
        when :string;  JsonDecoder.decode_string(default_value)
        when :int;     JsonDecoder.decode_int(default_value)
        when :long;    JsonDecoder.decode_long(default_value)
        when :float;   JsonDecoder.decode_float(default_value)
        when :double;  JsonDecoder.decode_double(default_value)
        when :bytes;   JsonDecoder.decode_bytes(default_value)
        when :fixed;   JsonDecoder.decode_fixed(default_value)
        when :enum;    default_value
        when :array
          read_array = []
          default_value.each do |json_val|
            item_val = read_default_value(field_schema.items, json_val)
            read_array << item_val
          end
          return read_array
        when :map
          read_map = {}
          default_value.each do |key, json_val|
            map_val = read_default_value(field_schema.values, json_val)
            read_map[key] = map_val
          end
          return read_map
        when :union
          return read_default_value(field_schema.schemas[0], default_value)
        when :record, :error
          read_record = {}
          field_schema.fields.each do |field|
            json_val = default_value[field.name]
            json_val = field.default unless json_val
            field_val = read_default_value(field.type, json_val)
            read_record[field.name] = field_val
          end
          return read_record
        else
          fail_msg = "Unknown type: #{field_schema.type}"
          raise AvroError, fail_msg
        end
      end

      def ensure_schema_match(writers_schema, readers_schema)
        # schema matching
        unless ::Avro::IO.match_schemas(writers_schema, readers_schema)
          raise SchemaMatchException.new(writers_schema, readers_schema)
        end
      end

      def union_schema_resolution(writers_schema, readers_schema)
        # schema resolution: reader's schema is a union, writer's schema is not
        if writers_schema.type_sym != :union && readers_schema.type_sym == :union
          rs = readers_schema.schemas.find do |s|
            ::Avro::IO.match_schemas(writers_schema, s)
          end

          raise SchemaMatchException.new(writers_schema, readers_schema) unless rs
          rs
        end
      end
    end
  end
end
