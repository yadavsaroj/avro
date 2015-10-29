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
    class JsonDatumWriter
      attr_accessor :writers_schema
      def initialize(writers_schema=nil)
        @writers_schema = writers_schema
      end

      def write(datum, encoder)
        data = encode_data(writers_schema, datum, encoder)
        encoder.writer.write(::JSON.dump(data))
      end

      private

      def encode_data(writers_schema, datum, encoder)
        unless Schema.validate(writers_schema, datum)
          raise AvroTypeError.new(writers_schema, datum)
        end

        # function dispatch to write datum
        encoded_data = case writers_schema.type_sym
          when :null;    encoder.encode_null(datum)
          when :boolean; encoder.encode_boolean(datum)
          when :string;  encoder.encode_string(datum)
          when :int;     encoder.encode_int(datum)
          when :long;    encoder.encode_long(datum)
          when :float;   encoder.encode_float(datum)
          when :double;  encoder.encode_double(datum)
          when :bytes;   encoder.encode_bytes(datum)
          when :fixed;   encode_fixed(writers_schema, datum, encoder)
          when :enum;    datum.to_s
          when :array;   encode_array(writers_schema, datum, encoder)
          when :map;     encode_map(writers_schema, datum, encoder)
          when :union;   encode_union(writers_schema, datum, encoder)
          when :record, :error, :request;  encode_record(writers_schema, datum, encoder)
          else
            raise AvroError.new("Unknown type: #{writers_schema.type}")
          end

        encoded_data
      end

      def encode_fixed(writers_schema, datum, encoder)
        encoder.encode_fixed(datum)
      end

      def encode_array(writers_schema, datum, encoder)
        items = []

        datum.each do |item|
          items << encode_data(writers_schema.items, item, encoder)
        end

        items
      end

      def encode_map(writers_schema, pairs, encoder)
        object = {}

        pairs.each do |k,v|
          field_val = encode_data(writers_schema.values, v, encoder)
          object[k] = field_val
        end

        object
      end

      def encode_union(writers_schema, datum, encoder)
        datum_schema = writers_schema.schemas.find {|schema| Schema.validate(schema, datum)}
        raise AvroTypeError.new(writers_schema, datum) unless datum_schema

        encoded_data = encode_data(datum_schema, datum, encoder)

        if datum_schema.type_sym == :null
          encoded_data
        else
          data = {}

          if datum_schema.respond_to?(:name)
            data[datum_schema.name] = encoded_data
          else
            data[datum_schema.type_sym.to_s] = encoded_data
          end

          data
        end
      end

      def encode_record(writers_schema, datum, encoder)
        record = {}

        writers_schema.fields.each do |field|
          field_val = encode_data(field.type, datum[field.name], encoder)
          record[field.name] = field_val
        end

        record
      end
    end
  end
end
