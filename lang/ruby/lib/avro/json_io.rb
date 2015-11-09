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

require 'avro/io'
require 'json'

module Avro
  module IO
    # JsonDecoder keeps track of the current data and reads it using a given data type.
    #
    # About validations - If JSONDatumReader has its own read_data implementation, data validations can be moved there instead.
    # That way reader can reuse Schema.validate method and have same behavior as the writer in case of schema and data mismatch.
    class JsonDecoder
      attr_reader :reader
      attr_accessor :data

      def read_from(reader)
        datum = reader.read
        datum = '""' if datum.empty?
        data = JSON.parse(datum, {:quirks_mode => true})

        @data = data

        self
      end

      def read_null
        error('null') unless data.nil?
        nil
      end

      def read_boolean
        error('boolean') unless Schema.is_boolean?(data)
        data
      end

      def read_int
        error('int') unless Schema.is_integer?(data)

        Integer(data)
      end

      def read_long
        error('"long"') unless Schema.is_long?(data)

        Integer(data)
      end

      def read_float
        error('"float"') unless Schema.is_floating_point?(data)

        Float(data)
      end

      def read_double
        read_float
      end

      def read_bytes
        error('"bytes"') unless data.is_a?(String)
        data.encode(Encoding::ISO_8859_1, Encoding::UTF_8)
      end

      def read_fixed(size)
        error("\"fixed of size: #{size}\"") unless Schema.is_fixed?(data, size)
        read_bytes
      end

      def read_string
        error('string') unless data.is_a?(String)
        data ? data.encode(Encoding::UTF_8) : data
      end

      def error(data_type)
        raise AvroTypeError.new(data_type, data)
      end
    end # JsonDecoder

    # Unlike DatumReader, JsonDatumReader reads the file first so that it can decode schema with the JSON data.
    # It uses JsonDecoder to keep track of current data as it is going through the schema.
    class JsonDatumReader < DatumReader
      def read_record(writers_schema, readers_schema, decoder)
        readers_fields_hash = readers_schema.fields_hash
        read_record = {}

        data = decoder.data
        writers_schema.fields.each do |field|
          if readers_field = readers_fields_hash[field.name]
            field_data = data ? data[field.name] : nil
            field_decoder = decoder.class.new
            field_decoder.data = field_data
            field_val = read_data(field.type, readers_field.type, field_decoder)
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

      def read_fixed(writers_schema, readers_schema, decoder)
        decoder.read_fixed(writers_schema.size)
      end

      def read_enum(writers_schema, readers_schema, decoder)
        unless Schema.validate(writers_schema, decoder.data)
          raise AvroTypeError.new(writers_schema, decoder.data)
        end
        decoder.data
      end

      def read_array(writers_schema, readers_schema, decoder)
        read_items = []
        data = decoder.data
        data.each do |item|
          item_decoder = decoder.class.new
          item_decoder.data = item
          read_items << read_data(writers_schema.items, readers_schema.items, item_decoder)
        end if decoder.data
        return read_items
      end

      def read_map(writers_schema, readers_schema, decoder)
        read_items = {}
        data = decoder.data
        data.each_pair do |key, value|
          value_decoder = decoder.class.new
          value_decoder.data = value
          read_items[key] = read_data(writers_schema.values, readers_schema.values, value_decoder)
        end
        return read_items
      end

      def read_union(writers_schema, readers_schema, decoder)
        if decoder.data.nil?
          schema_type, value = :null, nil
        else
          schema_type, value = decoder.data.first
        end

        data_schema = writers_schema.schemas.find do |writer_schema|
          writer_schema.type_sym == schema_type.to_sym ||
                                    (writer_schema.name == schema_type if writer_schema.respond_to?(:name))
        end

        value_decoder = decoder.class.new
        value_decoder.data = value
        read_data(data_schema, readers_schema, value_decoder)
      end
    end # JsonDatumReader

    # JsonEncoder encodes a given datum and, when asked, writes it to the writer.
    class JsonEncoder
      attr_reader :writer
      attr_accessor :current_data

      def initialize(writer)
        @writer = writer
      end

      def write
        writer.write(JSON.dump(@current_data))
      end

      def write_null(datum)
        @current_data = nil
      end

      def write_boolean(datum)
        @current_data = datum
      end

      def write_int(n)
        write_long(n)
      end

      def write_long(n)
        @current_data = Integer(n)
      end

      def write_float(n)
        @current_data = Float(n)
      end

      def write_double(n)
        write_float(n)
      end

      def write_bytes(datum)
        @current_data = datum.encode(Encoding::UTF_8, Encoding::ISO_8859_1)
      end

      def write_fixed(datum)
        @current_data = write_bytes(datum)
      end

      def write_string(datum)
        @current_data = datum.encode(Encoding::UTF_8)
      end
    end # JsonEncoder

    # Unlike DatumWriter, JsonDatumWriter writes the encoded data in memory. At the end, it writes the data to the writer.
    class JsonDatumWriter < DatumWriter
      def write(datum, encoder)
        super
        encoder.write
      end

      def write_record(writers_schema, datum, encoder)
        record = {}
        writers_schema.fields.each do |field|
          write_data(field.type, datum[field.name], encoder)
          record[field.name] = encoder.current_data
        end
        encoder.current_data = record
      end

      def write_array(writers_schema, datum, encoder)
        items = []
        datum.each do |item|
          write_data(writers_schema.items, item, encoder)
          items << encoder.current_data
        end
        encoder.current_data = items
      end

      def write_map(writers_schema, datum, encoder)
        object = {}
        datum.each do |k,v|
          write_data(writers_schema.values, v, encoder)
          object[k] = encoder.current_data
        end
        encoder.current_data = object
      end

      def write_union(writers_schema, datum, encoder)
        datum_schema = writers_schema.schemas.find {|schema| Schema.validate(schema, datum)}
        raise AvroTypeError.new(writers_schema, datum) unless datum_schema

        write_data(datum_schema, datum, encoder)

        if datum_schema.type_sym == :null
          encoder.current_data
        else
          data = {}
          if datum_schema.respond_to?(:name)
            data[datum_schema.name] = encoder.current_data
          else
            data[datum_schema.type_sym.to_s] = encoder.current_data
          end
          encoder.current_data = data
        end
      end

      def write_fixed(writers_schema, datum, encoder)
        encoder.write_fixed(datum)
      end

      def write_enum(writers_schema, datum, encoder)
        encoder.write_string(datum.to_s)
      end
    end # JsonDatumWriter
  end
end
