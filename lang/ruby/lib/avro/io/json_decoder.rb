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
require 'json'

module Avro
  module IO
    class JsonDecoder
      # reader is an object on which we can call read, seek and tell.
      attr_reader :reader

      def initialize(reader)
        @reader = reader
      end

      def data
        datum = reader.read
        datum = '""' if datum.empty?
        ::JSON.parse(datum, {:quirks_mode => true})
      end

      def decode_null(datum)
        nil
      end

      def decode_boolean(datum)
        datum
      end

      def decode_int(n)
        decode_long(n)
      end

      def decode_long(n)
        Integer(n)
      end

      def decode_float(n)
        Float(n)
      end

      def decode_double(n)
        decode_float(n)
      end

      def decode_bytes(datum)
        datum.encode(Encoding::ISO_8859_1, Encoding::UTF_8)
      end

      def decode_fixed(datum)
        decode_bytes(datum)
      end

      def decode_string(datum)
        datum.encode(Encoding::UTF_8)
      end
    end
  end
end
