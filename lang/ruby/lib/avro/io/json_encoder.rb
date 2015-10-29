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
    class JsonEncoder
      attr_reader :writer

      def initialize(writer)
        @writer = writer
      end

      def encode_null(datum)
        nil
      end

      def encode_boolean(datum)
        datum
      end

      def encode_int(n)
        encode_long(n)
      end

      def encode_long(n)
        Integer(n)
      end

      def encode_float(n)
        Float(n)
      end

      def encode_double(n)
        encode_float(n)
      end

      def encode_bytes(datum)
        datum.encode(Encoding::UTF_8, Encoding::ISO_8859_1)
      end

      def encode_fixed(datum)
        encode_bytes(datum)
      end

      def encode_string(datum)
        datum.encode(Encoding::UTF_8)
      end
    end
  end
end
