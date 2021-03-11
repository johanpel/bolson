// Copyright 2020 Teratide B.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <simdjson.h>

#include <list>

#include "bolson/convert/test_convert.h"
#include "bolson/parse/simd.h"

namespace bolson::convert {

TEST(Simd, Simple) {
  auto schema = arrow::schema({arrow::field("a", arrow::int64())});

  std::string test_str(R"({"a":0}
{"a":1}
{"a":-2})");

  simdjson::dom::parser parser;
  simdjson::dom::document_stream parsed_objects = parser.parse_many(test_str);

  std::shared_ptr<parse::DOMVisitor> walker;
  parse::DOMVisitor::Make(schema, &walker);

  for (auto obj : parsed_objects) {
    FAIL_ON_ERROR(walker->Append(obj));
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  FAIL_ON_ERROR(walker->Finish(&batch));
  // TODO: write check
  std::cout << batch->ToString() << std::endl;
}

TEST(Simd, Battery) {
  auto schema = arrow::schema({arrow::field("voltage", arrow::list(arrow::uint64()))});

  std::string test_str(R"({"voltage":[0,1,2]}
{"voltage":[3,4]}
{"voltage":[5]}
{"voltage":[]})");

  simdjson::dom::parser parser;
  simdjson::dom::document_stream parsed_objects = parser.parse_many(test_str);

  std::shared_ptr<parse::DOMVisitor> walker;
  parse::DOMVisitor::Make(schema, &walker);

  for (auto obj : parsed_objects) {
    FAIL_ON_ERROR(walker->Append(obj));
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  FAIL_ON_ERROR(walker->Finish(&batch));
  // TODO: write check
  std::cout << batch->ToString() << std::endl;
}

TEST(Simd, DOMSequenceVisitor) {
  auto schema = arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::list(arrow::int64())),
       arrow::field(
           "c", arrow::struct_(
                    {arrow::field("d", arrow::int64()),
                     arrow::field("e", arrow::list(arrow::struct_(
                                           {arrow::field("f", arrow::int64()),
                                            arrow::field("g", arrow::int64())})))}))});

  parse::DOMSequenceVisitor v;

  v.Analyze(*schema);

  std::cout << v.ToString() << std::endl;

  std::string test_str(
      R"({"a":0,"b":[1,2,3],"c":{"d":4,"e":[{"f":5,"g":6},{"f":7,"g":8}]}}
{"a":0,"b":[10,11],"c":{"d":12,"e":[{"f":13,"g":14}]}})");

  simdjson::dom::parser parser;
  simdjson::dom::document_stream parsed_objects = parser.parse_many(test_str);

  for (auto elem : parsed_objects) {
    auto obj = elem.get_object();
    for (const auto& kv : obj) {
      std::cout << kv.key << "=" << kv.value << std::endl;
    }
    for (const auto& node : v.nodes()) {
      std::cout << node.name << ":";
      std::cout << obj.at_pointer(node.name) << std::endl;
    }
  }
}

}  // namespace bolson::convert
