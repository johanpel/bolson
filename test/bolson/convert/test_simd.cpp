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

  std::shared_ptr<parse::ArrowDOMWalker> walker;
  parse::ArrowDOMWalker::Make(schema, &walker);

  for (auto obj : parsed_objects) {
    FAIL_ON_ERROR(walker->Append(obj));
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  walker->Finish(&batch);
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

  std::shared_ptr<parse::ArrowDOMWalker> walker;
  parse::ArrowDOMWalker::Make(schema, &walker);

  for (auto obj : parsed_objects) {
    FAIL_ON_ERROR(walker->Append(obj));
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  walker->Finish(&batch);
  // TODO: write check
  std::cout << batch->ToString() << std::endl;
}

struct DOMNode {
  
};

TEST(Simd, DOMWalkerSequence) {

}

}  // namespace bolson::convert
