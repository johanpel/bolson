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

#include "bolson/parse/simd/battery.h"

#include <simdjson.h>

#include <memory>

#include "bolson/log.h"

namespace bolson::parse::simd {

auto BatteryParser::Parse(const std::vector<illex::JSONBuffer*>& in,
                          std::vector<ParsedBatch>* out) -> Status {
  namespace sj = simdjson;
  sj::dom::parser parser;

  for (const auto& buf : in) {
    auto val_bld = std::make_shared<arrow::UInt64Builder>();
    arrow::ListBuilder lst_bld(arrow::default_memory_pool(), val_bld);

    sj::dom::document_stream docs =
        parser.parse_many(reinterpret_cast<const uint8_t*>(buf->data()), buf->size());

    size_t i = 0;
    for (auto doc : docs) {  // For each object
      ARROW_ROE(lst_bld.Append());
      auto array = doc["voltage"].get_array();
      if (array.error()) {
        return Status(Error::SimdError, sj::error_message(array.error()));
      }
      ARROW_ROE(val_bld->Reserve(array.size()));
      for (const auto& item : array) {
        auto val = item.get_uint64();
        if (val.error()) {
          return Status(Error::SimdError, sj::error_message(array.error()));
        }
        val_bld->UnsafeAppend(val.value());
      }
    }

    std::shared_ptr<arrow::ListArray> col;
    ARROW_ROE(lst_bld.Finish(&col));

    out->push_back(ParsedBatch(
        arrow::RecordBatch::Make(
            arrow::schema({arrow::field("voltage", arrow::list(arrow::uint64()), false)}),
            col->length(), {col}),
        buf->range()));
  }

  return Status::OK();
}

auto BatteryParserContext::Make(const BatteryOptions& opts, size_t num_parsers,
                                std::shared_ptr<ParserContext>* out) -> Status {
  auto result = std::shared_ptr<BatteryParserContext>(new BatteryParserContext());

  // Use default allocator.
  result->allocator_ = std::make_shared<buffer::Allocator>();

  // Initialize all parsers.
  result->parsers_ = std::vector<std::shared_ptr<BatteryParser>>(
      num_parsers, std::make_shared<BatteryParser>());
  *out = std::static_pointer_cast<ParserContext>(result);

  // Allocate buffers. Use number of parsers if number of buffers is 0 in options.
  auto num_buffers = opts.num_buffers == 0 ? num_parsers : opts.num_buffers;
  BOLSON_ROE(result->AllocateBuffers(num_buffers, opts.buf_capacity));

  *out = result;

  return Status::OK();
}

auto BatteryParserContext::parsers() -> std::vector<std::shared_ptr<Parser>> {
  return CastPtrs<Parser>(parsers_);
}
auto BatteryParserContext::schema() const -> std::shared_ptr<arrow::Schema> {
  return arrow::schema({arrow::field("voltage", arrow::uint64(), false)});
}

}  // namespace bolson::parse::simd
