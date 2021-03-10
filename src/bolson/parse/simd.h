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

#pragma once

#include <arrow/api.h>
#include <illex/client_buffering.h>
#include <simdjson.h>

#include <CLI/CLI.hpp>
#include <utility>
#include <vector>

#include "bolson/parse/parser.h"
#include "bolson/status.h"

namespace bolson::parse {

// Macro to convert simdjson result to Bolson status.
#define SIMD_ROE(x)                                                         \
  {                                                                         \
    auto result__ = x;                                                      \
    if (result__.error()) {                                                 \
      return Status(Error::SimdError, sj::error_message(result__.error())); \
    }                                                                       \
  }

/// \brief Options for SimdJSON parser.
struct SimdOptions {
  /// Arrow schema.
  std::shared_ptr<arrow::Schema> schema = nullptr;
  /// Path to Arrow schema.
  std::string schema_path;
  /// Number of input buffers to use.
  size_t num_buffers = 0;
  /// Capacity of input buffers.
  size_t buf_capacity = 256 * 1024 * 1024;
  /// Whether to store sequence numbers as a column.
  bool seq_column = true;

  auto ReadSchema() -> Status;
};

void AddSimdOptionsToCLI(CLI::App* sub, SimdOptions* out);

class ArrowDOMWalker {
 public:
  using ArrowField = std::shared_ptr<arrow::Field>;
  using ArrowFields = std::vector<ArrowField>;

  static auto Make(const std::shared_ptr<arrow::Schema>& schema,
                   std::shared_ptr<ArrowDOMWalker>* out) -> Status;

  [[nodiscard]] inline auto Append(const simdjson::dom::object& object) {
    BOLSON_ROE(AppendObjectAsRecord(object, batch_builder.get()));
    return Status::OK();
  }

  [[nodiscard]] inline auto Finish(std::shared_ptr<arrow::RecordBatch>* out) -> Status {
    ARROW_ROE(batch_builder->Flush(out));
    return Status::OK();
  }

 protected:
  ArrowDOMWalker() = default;

  [[nodiscard]] static auto AppendElement(const simdjson::dom::element& element,
                                          const ArrowField& expected_field,
                                          arrow::ArrayBuilder* builder) -> Status;

  [[nodiscard]] static auto AppendArrayAsList(const simdjson::dom::array& array,
                                              const ArrowField& item_field,
                                              arrow::ListBuilder* list_builder) -> Status;

  [[nodiscard]] static auto AppendObjectAsStruct(const simdjson::dom::object& object,
                                                 const ArrowFields& expected_fields,
                                                 arrow::StructBuilder* struct_builder)
      -> Status;

  [[nodiscard]] static auto AppendObjectAsRecord(const simdjson::dom::object& object,
                                                 arrow::RecordBatchBuilder* batch_builder)
      -> Status;

  std::unique_ptr<arrow::RecordBatchBuilder> batch_builder;
};

class SimdParser : public Parser {
 public:
  static auto Make(const std::shared_ptr<arrow::Schema>& schema,
                   std::shared_ptr<SimdParser>* out) -> Status;

  auto Parse(const std::vector<illex::JSONBuffer*>& in, std::vector<ParsedBatch>* out)
      -> Status override;

 protected:
  SimdParser() = default;

  simdjson::dom::parser parser;
  std::shared_ptr<ArrowDOMWalker> walker;
};

class SimdParserContext : public ParserContext {
 public:
  static auto Make(const SimdOptions& opts, size_t num_parsers,
                   std::shared_ptr<ParserContext>* out) -> Status;

  auto parsers() -> std::vector<std::shared_ptr<Parser>> override;

  [[nodiscard]] auto schema() const -> std::shared_ptr<arrow::Schema> override;

 private:
  SimdParserContext() = default;

  std::vector<std::shared_ptr<SimdParser>> parsers_;
  std::shared_ptr<arrow::Schema> schema_;
};

}  // namespace bolson::parse
