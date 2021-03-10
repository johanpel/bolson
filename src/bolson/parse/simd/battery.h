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

#include <CLI/CLI.hpp>
#include <vector>

#include "bolson/parse/parser.h"
#include "bolson/status.h"

namespace bolson::parse::simd {

struct BatteryOptions {
  /// Number of input buffers to use.
  size_t num_buffers = 0;
  /// Capacity of input buffers.
  size_t buf_capacity = 256 * 1024 * 1024;
};

void AddBatteryOptionsToCLI(CLI::App* sub, BatteryOptions* out);

class BatteryParser : public Parser {
 public:
  auto Parse(const std::vector<illex::JSONBuffer*>& in, std::vector<ParsedBatch>* out)
      -> Status override;
};

class BatteryParserContext : public ParserContext {
 public:
  static auto Make(const BatteryOptions& opts, size_t num_parsers,
                   std::shared_ptr<ParserContext>* out) -> Status;

  auto parsers() -> std::vector<std::shared_ptr<Parser>> override;

  [[nodiscard]] auto schema() const -> std::shared_ptr<arrow::Schema> override;

 private:
  BatteryParserContext() = default;

  std::vector<std::shared_ptr<BatteryParser>> parsers_;
};

}  // namespace bolson::parse::simd
