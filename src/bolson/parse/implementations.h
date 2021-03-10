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

#include <CLI/CLI.hpp>

#include "bolson/parse/arrow.h"
#include "bolson/parse/opae/battery.h"
#include "bolson/parse/opae/trip.h"
#include "bolson/parse/simd.h"
#include "bolson/parse/simd/battery.h"

namespace bolson::parse {

/// Available parser implementations.
enum class Impl {
  ARROW,         ///< A CPU version based on Arrow's internal JSON parser using RapidJSON.
  OPAE_BATTERY,  ///< An FPGA version for the "battery status" schema.
  OPAE_TRIP,     ///< An FPGA version for for the "trip report" schema.
  SIMD,          ///< simdjson for any schema
  SIMD_BATTERY,  ///< simdjson version for the "battery status schema.
};

/// All parser options.
struct ParserOptions {
  // Would have been nice to use a variant, but it doesn't work nicely with the CLI stuff.
  Impl impl = Impl::ARROW;
  ArrowOptions arrow;
  SimdOptions simd;
  opae::BatteryOptions opae_battery;
  opae::TripOptions opae_trip;
  simd::BatteryOptions simd_battery;

  static auto impls_map() -> std::map<std::string, parse::Impl> {
    static std::map<std::string, parse::Impl> result = {
        {"arrow", parse::Impl::ARROW},
        {"opae-battery", parse::Impl::OPAE_BATTERY},
        {"opae-trip", parse::Impl::OPAE_TRIP},
        {"simd", parse::Impl::SIMD},
        {"simd-battery", parse::Impl::SIMD_BATTERY}};

    return result;
  }
};

inline void AddParserOptions(CLI::App* sub, ParserOptions* opts) {
  sub->add_option("-p,--parser", opts->impl,
                  "Parser implementation. OPAE parsers have fixed schema and ignore "
                  "schema supplied to -i.")
      ->transform(CLI::CheckedTransformer(ParserOptions::impls_map(), CLI::ignore_case))
      ->default_val(parse::Impl::ARROW);

  parse::AddArrowOptionsToCLI(sub, &opts->arrow);
  parse::AddSimdOptionsToCLI(sub, &opts->simd);
  parse::opae::AddBatteryOptionsToCLI(sub, &opts->opae_battery);
  parse::opae::AddTripOptionsToCLI(sub, &opts->opae_trip);
}

inline auto ToString(const Impl& impl) -> std::string {
  switch (impl) {
    case Impl::ARROW:
      return "Arrow (CPU)";
    case Impl::SIMD:
      return "simdjson (CPU)";
    case Impl::OPAE_BATTERY:
      return "OPAE battery status (FPGA)";
    case Impl::OPAE_TRIP:
      return "OPAE trip report (FPGA)";
    case Impl::SIMD_BATTERY:
      return "simdjson battery status (CPU)";
  }
}

}  // namespace bolson::parse