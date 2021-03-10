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

#include <putong/status.h>

#include <iostream>
#include <vector>

namespace bolson {

/// Error types.
enum class Error {
  GenericError,  ///< Uncategorized errors.
  CLIError,      ///< Errors related to the command-line interface.
  PulsarError,   ///< Errors related to Pulsar.
  IllexError,    ///< Errors related to Illex.
  ArrowError,    ///< Errors related to Arrow.
  IOError,       ///< Errors related to input/output.
  OpaeError,     ///< Errors related to FPGA impl.
  SimdError      ///< Errors related to the simdjson impl.
};

/// \brief Return human-readable Error enum.
auto ToString(Error e) -> std::string;

/// Bolson status type.
using Status = putong::Status<Error>;

/// Status type reduce taking preference to error status.
inline auto operator+=(const Status& lhs, const Status& rhs) -> Status {
  if (!lhs.ok()) {
    return lhs;
  } else {
    return rhs;
  };
}

/// Status from multiple threads.
using MultiThreadStatus = std::vector<Status>;

/**
 * \brief Aggregate status from multiple threads.
 * \param status The statuses from multiple threads.
 * \param prefix Prefix for error messages.
 * \return A single status.
 */
auto Aggregate(const MultiThreadStatus& status, const std::string& prefix = "") -> Status;

/// Return on error status.
#define BOLSON_ROE(s)                    \
  {                                      \
    auto __status = (s);                 \
    if (!__status.ok()) return __status; \
  }                                      \
  void()

/// Convert Arrow status and return on error.
#define ARROW_ROE(s)                                                           \
  {                                                                            \
    auto __status = (s);                                                       \
    if (!__status.ok()) return Status(Error::ArrowError, __status.ToString()); \
  }                                                                            \
  void()

/// Convert Illex status and return on error.
#define BILLEX_ROE(s)                                                     \
  {                                                                       \
    auto __status = (s);                                                  \
    if (!__status.ok()) return Status(Error::IllexError, __status.msg()); \
  }                                                                       \
  void()

}  // namespace bolson