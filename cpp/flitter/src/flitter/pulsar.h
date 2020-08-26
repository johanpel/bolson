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

#include <memory>
#include <arrow/api.h>
#include <pulsar/Client.h>
#include <pulsar/Producer.h>

#include "./log.h"

namespace flitter {

/// @brief Pulsar options.
struct PulsarOptions {
  std::string url = "pulsar://localhost:6650/";
  std::string topic = "flitter";
  // From an obscure place in the Pulsar sources
  size_t max_msg_size = (5 * 1024 * 1024 - (10 * 1024));
};

// A custom logger for Pulsar.
class FlitterLogger : public pulsar::Logger {
  std::string _logger;
 public:
  explicit FlitterLogger(std::string logger);
  auto isEnabled(Level level) -> bool override;
  void log(Level level, int line, const std::string &message) override;
};

class FlitterLoggerFactory : public pulsar::LoggerFactory {
 public:
  auto getLogger(const std::string &fileName) -> pulsar::Logger * override;
  static auto create() -> std::unique_ptr<FlitterLoggerFactory>;
};

/**
 * Set a Pulsar client and producer up.
 * \param url    The Pulsar broker service URL.
 * \param topic  The Pulsar topic to produce message in.
 * \param logger        A logging device.
 * \param out           A pair with shared pointers to the client and producer objects.
 * \return              The Pulsar result of connecting the producer.
 */
auto SetupClientProducer(const std::string &url,
                         const std::string &topic,
                         pulsar::LoggerFactory *logger,
                         std::pair<std::shared_ptr<pulsar::Client>,
                                   std::shared_ptr<pulsar::Producer>> *out) -> pulsar::Result;

/**
 * Publish an Arrow buffer as a Pulsar message through a Pulsar producer.
 * \param producer      The Pulsar producer to publish the message through.
 * \param buffer        The Arrow buffer to publish.
 * \return              The Pulsar result of sending the message.
 */
auto PublishArrowBuffer(const std::shared_ptr<pulsar::Producer> &producer,
                        const std::shared_ptr<arrow::Buffer> &buffer) -> pulsar::Result;


}  // namespace flitter
