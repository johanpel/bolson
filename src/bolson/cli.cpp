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

#include <algorithm>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/json/api.h>

#include "bolson/log.h"
#include "bolson/cli.h"
#include "bolson/status.h"
#include "bolson/stream.h"

namespace bolson {

/// Function to read schema from a file.
static auto ReadSchemaFromFile(const std::string &file,
                               std::shared_ptr<arrow::Schema> *out) -> Status {
  // TODO(johanpel): use filesystem lib for path
  auto rfos = arrow::io::ReadableFile::Open(file);

  if (!rfos.ok()) { return Status(Error::IOError, rfos.status().message()); }
  auto fis = rfos.ValueOrDie();

  // Dictionaries are not supported yet, hence nullptr. If there are actual dictionaries,
  // the function will return an error status, which is propagated to the caller of this
  // function.
  auto rsch = arrow::ipc::ReadSchema(fis.get(), nullptr);

  if (rsch.ok()) { *out = rsch.ValueOrDie(); }
  else { return Status(Error::IOError, rsch.status().message()); }

  auto status = fis->Close();

  return Status::OK();
}

/// Create a mock IPC message to figure out the IPC message header size.
static auto CreateMockIPC(const std::shared_ptr<arrow::Schema> &schema,
                          std::shared_ptr<arrow::Buffer> *out) -> Status {
  // Construct an empty RecordBatch from the schema.
  std::unique_ptr<arrow::RecordBatchBuilder> mock_builder;
  std::shared_ptr<arrow::RecordBatch> mock_batch;
  // TODO: Error check the Arrow results and statuses below
  arrow::Status status = arrow::RecordBatchBuilder::Make(schema,
                                                         arrow::default_memory_pool(),
                                                         &mock_builder);
  // todo: write status converters from arrow, and perhaps the other libs also
  if (!status.ok()) { return Status(Error::ArrowError, status.message()); }
  status = mock_builder->Flush(&mock_batch);
  if (!status.ok()) { return Status(Error::ArrowError, status.message()); }
  auto result = arrow::ipc::SerializeRecordBatch(*mock_batch,
                                                 arrow::ipc::IpcWriteOptions::Defaults());
  if (!result.ok()) { return Status(Error::ArrowError, result.status().message()); }
  (*out) = result.ValueOrDie();
  return Status::OK();
}

static auto CalcThreshold(size_t max_size,
                          const std::shared_ptr<arrow::Schema> &schema,
                          size_t *out) -> Status {
  // Create empty IPC message to get an indication of the IPC header size.
  std::shared_ptr<arrow::Buffer> mock_ipc;
  BOLSON_ROE(CreateMockIPC(schema, &mock_ipc));

  // Subtract IPC header size from maximum Pulsar size.
  ssize_t desired_threshold = static_cast<ssize_t>(max_size) - mock_ipc->size();

  // Warn the user in case the threshold is below zero.
  if (desired_threshold < 0) {
    spdlog::warn(
        "Arrow IPC header size as result of supplied Arrow schema is larger than supplied"
        " Pulsar maximum message size. Setting batch threshold to 1 byte.");
  }

  *out = static_cast<size_t>(std::max(1L, desired_threshold));

  return Status::OK();
}

static void AddConvertOpts(CLI::App *sub, convert::Impl *impl, size_t *json_thresh) {
  std::map<std::string, convert::Impl> conversion_map{{"cpu", convert::Impl::CPU},
                                                      {"opae-battery",
                                                       convert::Impl::OPAE_BATTERY}};
  sub->add_option("--conversion", *impl, "Converter implementation.")
      ->transform(CLI::CheckedTransformer(conversion_map, CLI::ignore_case))
      ->default_val(convert::Impl::CPU);
  sub->add_option("--json-buffer-threshold",
                  *json_thresh,
                  "Number of JSONs to buffer before converting.")
      ->default_val(1024);
}

static void AddStatsOpts(CLI::App *sub, bool *csv) {
  sub->add_flag("-c", *csv, "Print output CSV-style.");
}

static void AddArrowOpts(CLI::App *sub, std::string *schema_file) {
  sub->add_option("input,-i,--input",
                  *schema_file,
                  "The Arrow schema to generate the JSON from.")
      ->check(CLI::ExistingFile)->required();
}

static void AddThreadsOpts(CLI::App *sub, size_t *num_threads) {
  sub->add_option("--threads", *num_threads, "Number of threads to use.")
      ->default_val(1);
}

static void AddPulsarOpts(CLI::App *sub, PulsarOptions *pulsar) {
  sub->add_option("-u,--pulsar-url", pulsar->url, "Pulsar broker service URL.")
      ->default_str("pulsar://localhost:6650/");
  sub->add_option("-t,--pulsar-topic", pulsar->topic, "Pulsar topic.")
      ->default_str("bolson");
  sub->add_option("-r,--pulsar-max-msg-size",
                  pulsar->max_msg_size,
                  "Pulsar max. message size (bytes).")
      ->default_val(PULSAR_DEFAULT_MAX_MESSAGE_SIZE);
}

auto AppOptions::FromArguments(int argc, char **argv, AppOptions *out) -> Status {
  std::string schema_file;
  uint16_t stream_port = 0;
  bool csv = false;

  CLI::App app{"bolson : A JSON stream to Arrow IPC to Pulsar conversion and publish "
               "tool."};

  app.require_subcommand();
  app.get_formatter()->column_width(50);

  // 'file' subcommand:
  auto *file = app.add_subcommand("file", "Produce Pulsar messages from a JSON file.");
  file->add_option("f,-f,--file", out->file.input, "Input file with Tweets.")
      ->check(CLI::ExistingFile)->required();
  AddArrowOpts(file, &schema_file);
  AddPulsarOpts(file, &out->file.pulsar);


  // 'stream' subcommand:
  auto *stream = app.add_subcommand("stream",
                                    "Produce Pulsar messages from a JSON TCP stream.");
  auto *port_opt = stream->add_option("-p,--port", stream_port, "Port.")
      ->default_val(illex::RAW_PORT);
  // The below option needs to be removed because the sequence numbers are used for the
  // latency timers:
//  stream->add_option("--seq",
//                     out->stream.seq,
//                     "Starting sequence number, 64-bit unsigned integer.")
//      ->default_val(0);
  stream->add_option("--l-samples",
                     out->stream.latency.num_samples,
                     "Number of latency samples.")
      ->default_val(1024);
  stream->add_option("--l-interval", out->stream.latency.interval)
      ->default_val(1024);
  AddConvertOpts(stream, &out->stream.conversion, &out->stream.json_threshold);
  AddStatsOpts(stream, &csv);
  AddArrowOpts(stream, &schema_file);
  AddPulsarOpts(stream, &out->stream.pulsar);
  AddThreadsOpts(stream, &out->stream.num_threads);


  // 'bench' subcommand:
  auto *bench = app.add_subcommand("bench", "Run some micro-benchmarks.")
      ->require_subcommand();
  AddStatsOpts(bench, &csv);

  // 'bench client' subcommand.
  auto *bench_client =
      bench->add_subcommand("client", "Run TCP client interface microbenchmark.");

  // 'bench convert' subcommand.
  auto *bench_conv =
      bench->add_subcommand("convert", "Run JSON to Arrow IPC convert microbenchmark.");
  AddConvertOpts(bench_conv,
                 &out->bench.convert.conversion,
                 &out->bench.convert.json_threshold);
  bench_conv->add_option("--num-jsons",
                         out->bench.convert.num_jsons,
                         "Number of JSONs to convert.")
      ->default_val(1024);
  bench_conv->add_option("--max-ipc-size",
                         out->bench.convert.max_ipc_size,
                         "Maximum size of the IPC messages.")
      ->default_val(PULSAR_DEFAULT_MAX_MESSAGE_SIZE);
  bench_conv->add_option("--seed", out->bench.convert.generate.seed, "Generation seed.");
  AddArrowOpts(bench_conv, &schema_file);
  AddThreadsOpts(bench_conv, &out->bench.convert.num_threads);

  // 'bench pulsar' subcommand.
  auto *bench_pulsar =
      bench->add_subcommand("pulsar", "Run Pulsar microbenchmark.");
  bench_pulsar->add_option("--message-size",
                           out->bench.pulsar.message_size,
                           "Pulsar message size.")
      ->default_val(PULSAR_DEFAULT_MAX_MESSAGE_SIZE);
  bench_pulsar->add_option("--num-messages",
                           out->bench.pulsar.num_messages,
                           "Pulsar number of messages.")->default_val(1024);
  AddPulsarOpts(bench, &out->bench.pulsar.pulsar);

  // 'bench queue' subcommand
  auto *bench_queue = bench->add_subcommand("queue", "Run queue microbenchmark.");
  bench_queue->add_option("m,-m,--num-items,", out->bench.queue.num_items)
      ->default_val(256);

  // Attempt to parse the CLI arguments.
  try {
    app.parse(argc, argv);
  } catch (CLI::CallForHelp &e) {
    // User wants to see help.
    std::cout << app.help() << std::endl;
    return Status::OK();
  } catch (CLI::Error &e) {
    // There is some CLI error.
    std::cerr << app.help() << std::endl;
    return Status(Error::CLIError, "CLI Error: " + e.get_name() + ":" + e.what());
  }

  std::shared_ptr<arrow::Schema> schema;
  arrow::json::ParseOptions parse_options;
  arrow::json::ReadOptions read_options;

  if (!schema_file.empty()) {
    // Read the Arrow schema.
    BOLSON_ROE(ReadSchemaFromFile(schema_file, &schema));

    // Generate parse options.
    parse_options = arrow::json::ParseOptions::Defaults();
    parse_options.explicit_schema = schema;
    parse_options.unexpected_field_behavior = arrow::json::UnexpectedFieldBehavior::Error;
  }

  read_options.use_threads = false;
  read_options.block_size = 2 * read_options.block_size;

  if (file->parsed()) {
    out->sub = SubCommand::FILE;
    out->file.succinct = out->succinct;
  } else if (stream->parsed()) {
    out->sub = SubCommand::STREAM;

    illex::RawProtocol raw;
    if (*port_opt) {
      raw.port = stream_port;
    }
    out->stream.protocol = raw;
    out->stream.succinct = csv;
    out->stream.parse = parse_options;
    out->stream.read = read_options;
    // TODO: move this to subcommand execution
    BOLSON_ROE(CalcThreshold(out->stream.pulsar.max_msg_size,
                             schema,
                             &out->stream.batch_threshold));

  } else if (bench->parsed()) {
    out->sub = SubCommand::BENCH;
    if (bench_client->parsed()) {
      out->bench.bench = Bench::CLIENT;
      return Status(Error::GenericError, "Not yet implemented.");
    } else if (bench_conv->parsed()) {
      out->bench.bench = Bench::CONVERT;
      out->bench.convert.csv = csv;
      out->bench.convert.schema = schema;
      out->bench.convert.parse_opts = parse_options;
      out->bench.convert.read_opts = read_options;
      BOLSON_ROE(CalcThreshold(out->bench.convert.max_ipc_size,
                               schema,
                               &out->bench.convert.batch_threshold));
    } else if (bench_pulsar->parsed()) {
      out->bench.bench = Bench::PULSAR;
      out->bench.pulsar.csv = csv;
    } else if (bench_queue->parsed()) {
      out->bench.bench = Bench::QUEUE;
    }
  }

  return Status::OK();
}

}  // namespace bolson
