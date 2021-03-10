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

#include "bolson/parse/simd.h"

#include <simdjson.h>

#include <memory>

#include "bolson/log.h"
#include "bolson/parse/arrow.h"

namespace bolson::parse {

namespace sj = simdjson;

void AddSimdOptionsToCLI(CLI::App* sub, SimdOptions* out) {
  // TODO: figure something out for options common with other impls
  sub->add_option("--simd-input", out->schema_path,
                  "Serialized Arrow schema file for records to convert to.")
      ->check(CLI::ExistingFile);
  sub->add_option("--simd-buf-cap", out->buf_capacity, "simdjson input buffer capacity.")
      ->default_val(256 * 1024 * 1024);
  sub->add_flag("--simd-seq-col", out->seq_column,
                "simdjson parser, retain ordering information by adding a sequence "
                "number column.")
      ->default_val(false);
}

auto SimdOptions::ReadSchema() -> Status {
  BOLSON_ROE(ReadSchemaFromFile(schema_path, &schema));
  return Status::OK();
}

auto SimdParser::Parse(const std::vector<illex::JSONBuffer*>& in,
                       std::vector<ParsedBatch>* out) -> Status {
  for (const auto& buf : in) {
    sj::dom::document_stream objects =
        parser.parse_many(reinterpret_cast<const uint8_t*>(buf->data()), buf->size());

    for (auto obj : objects) {
      walker->Append(obj);
    }

    std::shared_ptr<arrow::RecordBatch> batch_out;
    walker->Finish(&batch_out);

    out->push_back(ParsedBatch(batch_out, buf->range()));
  }

  return Status::OK();
}

auto SimdParser::Make(const std::shared_ptr<arrow::Schema>& schema,
                      std::shared_ptr<SimdParser>* out) -> Status {
  auto result = std::shared_ptr<SimdParser>(new SimdParser());
  BOLSON_ROE(ArrowDOMWalker::Make(schema, &result->walker));
  *out = result;
  return Status::OK();
}

auto SimdParserContext::Make(const SimdOptions& opts, size_t num_parsers,
                             std::shared_ptr<ParserContext>* out) -> Status {
  auto result = std::shared_ptr<SimdParserContext>(new SimdParserContext());

  // Use default allocator.
  result->allocator_ = std::make_shared<buffer::Allocator>();

  // Determine simdjson parser options.
  if (opts.schema == nullptr) {
    BOLSON_ROE(ReadSchemaFromFile(opts.schema_path, &result->schema_));
  } else {
    result->schema_ = opts.schema;
  }

  // Initialize all parsers.
  for (size_t i = 0; i < num_parsers; i++) {
    std::shared_ptr<SimdParser> p;
    BOLSON_ROE(SimdParser::Make(result->schema_, &p));
    result->parsers_.push_back(p);
  }

  *out = std::static_pointer_cast<ParserContext>(result);

  // Allocate buffers. Use number of parsers if number of buffers is 0 in options.
  auto num_buffers = opts.num_buffers == 0 ? num_parsers : opts.num_buffers;
  BOLSON_ROE(result->AllocateBuffers(num_buffers, opts.buf_capacity));

  *out = result;

  return Status::OK();
}

auto SimdParserContext::parsers() -> std::vector<std::shared_ptr<Parser>> {
  return CastPtrs<Parser>(parsers_);
}
auto SimdParserContext::schema() const -> std::shared_ptr<arrow::Schema> {
  return arrow::schema({arrow::field("voltage", arrow::uint64(), false)});
}

inline auto ToString(sj::dom::element_type type) -> std::string {
  switch (type) {
    case sj::dom::element_type::ARRAY:
      return "array";
    case sj::dom::element_type::OBJECT:
      return "object";
    case sj::dom::element_type::INT64:
      return "int64";
    case sj::dom::element_type::UINT64:
      return "uint64";
    case sj::dom::element_type::DOUBLE:
      return "double";
    case sj::dom::element_type::STRING:
      return "string";
    case sj::dom::element_type::BOOL:
      return "bool";
    case sj::dom::element_type::NULL_VALUE:
      return "null";
  }
  return "CORRUPT TYPE";
}

inline auto TypeErrorStatus(sj::dom::element_type json, const arrow::DataType& arrow)
    -> Status {
  return Status(Error::SimdError, "Encountered JSON type " + ToString(json) +
                                      " with unsupported Arrow type " + arrow.name());
}

auto ArrowDOMWalker::AppendArrayAsList(const sj::dom::array& array,
                                       const ArrowField& item_field,
                                       arrow::ListBuilder* list_builder) -> Status {
  assert(list_builder != nullptr);

  ARROW_ROE(list_builder->Append());
  ARROW_ROE(list_builder->value_builder()->Reserve(array.size()));

  for (const sj::dom::element& elem : array) {
    AppendElement(elem, item_field, list_builder->value_builder());
  }
  return Status::OK();
}

auto ArrowDOMWalker::AppendObjectAsStruct(const sj::dom::object& object,
                                          const ArrowFields& expected_fields,
                                          arrow::StructBuilder* struct_builder)
    -> Status {
  assert(struct_builder != nullptr);
  assert(expected_fields.size() == struct_builder->num_children());

  if (object.size() != expected_fields.size()) {
    // Not present keys could also be interpreted as null values?
    return Status(Error::SimdError,
                  "JSON Object with " + std::to_string(object.size()) +
                      " members does not match expected number of Arrow fields " +
                      std::to_string(expected_fields.size()));
  }

  for (size_t i = 0; i < expected_fields.size(); i++) {
    auto elem = object.at_key(expected_fields[i]->name());
    AppendElement(elem.value_unsafe(), expected_fields[i],
                  struct_builder->child_builder(i).get());
  }
  return Status::OK();
}

auto ArrowDOMWalker::AppendElement(const sj::dom::element& element,
                                   const ArrowField& expected_field,
                                   arrow::ArrayBuilder* builder) -> Status {
  assert(builder != nullptr);

  switch (element.type()) {
    case sj::dom::element_type::ARRAY: {
      // list or fixed size list builder
      switch (expected_field->type()->id()) {
        default:
          return TypeErrorStatus(element.type(), *expected_field->type());
        case arrow::Type::LIST:
          auto* list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
          auto item_field = expected_field->type()->field(0);
          BOLSON_ROE(AppendArrayAsList(element.get_array(), item_field, list_builder));
          break;
      }
    } break;
    case sj::dom::element_type::OBJECT: {
      auto* struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
      BOLSON_ROE(AppendObjectAsStruct(element.get_object(),
                                      expected_field->type()->fields(), struct_builder));
    } break;
    case sj::dom::element_type::INT64: {
      switch (expected_field->type()->id()) {
        default:
          return TypeErrorStatus(element.type(), *expected_field->type());
        case arrow::Type::INT8:
          ARROW_ROE(dynamic_cast<arrow::Int8Builder*>(builder)->Append(
              static_cast<int8_t>(element.get_int64())));
          break;
        case arrow::Type::INT16:
          ARROW_ROE(dynamic_cast<arrow::Int16Builder*>(builder)->Append(
              static_cast<int16_t>(element.get_int64())));
          break;
        case arrow::Type::INT32:
          ARROW_ROE(dynamic_cast<arrow::Int32Builder*>(builder)->Append(
              static_cast<int32_t>(element.get_int64())));
          break;
        case arrow::Type::INT64:
          ARROW_ROE(dynamic_cast<arrow::Int64Builder*>(builder)->Append(
              static_cast<int64_t>(element.get_int64())));
          break;
        case arrow::Type::UINT8:
          ARROW_ROE(dynamic_cast<arrow::UInt8Builder*>(builder)->Append(
              static_cast<uint8_t>(element.get_uint64())));
          break;
        case arrow::Type::UINT16:
          ARROW_ROE(dynamic_cast<arrow::UInt16Builder*>(builder)->Append(
              static_cast<uint16_t>(element.get_uint64())));
          break;
        case arrow::Type::UINT32:
          ARROW_ROE(dynamic_cast<arrow::UInt32Builder*>(builder)->Append(
              static_cast<uint32_t>(element.get_uint64())));
          break;
        case arrow::Type::UINT64:
          ARROW_ROE(dynamic_cast<arrow::UInt64Builder*>(builder)->Append(
              static_cast<uint64_t>(element.get_uint64())));
          break;
      }
    } break;
    case sj::dom::element_type::UINT64: {
      switch (expected_field->type()->id()) {
        default:
          return TypeErrorStatus(element.type(), *expected_field->type());
        case arrow::Type::UINT8:
          ARROW_ROE(dynamic_cast<arrow::UInt8Builder*>(builder)->Append(
              static_cast<uint8_t>(element.get_uint64())));
          break;
        case arrow::Type::UINT16:
          ARROW_ROE(dynamic_cast<arrow::UInt16Builder*>(builder)->Append(
              static_cast<uint16_t>(element.get_uint64())));
          break;
        case arrow::Type::UINT32:
          ARROW_ROE(dynamic_cast<arrow::UInt32Builder*>(builder)->Append(
              static_cast<uint32_t>(element.get_uint64())));
          break;
        case arrow::Type::UINT64:
          ARROW_ROE(dynamic_cast<arrow::UInt64Builder*>(builder)->Append(
              static_cast<uint64_t>(element.get_uint64())));
          break;
      }
    } break;
    case sj::dom::element_type::DOUBLE: {
      switch (expected_field->type()->id()) {
        default:
          return TypeErrorStatus(element.type(), *expected_field->type());
        case arrow::Type::DOUBLE:
          ARROW_ROE(dynamic_cast<arrow::DoubleBuilder*>(builder)->Append(
              static_cast<double_t>(element.get_double())));
          break;
      }
    } break;
    case sj::dom::element_type::STRING: {
      switch (expected_field->type()->id()) {
        default:
          return TypeErrorStatus(element.type(), *expected_field->type());
        case arrow::Type::STRING:
          auto* sb = dynamic_cast<arrow::StringBuilder*>(builder);
          auto str = element.get_string().value_unsafe();  // previously checked to be str
          ARROW_ROE(sb->Append(arrow::util::string_view(str.data(), str.length())));
          break;
      }
    } break;
    case sj::dom::element_type::BOOL:
    case sj::dom::element_type::NULL_VALUE:
      return Status(Error::SimdError, "Not implemented.");
  }
  return Status::OK();
}

auto ArrowDOMWalker::AppendObjectAsRecord(const sj::dom::object& object,
                                          arrow::RecordBatchBuilder* batch_builder)
    -> Status {
  if (object.size() != batch_builder->schema()->num_fields()) {
    // Not present keys could also be interpreted as null values?
    return Status(Error::SimdError,
                  "JSON Object with " + std::to_string(object.size()) +
                      " members does not match expected number of Arrow fields " +
                      std::to_string(batch_builder->schema()->num_fields()));
  }

  size_t i = 0;
  for (const auto& member : object) {
    BOLSON_ROE(AppendElement(member.value, batch_builder->schema()->field(i),
                             batch_builder->GetField(i)));
    i++;
  }
  return Status::OK();
}

auto ArrowDOMWalker::Make(const std::shared_ptr<arrow::Schema>& schema,
                          std::shared_ptr<ArrowDOMWalker>* out) -> Status {
  auto result = std::shared_ptr<ArrowDOMWalker>(new ArrowDOMWalker());
  ARROW_ROE(arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(),
                                            &result->batch_builder));
  *out = result;
  return Status::OK();
}

}  // namespace bolson::parse
