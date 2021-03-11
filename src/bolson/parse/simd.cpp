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
      BOLSON_ROE(walker->Append(obj));
    }

    std::shared_ptr<arrow::RecordBatch> batch_out;
    BOLSON_ROE(walker->Finish(&batch_out));

    out->push_back(ParsedBatch(batch_out, buf->range()));
  }

  return Status::OK();
}

auto SimdParser::Make(const std::shared_ptr<arrow::Schema>& schema,
                      std::shared_ptr<SimdParser>* out) -> Status {
  auto result = std::shared_ptr<SimdParser>(new SimdParser());
  BOLSON_ROE(DOMVisitor::Make(schema, &result->walker));
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

auto DOMVisitor::AppendArrayAsList(const sj::dom::array& array,
                                   const ArrowField& item_field,
                                   arrow::ListBuilder* list_builder) -> Status {
  assert(list_builder != nullptr);

  ARROW_ROE(list_builder->Append());
  ARROW_ROE(list_builder->value_builder()->Reserve(array.size()));

  if (arrow::is_primitive(item_field->type()->id())) {
    // Lists of primitives:
    switch (item_field->type()->id()) {
      default:
        return Status(Error::SimdError,
                      "Appending primitives of type " + item_field->type()->name() +
                          " to list from JSON array is not supported or implemented.");
      case arrow::Type::UINT64: {
        auto* bld = dynamic_cast<arrow::UInt64Builder*>(list_builder->value_builder());
        for (const sj::dom::element& elem : array) {
          bld->UnsafeAppend(elem.get_uint64());
        }
      } break;
      case arrow::Type::INT64: {
        auto* bld = dynamic_cast<arrow::Int64Builder*>(list_builder->value_builder());
        for (const sj::dom::element& elem : array) {
          bld->UnsafeAppend(elem.get_int64());
        }
      } break;
    }
  } else {
    // Lists of anything else:
    for (const sj::dom::element& elem : array) {
      BOLSON_ROE(AppendElement(elem, item_field, list_builder->value_builder()));
    }
  }
  return Status::OK();
}

auto DOMVisitor::AppendArrayAsFixedSizeList(const sj::dom::array& array,
                                            const ArrowField& item_field,
                                            arrow::FixedSizeListBuilder* fsl_builder)
    -> Status {
  assert(fsl_builder != nullptr);

  // Sanity check size.
  size_t expected_size =
      std::static_pointer_cast<arrow::FixedSizeListType>(fsl_builder->type())
          ->list_size();
  if (array.size() != expected_size) {
    return Status(Error::SimdError, "JSON array contained " +
                                        std::to_string(array.size()) +
                                        " items, but FixedSizeList requires " +
                                        std::to_string(expected_size));
  }

  ARROW_ROE(fsl_builder->Append());
  ARROW_ROE(fsl_builder->value_builder()->Reserve(array.size()));

  if (arrow::is_primitive(item_field->type()->id())) {
    // Lists of primitives:
    switch (item_field->type()->id()) {
      default:
        return Status(Error::SimdError,
                      "Appending primitives of type " + item_field->type()->name() +
                          " to list from JSON array is not supported or implemented.");
      case arrow::Type::UINT64: {
        auto* bld = dynamic_cast<arrow::UInt64Builder*>(fsl_builder->value_builder());
        for (const sj::dom::element& elem : array) {
          bld->UnsafeAppend(elem.get_uint64());
        }
      } break;
      case arrow::Type::INT64: {
        auto* bld = dynamic_cast<arrow::Int64Builder*>(fsl_builder->value_builder());
        for (const sj::dom::element& elem : array) {
          bld->UnsafeAppend(elem.get_int64());
        }
      } break;
    }
  } else {
    // Lists of anything else:
    for (const sj::dom::element& elem : array) {
      BOLSON_ROE(AppendElement(elem, item_field, fsl_builder->value_builder()));
    }
  }
  return Status::OK();
}

auto DOMVisitor::AppendObjectAsStruct(const sj::dom::object& object,
                                      const ArrowFields& expected_fields,
                                      arrow::StructBuilder* struct_builder) -> Status {
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
    BOLSON_ROE(AppendElement(elem.value_unsafe(), expected_fields[i],
                             struct_builder->child_builder(i).get()));
  }
  return Status::OK();
}

#define BOLSON_SIMD_PRIM_CASE(ARROW_TYPE, BUILDER_TYPE, C_TYPE) \
  case ARROW_TYPE: {                                            \
    ARROW_ROE(dynamic_cast<BUILDER_TYPE*>(builder)->Append(     \
        static_cast<C_TYPE>(element.get_int64())));             \
  } break;

auto DOMVisitor::AppendElement(const sj::dom::element& element,
                               const ArrowField& expected_field,
                               arrow::ArrayBuilder* builder) -> Status {
  assert(builder != nullptr);

  switch (element.type()) {
    case sj::dom::element_type::ARRAY: {
      // list or fixed size list builder
      switch (expected_field->type()->id()) {
        default:
          return TypeErrorStatus(element.type(), *expected_field->type());
        case arrow::Type::LIST: {
          auto* list_builder = dynamic_cast<arrow::ListBuilder*>(builder);
          auto item_field = expected_field->type()->field(0);
          BOLSON_ROE(AppendArrayAsList(element.get_array(), item_field, list_builder));
        } break;
        case arrow::Type::FIXED_SIZE_LIST: {
          auto* fsl_builder = dynamic_cast<arrow::FixedSizeListBuilder*>(builder);
          auto item_field = expected_field->type()->field(0);
          BOLSON_ROE(
              AppendArrayAsFixedSizeList(element.get_array(), item_field, fsl_builder));
        } break;
      }
    } break;
    case sj::dom::element_type::OBJECT: {
      auto* struct_builder = dynamic_cast<arrow::StructBuilder*>(builder);
      BOLSON_ROE(AppendObjectAsStruct(element.get_object(),
                                      expected_field->type()->fields(), struct_builder));
    } break;
    case sj::dom::element_type::INT64: {
      switch (expected_field->type()->id()) {
        BOLSON_SIMD_PRIM_CASE(arrow::Type::UINT8, arrow::UInt8Builder, uint8_t);
        BOLSON_SIMD_PRIM_CASE(arrow::Type::UINT16, arrow::UInt16Builder, uint16_t);
        BOLSON_SIMD_PRIM_CASE(arrow::Type::UINT32, arrow::UInt32Builder, uint32_t);
        BOLSON_SIMD_PRIM_CASE(arrow::Type::UINT64, arrow::UInt64Builder, uint64_t);
        BOLSON_SIMD_PRIM_CASE(arrow::Type::INT8, arrow::Int8Builder, int8_t);
        BOLSON_SIMD_PRIM_CASE(arrow::Type::INT16, arrow::Int16Builder, int16_t);
        BOLSON_SIMD_PRIM_CASE(arrow::Type::INT32, arrow::Int32Builder, int32_t);
        BOLSON_SIMD_PRIM_CASE(arrow::Type::INT64, arrow::Int64Builder, int64_t);
        default:
          return TypeErrorStatus(element.type(), *expected_field->type());
      }
    } break;
    case sj::dom::element_type::UINT64: {
      // From simdjson docs: uint64_t: any integer that fits in uint64_t but *not* int64_t
      switch (expected_field->type()->id()) {
        default:
          return TypeErrorStatus(element.type(), *expected_field->type());
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
      switch (expected_field->type()->id()) {
        default:
          return TypeErrorStatus(element.type(), *expected_field->type());
        case arrow::Type::BOOL:
          ARROW_ROE(
              dynamic_cast<arrow::BooleanBuilder*>(builder)->Append(element.get_bool()));
          break;
      }
      break;
    case sj::dom::element_type::NULL_VALUE:
      return Status(Error::SimdError, "Null value not implemented.");
  }
  return Status::OK();
}  // namespace bolson::parse

auto DOMVisitor::AppendObjectAsRecord(const sj::dom::object& object,
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
    auto status = AppendElement(member.value, batch_builder->schema()->field(i),
                                batch_builder->GetField(i));
    if (!status.ok()) {
      std::stringstream ss;
      ss << "Encountered problem: " << status.msg() << std::endl;
      ss << "  when appending member: " << member.key << std::endl;
      ss << "  from object: " << std::endl << object << std::endl;
      return Status(Error::SimdError, ss.str());
    }
    i++;
  }
  return Status::OK();
}

auto DOMVisitor::Make(const std::shared_ptr<arrow::Schema>& schema,
                      std::shared_ptr<DOMVisitor>* out) -> Status {
  auto result = std::shared_ptr<DOMVisitor>(new DOMVisitor());
  ARROW_ROE(arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(),
                                            &result->batch_builder));
  *out = result;
  return Status::OK();
}

auto DOMSequenceVisitor::Analyze(const arrow::Schema& schema) -> Status {
  for (const auto& field : schema.fields()) {
    VisitField(*field);
  }
  return Status::OK();
}

auto DOMSequenceVisitor::VisitField(const arrow::Field& field) -> Status {
  path_.emplace_back(field.name());
  VisitType(*field.type());
  return Status::OK();
}

auto DOMSequenceVisitor::VisitType(const arrow::DataType& type) -> Status {
  ARROW_ROE(type.Accept(this));
  return Status::OK();
}

#define BOLSON_SIMD_PRIM_VISIT_IMPL(ARROW_TYPE)                                        \
  auto DOMSequenceVisitor::Visit(const arrow::ARROW_TYPE##Type& type)->arrow::Status { \
    nodes_.emplace_back(join(path_), std::make_shared<arrow::ARROW_TYPE##Builder>());  \
    path_.clear();                                                                     \
    return arrow::Status::OK();                                                        \
  }

BOLSON_SIMD_PRIM_VISIT_IMPL(Boolean);
BOLSON_SIMD_PRIM_VISIT_IMPL(Int8);
BOLSON_SIMD_PRIM_VISIT_IMPL(Int16);
BOLSON_SIMD_PRIM_VISIT_IMPL(Int32);
BOLSON_SIMD_PRIM_VISIT_IMPL(Int64);
BOLSON_SIMD_PRIM_VISIT_IMPL(UInt8);
BOLSON_SIMD_PRIM_VISIT_IMPL(UInt16);
BOLSON_SIMD_PRIM_VISIT_IMPL(UInt32);
BOLSON_SIMD_PRIM_VISIT_IMPL(UInt64);
BOLSON_SIMD_PRIM_VISIT_IMPL(Double);
BOLSON_SIMD_PRIM_VISIT_IMPL(String);

auto DOMSequenceVisitor::Visit(const arrow::ListType& type) -> arrow::Status {
  // Push back the node for the list builder.
  // We can't construct the list builder yet, because we need the values builder first.
  auto& this_node_ref = nodes_.emplace_back(join(path_));

  // Get the iterator to the current node.
  auto iter = nodes_.end()--;

  // Walk down the tree.
  VisitField(*type.value_field());

  // Move to the child node that was appended.
  // Construct the list builder, taking the child node builder.
  this_node_ref.builder = std::make_shared<arrow::ListBuilder>(
      arrow::default_memory_pool(), (++iter)->builder);

  return arrow::Status::OK();
}

auto DOMSequenceVisitor::Visit(const arrow::StructType& type) -> arrow::Status {
  // Push back the node for the struct builder.
  // We can't construct the struct builder yet, because we need the values builders first.
  auto& this_node_ref = nodes_.emplace_back(join(path_));
  // Get the iterator to the current node.
  auto iter = --nodes_.end();
  // Make a copy of the current path, so we can restore it after visiting every field.
  auto this_path = path_;

  std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders;

  for (const auto& field : type.fields()) {
    // Visit the child field.
    VisitField(*field);
    // Move to the child node.
    // Push back a copy of the pointer to the child builder.
    child_builders.push_back((++iter)->builder);
    // Set the iterator to the last node.
    iter = --nodes_.end();
    // Restore the current path.
    path_ = this_path;
  }

  // Create the struct builder and place it on the current node.
  this_node_ref.builder = std::make_shared<arrow::StructBuilder>(
      arrow::struct_(type.fields()), arrow::default_memory_pool(), child_builders);

  return arrow::Status::OK();
}

auto DOMSequenceVisitor::ToString() const -> std::string {
  std::ostringstream ss;
  size_t longest = 0;
  for (const auto& node : nodes_) {
    if (node.name.length() > longest) {
      longest = node.name.length();
    }
  }
  for (const auto& node : nodes_) {
    ss << std::left << std::setw(longest) << node.name << " : "
       << (node.builder == nullptr ? "nullptr" : node.builder->type()->name())
       << std::endl;
  }
  return ss.str();
}

}  // namespace bolson::parse
