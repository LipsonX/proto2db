// =================================
// Created by Lipson on 24-4-1.
// Email to LipsonChan@yahoo.com
// Copyright (c) 2024 Lipson. All rights reserved.
// Version 1.0
// =================================

#ifndef PROTODB_PROTO_DB_CONVERTOR_HPP
#define PROTODB_PROTO_DB_CONVERTOR_HPP

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <sqlite3.h>
#include <string>
#include <unordered_set>
#include <vector>

namespace proto_mapper {

class SafeSQLite {
public:
  SafeSQLite(const std::string &filename) {
    int rc = sqlite3_open(filename.c_str(), &db_);
    if (rc != SQLITE_OK) {
      throw std::runtime_error("Failed to open database");
    }
  }

  ~SafeSQLite() { sqlite3_close(db_); }

  sqlite3 *get() const { return db_; }
  std::mutex &mutex() { return mutex_; }

  void execute(const std::string &sql = "") {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!sql.empty()) {
      sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, nullptr);
    }
  }

private:
  sqlite3 *db_;
  std::mutex mutex_;
};

template <typename T>
bool DEFAULT_SQLITE_BIND(sqlite3_stmt *, int, const std::string &, const T &) {
  return false;
}

// template <typename T, std::function<bool(sqlite3_stmt *, int, const std::string &, const T &)> ConvF = DEFAULT_CONVERT> template <typename T, typename ConvF>
template <typename T, bool BindF(sqlite3_stmt *, int, const std::string &,
                                 const T &) = DEFAULT_SQLITE_BIND>
class ProtoDBConvertor {
public:
  explicit ProtoDBConvertor(
      const std::shared_ptr<SafeSQLite> &db,
      const std::string &primary_key = "",
      const std::unordered_set<std::string> &skip_names = {},
      bool need_create_table = false)
      : db_(db), primary_key_(primary_key) {
    std::unique_lock<std::mutex> lock(db_->mutex());

    const google::protobuf::Descriptor *descriptor = T::descriptor();
    table_name_ = descriptor->name();

    for (int i = 0; i < descriptor->field_count(); ++i) {
      const auto &field = *descriptor->field(i);
      std::string field_name = field.name();
      if (skip_names.find(field_name) != skip_names.end()) {
        continue;
      }
      member_names_.push_back(field_name);
    }

    if (need_create_table) {
      create_table_();
    }

    std::string insert_sql;
    if (primary_key_.empty()) {
      insert_sql = "INSERT INTO " + table_name_ + " (";
    } else {
      insert_sql = "REPLACE INTO " + table_name_ + " (";
    }

    for (const auto &field_name : member_names_) {
      insert_sql += field_name + ",";
    }
    insert_sql.pop_back();
    insert_sql += ") VALUES (";
    for (size_t i = 0; i < member_names_.size(); ++i) {
      insert_sql += "?,";
    }
    insert_sql.pop_back();
    insert_sql += ");";

    if (!primary_key_.empty()) {
      auto sql = "DELETE FROM " + T::descriptor()->name() + " WHERE " +
                 primary_key_ + " = ?";
      if (sqlite3_prepare_v2(db_->get(), sql.c_str(), -1, &delete_stmt_,
                             nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare statement");
      }
    }

    if (sqlite3_prepare_v2(db_->get(), insert_sql.c_str(), -1, &insert_stmt_,
                           nullptr) != SQLITE_OK) {
      throw std::runtime_error("Failed to prepare statement");
    }
  }

  ~ProtoDBConvertor() {
    sqlite3_finalize(insert_stmt_);
    sqlite3_finalize(delete_stmt_);
  }

  bool create_table() {
    std::unique_lock<std::mutex> lock(db_->mutex());
    return create_table_();
  }

  bool create_table_() {
    std::string create_sql = "CREATE TABLE IF NOT EXISTS " + table_name_ + " (";
    for (const auto &memberName : member_names_) {
      const auto &field = *T::descriptor()->FindFieldByName(memberName);
      std::string field_name = field.name();
      std::string field_type;
      switch (field.type()) {
      case google::protobuf::FieldDescriptor::TYPE_INT32:
      case google::protobuf::FieldDescriptor::TYPE_INT64:
      case google::protobuf::FieldDescriptor::TYPE_UINT32:
      case google::protobuf::FieldDescriptor::TYPE_UINT64:
      case google::protobuf::FieldDescriptor::TYPE_SINT32:
      case google::protobuf::FieldDescriptor::TYPE_SINT64:
      case google::protobuf::FieldDescriptor::TYPE_FIXED32:
      case google::protobuf::FieldDescriptor::TYPE_FIXED64:
      case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
      case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
      case google::protobuf::FieldDescriptor::TYPE_ENUM:
        field_type = "INTEGER";
        break;
      case google::protobuf::FieldDescriptor::TYPE_FLOAT:
      case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
        field_type = "REAL";
        break;
      case google::protobuf::FieldDescriptor::TYPE_BOOL:
        field_type = "BOOLEAN";
        break;
      case google::protobuf::FieldDescriptor::TYPE_STRING:
        field_type = "TEXT";
        break;
      default:
        field_type = "BLOB";
        break;
      }
      if (primary_key_ == field_name) {
        create_sql += field_name + " " + field_type +
                      " PRIMARY KEY NOT NULL ON CONFLICT REPLACE,";
      } else {
        create_sql += field_name + " " + field_type + ",";
      }
    }
    create_sql.pop_back();
    create_sql += ");";

    char *errmsg;
    if (sqlite3_exec(db_->get(), create_sql.c_str(), nullptr, nullptr,
                     &errmsg) != SQLITE_OK) {
      std::cerr << "Failed to create table: " << errmsg;
      return false;
    }

    return true;
  }

  void write(const T &obj) {
    std::unique_lock<std::mutex> lock(db_->mutex());
    write_(obj);
  }

  void write(const std::vector<T> &obj) {
    std::unique_lock<std::mutex> lock(db_->mutex());
    for (auto &o : obj) {
      write_(o);
    }
  }

  void convert_filed_(sqlite3_stmt *stmt, int index, const std::string &name,
                      const T &obj) {
    if (!BindF(stmt, index, name, obj)) {
      const auto &field = *T::descriptor()->FindFieldByName(name);
      switch (field.type()) {
      case google::protobuf::FieldDescriptor::TYPE_INT32:
        sqlite3_bind_int(stmt, index,
                         obj.GetReflection()->GetInt32(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_INT64:
        sqlite3_bind_int64(stmt, index,
                           obj.GetReflection()->GetInt64(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_UINT32:
        sqlite3_bind_int(stmt, index,
                         obj.GetReflection()->GetUInt32(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_UINT64:
        sqlite3_bind_int64(stmt, index,
                           obj.GetReflection()->GetUInt64(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_FIXED32:
        sqlite3_bind_int(stmt, index,
                         obj.GetReflection()->GetUInt32(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_FIXED64:
        sqlite3_bind_int64(stmt, index,
                           obj.GetReflection()->GetUInt64(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
        sqlite3_bind_int(stmt, index,
                         obj.GetReflection()->GetInt32(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
        sqlite3_bind_int64(stmt, index,
                           obj.GetReflection()->GetInt64(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_FLOAT:
        sqlite3_bind_double(stmt, index,
                            obj.GetReflection()->GetFloat(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
        sqlite3_bind_double(stmt, index,
                            obj.GetReflection()->GetDouble(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_BOOL:
        sqlite3_bind_int(stmt, index,
                         obj.GetReflection()->GetBool(obj, &field));
        break;
      case google::protobuf::FieldDescriptor::TYPE_STRING:
        sqlite3_bind_text(stmt, index,
                          obj.GetReflection()->GetString(obj, &field).c_str(),
                          -1, SQLITE_TRANSIENT);
        break;
      default: // handle bytes
        throw std::runtime_error("Unsupported field type: " + field.name());
      }
    }
  }

  void write_(const T &obj) {
    int index = 1;
    for (const auto &name : member_names_) {
      convert_filed_(insert_stmt_, index, name, obj);
      ++index;
    }
    sqlite3_step(insert_stmt_);
    sqlite3_reset(insert_stmt_);
  }

  std::vector<T>
  read(const std::string &sql = "select * from " +
                                std::string(T::descriptor()->name())) {
    const google::protobuf::Descriptor *descriptor = T::descriptor();

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db_->get(), sql.c_str(), -1, &stmt, nullptr) !=
        SQLITE_OK) {
      throw std::runtime_error("Error querying database: " +
                               std::string(sqlite3_errmsg(db_->get())));
    }

    std::vector<T> result;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      T obj;
      const google::protobuf::Reflection *reflection = obj.GetReflection();
      for (int i = 0; i < sqlite3_column_count(stmt); ++i) {
        const char *name = sqlite3_column_name(stmt, i);
        const google::protobuf::FieldDescriptor *field =
            descriptor->FindFieldByName(name);
        if (field == nullptr) {
          throw std::runtime_error("Unknown field name: " + std::string(name));
        }
        switch (field->cpp_type()) {
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT32:
          reflection->SetInt32(&obj, field, sqlite3_column_int(stmt, i));
          break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_INT64:
          reflection->SetInt64(&obj, field, sqlite3_column_int64(stmt, i));
          break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT32:
          reflection->SetUInt32(&obj, field, sqlite3_column_int(stmt, i));
          break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_UINT64:
          reflection->SetUInt64(&obj, field, sqlite3_column_int64(stmt, i));
          break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_FLOAT:
          reflection->SetFloat(&obj, field, sqlite3_column_double(stmt, i));
          break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_DOUBLE:
          reflection->SetDouble(&obj, field, sqlite3_column_double(stmt, i));
          break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_BOOL:
          reflection->SetBool(&obj, field, sqlite3_column_int(stmt, i) != 0);
          break;
        case google::protobuf::FieldDescriptor::CppType::CPPTYPE_STRING:
          reflection->SetString(
              &obj, field,
              reinterpret_cast<const char *>(sqlite3_column_text(stmt, i)));
          break;
        default:
          throw std::runtime_error("Unsupported field type: " + field->name());
        }
      }
      result.push_back(obj);
    }

    sqlite3_finalize(stmt);

    return result;
  }

  void delete_obj(const T &obj) {
    if (primary_key_.empty()) {
      return;
    }

    convert_filed_(delete_stmt_, 1, primary_key_, obj);
    sqlite3_step(delete_stmt_);
    sqlite3_reset(delete_stmt_);
  }

private:
  sqlite3_stmt *insert_stmt_ = nullptr;
  sqlite3_stmt *delete_stmt_ = nullptr;
  std::string table_name_;
  std::string primary_key_;
  std::vector<std::string> member_names_;
  std::shared_ptr<SafeSQLite> db_;
};

}
#endif // PROTODB_PROTO_DB_CONVERTOR_HPP
