// =================================
// Created by Lipson on 24-4-1.
// Email to LipsonChan@yahoo.com
// Copyright (c) 2024 Lipson. All rights reserved.
// Version 1.0
// =================================

#ifndef PROTODB_PROTO_DB_CONVERTOR_HPP
#define PROTODB_PROTO_DB_CONVERTOR_HPP

#include <google/protobuf/descriptor.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <sqlite3.h>
#include <string>
#include <unordered_set>
#include <vector>

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

  void excute(const std::string& sql="") {
    if (!sql.empty()) {
      sqlite3_exec(db_, sql.c_str(), 0, 0, 0);
    }
  }

private:
  sqlite3 *db_;
  std::mutex mutex_;
};

template <typename T>
bool DEFAULT_CONVERT(sqlite3_stmt *, int, const std::string &, const T &) {
  return false;
}

template <typename T, bool ConvF(sqlite3_stmt *, int, const std::string &,
                                 const T &) = DEFAULT_CONVERT>
class ProtoDBConvertor {
public:
  ProtoDBConvertor(const std::shared_ptr<SafeSQLite> db,
                         const std::unordered_set<std::string> skip_names = {},
                         bool need_create_table = false)
      : db_(db) {
    const google::protobuf::Descriptor *descriptor = T::descriptor();
    tableName_ = descriptor->name();

    for (int i = 0; i < descriptor->field_count(); ++i) {
      const auto &field = *descriptor->field(i);
      std::string fieldName = field.name();
      if (skip_names.find(fieldName) != skip_names.end()) {
        continue;
      }

      memberNames_.push_back(fieldName);
    }

    if (need_create_table) {
      create_table();
    }

    std::string insertSql = "INSERT INTO " + tableName_ + " (";
    for (const auto &fieldName : memberNames_) {
      insertSql += fieldName + ",";
    }
    insertSql.pop_back();
    insertSql += ") VALUES (";
    for (size_t i = 0; i < memberNames_.size(); ++i) {
      insertSql += "?,";
    }
    insertSql.pop_back();
    insertSql += ");";

    std::unique_lock<std::mutex> lock(db_->mutex());
    if (sqlite3_prepare_v2(db_->get(), insertSql.c_str(), -1, &insert_stmt_,
                           nullptr) != SQLITE_OK) {
      throw std::runtime_error("Failed to prepare statement");
    }
  }

  ~ProtoDBConvertor() { sqlite3_finalize(insert_stmt_); }

  bool create_table() {
    std::unique_lock<std::mutex> lock(db_->mutex());

    std::string createTableSql =
        "CREATE TABLE IF NOT EXISTS " + tableName_ + " (";
    for (const auto &memberName : memberNames_) {
      const auto &field = *T::descriptor()->FindFieldByName(memberName);
      std::string fieldName = field.name();
      std::string fieldType;
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
        fieldType = "INTEGER";
        break;
      case google::protobuf::FieldDescriptor::TYPE_FLOAT:
      case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
        fieldType = "REAL";
        break;
      case google::protobuf::FieldDescriptor::TYPE_BOOL:
        fieldType = "BOOLEAN";
        break;
      case google::protobuf::FieldDescriptor::TYPE_STRING:
        fieldType = "TEXT";
        break;
      default:
        fieldType = "BLOB";
        break;
      }
      createTableSql += fieldName + " " + fieldType + ",";
    }
    createTableSql.pop_back();
    createTableSql += ");";

    char *errmsg;
    if (sqlite3_exec(db_->get(), createTableSql.c_str(), nullptr, nullptr, &errmsg) !=
        SQLITE_OK) {
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

  void write_(const T &obj) {
    int index = 1;
    for (const auto &memberName : memberNames_) {
      const auto &field = *T::descriptor()->FindFieldByName(memberName);
      if (!ConvF(insert_stmt_, index, field.name(), obj)) {
        switch (field.type()) {
        case google::protobuf::FieldDescriptor::TYPE_INT32:
          sqlite3_bind_int(insert_stmt_, index,
                           obj.GetReflection()->GetInt32(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_INT64:
          sqlite3_bind_int64(insert_stmt_, index,
                             obj.GetReflection()->GetInt64(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_UINT32:
          sqlite3_bind_int(insert_stmt_, index,
                           obj.GetReflection()->GetUInt32(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_UINT64:
          sqlite3_bind_int64(insert_stmt_, index,
                             obj.GetReflection()->GetUInt64(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_FIXED32:
          sqlite3_bind_int(insert_stmt_, index,
                           obj.GetReflection()->GetUInt32(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_FIXED64:
          sqlite3_bind_int64(insert_stmt_, index,
                             obj.GetReflection()->GetUInt64(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
          sqlite3_bind_int(insert_stmt_, index,
                           obj.GetReflection()->GetInt32(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
          sqlite3_bind_int64(insert_stmt_, index,
                             obj.GetReflection()->GetInt64(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_FLOAT:
          sqlite3_bind_double(insert_stmt_, index,
                              obj.GetReflection()->GetFloat(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
          sqlite3_bind_double(insert_stmt_, index,
                              obj.GetReflection()->GetDouble(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_BOOL:
          sqlite3_bind_int(insert_stmt_, index,
                           obj.GetReflection()->GetBool(obj, &field));
          break;
        case google::protobuf::FieldDescriptor::TYPE_STRING:
          sqlite3_bind_text(insert_stmt_, index,
                            obj.GetReflection()->GetString(obj, &field).c_str(),
                            -1, SQLITE_TRANSIENT);
          break;
        default: // handle bytes
          throw std::runtime_error("Unsupported field type: " + field.name());
        }
      }

      ++index;
    }
    sqlite3_step(insert_stmt_);
    sqlite3_reset(insert_stmt_);
  }

  std::vector<T> read(const std::string& sql="select * from " + std::string(T::descriptor()->name())) {
    const google::protobuf::Descriptor *descriptor = T::descriptor();

    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db_->get(), sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
      throw std::runtime_error("Error querying database: " + std::string(sqlite3_errmsg(db_->get())));
    }

    std::vector<T> result;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      T obj;
      const google::protobuf::Reflection *reflection = obj.GetReflection();
      for (int i = 0; i < sqlite3_column_count(stmt); ++i) {
        const char *name = sqlite3_column_name(stmt, i);
        const google::protobuf::FieldDescriptor *field = descriptor->FindFieldByName(name);
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
          reflection->SetString(&obj, field, reinterpret_cast<const char *>(sqlite3_column_text(stmt, i)));
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

private:
  sqlite3_stmt *insert_stmt_;
  std::string tableName_;
  std::vector<std::string> memberNames_;
  std::shared_ptr<SafeSQLite> db_;
};

#endif // PROTODB_PROTO_DB_CONVERTOR_HPP
