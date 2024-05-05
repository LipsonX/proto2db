// =================================
// Created by Lipson on 24-4-20.
// Email to LipsonChan@yahoo.com
// Copyright (c) 2024 Lipson. All rights reserved.
// Version 1.0
// =================================

#ifndef PROTO2DB_PROTO_C_MAPPER_H
#define PROTO2DB_PROTO_C_MAPPER_H

#include <google/protobuf/message.h>
#include <string>

namespace proto_mapper {

using namespace google::protobuf;

template <typename P, typename C> void DEFAULT_P2C_CONVERT(const P &, C &) {}
template <typename P, typename C> void DEFAULT_C2P_CONVERT(const C &, P &) {}

template <typename P, typename C, void P2C(const P &, C &),
          void C2P(const C &, P &), typename... Fields>
class ProtoCStructMapper;

template <typename P, typename C, void P2C(const P &, C &),
          void C2P(const C &, P &)>
class ProtoCStructMapper<P, C, P2C, C2P> {
public:
  inline static P toP(const C &c) {
    P p;
    convert(c, p);
    return p;
  };

  inline static C toC(const P &p) {
    C c;
    convert(p, c);
    return c;
  };

  inline static void convert(const P &p, C &c) { P2C(p, c); };
  inline static void convert(const C &c, P &p) { C2P(c, p); };
};

template <typename P, typename C, void P2C(const P &, C &),
          void C2P(const C &, P &), typename Field, typename... Fields>
class ProtoCStructMapper<P, C, P2C, C2P, Field, Fields...>
/*: public ProtoBufCStructMapping<P, C, P2C, C2P, Fields...>*/ {
public:
  inline static P toP(const C &c) {
    P p;
    convert(c, p);
    return p;
  };

  inline static C toC(const P &p) {
    C c;
    convert(p, c);
    return c;
  };

  inline static void convert(const P &p, C &c) {
    auto desc = p.GetDescriptor()->FindFieldByName(Field::name);
    if (desc != nullptr) {
      if constexpr (std::is_same_v<typename Field::type, int>) {
        int value = p.GetReflection()->GetInt32(p, desc);
        *(int *)((char *)&c + Field::offset) = value;
      } else if constexpr (std::is_same_v<typename Field::type,
                                          char *>) { // be careful of memory
                                                     // overflow
        string value = p.GetReflection()->GetString(p, desc);
        memcpy((char *)&c + Field::offset, value.c_str(), value.size());
      } else if constexpr (std::is_same_v<typename Field::type, bool>) {
        bool value = p.GetReflection()->GetBool(p, desc);
        *(bool *)((char *)&c + Field::offset) = value;
      } else if constexpr (std::is_same_v<typename Field::type, std::string>) {
        std::string value = p.GetReflection()->GetString(p, desc);
        *(std::string *)((char *)&c + Field::offset) = value;
      } else if constexpr (std::is_same_v<typename Field::type, double>) {
        double value = p.GetReflection()->GetDouble(p, desc);
        *(double *)((char *)&c + Field::offset) = value;
      } else if constexpr (std::is_same_v<
                               typename Field::type,
                               std::vector<typename Field::type::value_type>>) {
        if constexpr (std::is_same_v<std::string,
                                     typename Field::type::value_type>) {
          const auto &fields =
              p.GetReflection()
                  ->template GetRepeatedPtrField<
                      typename Field::type::value_type>(p, desc);
          std::vector<typename Field::type::value_type> &data =
              *(std::vector<typename Field::type::value_type> *)((char *)&c +
                                                                 Field::offset);
          data.assign(fields.begin(), fields.end());
        } else {
          const auto &fields =
              p.GetReflection()
                  ->template GetRepeatedField<typename Field::type::value_type>(
                      p, desc);
          std::vector<typename Field::type::value_type> &data =
              *(std::vector<typename Field::type::value_type> *)((char *)&c +
                                                                 Field::offset);
          data.assign(fields.begin(), fields.end());
        }
      }
    }
    ProtoCStructMapper<P, C, P2C, C2P, Fields...>::convert(p, c);
  }

  inline static void convert(const C &c, P &p) {
    auto desc = p.GetDescriptor()->FindFieldByName(Field::name);
    std::string name = desc->name();
    if (desc != nullptr) {
      if constexpr (std::is_same_v<typename Field::type, int>) {
        int value = *(int *)((char *)&c + Field::offset);
        p.GetReflection()->SetInt32(&p, desc, value);
      } else if constexpr (std::is_same_v<typename Field::type, bool>) {
        bool value = *(bool *)((char *)&c + Field::offset);
        p.GetReflection()->SetBool(&p, desc, value);
      } else if constexpr (std::is_same_v<typename Field::type, char *>) {
        p.GetReflection()->SetString(&p, desc, ((char *)&c + Field::offset));
      } else if constexpr (std::is_same_v<typename Field::type, double>) {
        double value = *(double *)((char *)&c + Field::offset);
        p.GetReflection()->SetDouble(&p, desc, value);
      } else if constexpr (std::is_same_v<typename Field::type, std::string>) {
        string value = *(std::string *)((char *)&c + Field::offset);
        p.GetReflection()->SetString(&p, desc, value);
      } else if constexpr (std::is_same_v<
                               typename Field::type,
                               std::vector<typename Field::type::value_type>>) {
        if constexpr (std::is_same_v<std::string,
                                     typename Field::type::value_type>) {
          const std::vector<typename Field::type::value_type> &data =
              *(std::vector<typename Field::type::value_type> *)((char *)&c +
                                                                 Field::offset);
          auto fields = p.GetReflection()
                            ->template MutableRepeatedPtrField<
                                typename Field::type::value_type>(&p, desc);
          fields->Clear();
          for (const auto &element : data) {
            *fields->Add() = element;
          }
        } else {
          const std::vector<typename Field::type::value_type> &data =
              *(std::vector<typename Field::type::value_type> *)((char *)&c +
                                                                 Field::offset);
          auto fields = p.GetReflection()
                            ->template MutableRepeatedField<
                                typename Field::type::value_type>(&p, desc);
          fields->Clear();
          for (const auto &element : data) {
            if constexpr (std::is_base_of<
                              google::protobuf::Message,
                              typename Field::type::value_type>::value) {
              fields->Add()->CopyFrom(element);
            } else {
              *fields->Add() = element;
            }
          }
        }
      }
    }
    ProtoCStructMapper<P, C, P2C, C2P, Fields...>::convert(c, p);
  }
};

template <char... chars> using tstring = std::integer_sequence<char, chars...>;

template <typename T, T... chars>
constexpr tstring<chars...> operator""_tstr() {
  return {};
}

template <typename T, size_t Offset, typename> struct MemberX;

template <typename T, size_t Offset, char... elements>
struct MemberX<T, Offset, tstring<elements...>> {
  static constexpr char name[] = {elements..., '\0'};
  static constexpr size_t offset = Offset;
  using type = T;
};
} // namespace proto_mapper

#define PROTO_MEMBER(P, t, name)                                               \
  proto_mapper::MemberX<t, offsetof(P, name), decltype(#name##_tstr)>

#define PROTO_MAPER(P, C, P2C, C2P, ...)                                       \
  typedef proto_mapper::ProtoCStructMapper<P, C, P2C, C2P, __VA_ARGS__>        \
      C##PBMapper;

#define PROTO_MAPER_NO_DEFAULT(P, C, ...)                                      \
  typedef proto_mapper::ProtoBufCStructMapping<                                \
      P, C, proto_mapper::DEFAULT_P2C_CONVERT,                                 \
      proto_mapper::DEFAULT_C2P_CONVERT, __VA_ARGS__>                          \
      C##PBMapper;

#endif // PROTO2DB_PROTO_C_MAPPER_H
