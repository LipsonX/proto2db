#include "person.pb.h"
#include "persons.pb.h"
#include "proto_db_convertor.hpp"
#include <iostream>
#include <memory>
#include <sqlite3.h>
#include "proto_c_mapper.hpp"


using namespace proto_mapper;

struct PersonS {
  int age;
  char name[20];
  std::string temp;
  std::vector<int> numbers;
  std::vector<std::string> alas;
};

PROTO_MAPER(PROTO::Person, PersonS, DEFAULT_P2C_CONVERT,
            DEFAULT_C2P_CONVERT,
            PROTO_MEMBER(PersonS, char *, name),
            PROTO_MEMBER(PersonS, std::string, temp),
            PROTO_MEMBER(PersonS, std::vector<int>, numbers),
            PROTO_MEMBER(PersonS, std::vector<std::string>, alas),
            PROTO_MEMBER(PersonS, int, age));

int main() {
  PROTO::Person msg;
  msg.set_name("test");
  msg.set_age(3);

  auto sqLite = std::make_shared<SafeSQLite>("safe.sqlite");
  ProtoDBConvertor<PROTO::Person> personConv(
      sqLite, "name", {"numbers", "alas"}, true);
  personConv.create_table();
  personConv.write(msg);
  personConv.write({msg, msg, msg});
  auto res = personConv.read();
  for (auto &r : res) {
    std::cout << r.ShortDebugString() << std::endl;
  }


  PROTO::Persons ps;
  ps.set_group_id("test");
  ps.set_count(5);
  ps.add_people()->CopyFrom(msg);
  ps.add_people()->CopyFrom(msg);

  ProtoDBConvertor<PROTO::Persons> personsConv(sqLite, "group_id", {"people"},
                                               true);
  personsConv.write(ps);
  auto pses = personsConv.read();
  for (auto &r : pses) {
    std::cout << r.ShortDebugString() << std::endl;
  }

  auto pses2 = personsConv.read("select * from Persons where count = 5;");
  for (auto &r : pses) {
    std::cout << r.ShortDebugString() << std::endl;
  }


  PROTO::Person person;
  person.set_age(18);
  person.set_name("Tom");
  person.add_numbers(1);
  person.add_numbers(2);
  person.add_alas("fuck");
  person.add_alas("fuck2");
  person.set_temp("fuck");

  auto pc = PersonSPBMapper::toC(person);
  auto person2 = PersonSPBMapper::toP(pc);

  std::cout << person2.ShortDebugString() << std::endl;

  return 0;
}
