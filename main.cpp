#include "proto.out/person.pb.h"
#include "proto.out/persons.pb.h"
#include "proto_db_convertor.hpp"
#include <iostream>
#include <memory>
#include <sqlite3.h>

int main() {
  PROTO::Person msg;
  msg.set_name("test");
  msg.set_age(3);

  auto sqLite = std::make_shared<SafeSQLite>("safe.sqlite");
  ProtoDBConvertor<PROTO::Person> personConv(sqLite, {}, true);
  personConv.create_table();
  personConv.write(msg);
  personConv.write({msg, msg, msg});
  auto res = personConv.read();
  for (auto &r : res) {
    std::cout << r.ShortDebugString() << std::endl;
  }


  PROTO::Persons ps;
  ps.set_gourp("test");
  ps.set_count(5);
  ps.add_people()->CopyFrom(msg);
  ps.add_people()->CopyFrom(msg);

  ProtoDBConvertor<PROTO::Persons> personsConv(sqLite, {"people"}, true);
  personsConv.write(ps);
  auto pses = personsConv.read();
  for (auto &r : pses) {
    std::cout << r.ShortDebugString() << std::endl;
  }

  auto pses2 = personsConv.read("select * from Persons where count = 5;");
  for (auto &r : pses) {
    std::cout << r.ShortDebugString() << std::endl;
  }

  return 0;
}
