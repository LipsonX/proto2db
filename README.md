## A helpful C++ tool for writing/reading protobuf to/from db

#### require
- C++17
- protobuf
- sqlite3

for debian:
```
apt install protobuf-compiler libprotobuf-dev sqlite3 libsqlite3-dev
```

#### how to use
```
# proto
message Person {
    required string name = 1;
    required int32 age = 2;
}

# demo
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
```