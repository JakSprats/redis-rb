# encoding: UTF-8

require File.expand_path("./helper", File.dirname(__FILE__))

setup do
  init Redis.new(OPTIONS)
end

test "101 CREATE TABLE" do |r|
  r.changedb 12
  if "table" == r.type("new_table")
    r.drop_table("new_table");
  end
  r.create_table("new_table", "(id INT, val TEXT)")

  assert ["id | INT | INDEX: new_table:id:index [BYTES: 0]", "val | TEXT ", "INFO: KEYS: [NUM: 0 MIN: (null) MAX: (null)] BYTES: [BT-DATA: 0 BT-TOTAL: 4145 INDEX: 0]"] == r.desc("new_table")

  assert ["DROP TABLE IF EXISTS `mysql_new_table`;","CREATE TABLE `mysql_new_table` (  id INT PRIMARY KEY, val TEXT);", "LOCK TABLES `mysql_new_table` WRITE;", "UNLOCK TABLES;"] == r.dump_to_mysql("new_table", "mysql_new_table")
end

test "102 INSERT" do |r|
  r.changedb 12
  r.insert("new_table", "(1,ONE)");
  assert "ONE" == r.select("val", "new_table", "id = 1")
  assert "INFO: BYTES: [ROW: 8 BT-DATA: 16 BT-TOTAL: 4161 INDEX: 0]" == r.insert_and_return_size("new_table", "(2,TWO)");
  assert "2" == r.select("id", "new_table", "id = 2")
end

test "103 RANGE_QUERY" do |r|
  r.changedb 12
  assert ["1,ONE", "2,TWO"] == r.select("*", "new_table", "id BETWEEN 1 AND 2")
end

test "104 JOIN" do |r|
  r.changedb 12
  r.create_table("join_table", "(id INT, val TEXT)")
  assert "table" == r.type("join_table")
  r.insert("join_table", "(1,J_ONE)");
  assert "J_ONE" == r.select("val", "join_table", "id = 1")
  r.insert("join_table", "(2,J_TWO)");
  assert "2" == r.select("id", "join_table", "id = 2")
  assert ["ONE,J_ONE", "TWO,J_TWO"] == r.select("new_table.val,join_table.val", "new_table,join_table", "new_table.id = join_table.id AND new_table.id BETWEEN 1 AND 2")
  r.drop_table("join_table");
  assert "none" == r.type("join_table")
end

test "105 SCANSELECT" do |r|
  r.changedb 12
  assert ["1,ONE"] == r.scanselect("*", "new_table", "val = ONE")
end

test "106 CREATE/DROP INDEX" do |r|
  r.changedb 12
  r.create_index("new_table:val:index", "new_table", "val")
  assert "index" == r.type("new_table:val:index")

  r.drop_index("new_table:val:index")
  assert "none" == r.type("new_table:val:index")
end

test "107 UPDATE" do |r|
  r.changedb 12
  r.update("new_table", "val=two", "id = 2")
  assert "two" == r.select("val", "new_table", "id = 2")
end

test "108 DELETE" do |r|
  r.changedb 12
  r.delete("new_table", "id = 2")
  assert nil == r.select("id", "new_table", "id = 2")
end

test "109 CREATE TABLE EXTENSIONS & SELECT_STORE" do |r|
  r.changedb 12
  if "table" == r.type("z_table")
    r.drop_table("z_table");
  end
  if "table" == r.type("x_table")
    r.drop_table("x_table");
  end
  r.zadd "zset", 5, "z5"
  r.zadd "zset", 1, "z1"
  r.zadd "zset", 3, "z3"
  r.zadd "zset", 2, "z2"
  r.zadd "zset", 4, "z4"

  r.create_table_from_redis_object("z_table", "zset");
  r.create_index("z_table:zkey:index", "z_table", "zkey")
  assert ["5.000000"] == r.select("zvalue", "z_table", "zkey = z5")

  r.create_table_as("x_table", "ZRANGE", "zset", "0 1 WITHSCORES")
  assert "2" == r.select("value", "x_table", "pk = 4")

  r.select_store("zkey,zvalue", "z_table", "zkey BETWEEN z2 AND z4", "HSET", "z_hash")
  assert "4.000000" == r.hget("z_hash", "z4")
  r.del("z_hash")
  assert "none" == r.type("z_hash")

  r.drop_table("z_table"); # deletes "z_table:zkey:index" also
  assert "none" == r.type("z_table")

  r.drop_table("x_table");
  assert "none" == r.type("x_table")
end

test "110 DE/NORMALISATION" do |r|
  r.changedb 12
  if "table" == r.type("user")
    r.drop_table("user");
  end
  if "table" == r.type("user_address")
    r.drop_table("user_address");
  end
  if "table" == r.type("user_payment")
    r.drop_table("user_payment");
  end
  r.set("user:1:name", "bill");
  r.set("user:1:age", "33");
  r.set("user:1:status", "member");
  r.set("user:1:address:street", "12345 main st");
  r.set("user:1:address:city", "capitol city");
  r.set("user:1:address:zipcode", "55566");
  r.set("user:1:payment:type", "credit card");
  r.set("user:1:payment:account", "1234567890");

  r.set("user:2:name", "jane");
  r.set("user:2:age", "22");
  r.set("user:2:status", "premium");
  r.set("user:2:address:street", "345 side st");
  r.set("user:2:address:city", "capitol city");
  r.set("user:2:address:zipcode", "55566");
  r.set("user:2:payment:type", "checking");
  r.set("user:2:payment:account", "44441111");

  r.set("user:3:name", "ken");
  r.set("user:3:age", "44");
  r.set("user:3:status", "guest");
  r.set("user:3:address:street", "876 big st");
  r.set("user:3:address:city", "houston");
  r.set("user:3:address:zipcode", "87654");
  r.set("user:3:payment:type", "cash");

  r.normalize("user", "address,payment");
  assert ["1,bill,member,capitol city,12345 main st,1,55566", "2,jane,premium,capitol city,345 side st,2,55566", "3,ken,guest,houston,876 big st,3,87654"] == r.select("user.pk,user.name,user.status,user_address.city,user_address.street,user_address.pk,user_address.zipcode", "user,user_address", "user.pk=user_address.pk AND user.pk BETWEEN 1 AND 5")

  r.denormalize("user", 'user:*')
  assert({"name" => "bill", "age" => "33", "status" => "member"}) == r.hgetall("user:1")
end

test "111 FLUSHDB 12" do |r|
  r.changedb 12
  r.flushdb()
end
