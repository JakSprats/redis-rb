# encoding: UTF-8

require File.expand_path("./helper", File.dirname(__FILE__))

setup do
  init Redis.new(OPTIONS)
end

test "101 CREATE TABLE" do |r|
  r.changedb 0
  r.create_table("new_table", "(id INT, val TEXT)")
  assert "table" == r.type("new_table")
end

test "102 DESC" do |r|
  r.changedb 0
  assert ["id | INT | INDEX: new_table:id:index [BYTES: 0]", "val | TEXT ", "INFO: KEYS: [NUM: 0 MIN: (null) MAX: (null)] BYTES: [BT-DATA: 0 BT-TOTAL: 4145 INDEX: 0]"] == r.desc("new_table")
end

test "103 DUMP TO MYSQL" do |r|
  r.changedb 0
  assert ["DROP TABLE IF EXISTS `new_table`;","CREATE TABLE `new_table` (  id INT PRIMARY KEY, val TEXT);", "LOCK TABLES `new_table` WRITE;", "UNLOCK TABLES;"] == r.dump_to_mysql("new_table")
end

test "104 INSERT" do |r|
  r.changedb 0
  r.insert("new_table", "(1,ONE)");
  assert "ONE" == r.select("val", "new_table", "id = 1")
  r.insert("new_table", "(2,TWO)");
  assert "2" == r.select("id", "new_table", "id = 2")
end

test "105 RANGE_QUERY" do |r|
  r.changedb 0
  assert ["1,ONE", "2,TWO"] == r.select("*", "new_table", "id BETWEEN 1 AND 2")
end

test "106 JOIN" do |r|
  r.changedb 0
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

test "107 SCANSELECT" do |r|
  r.changedb 0
  assert ["1,ONE"] == r.scanselect("*", "new_table", "val = ONE")
end

test "108 CREATE INDEX" do |r|
  r.changedb 0
  r.create_index("new_table:val:index", "(new_table.val)")
  assert "index" == r.type("new_table:val:index")
end

test "109 DROP INDEX" do |r|
  r.changedb 0
  r.drop_index("new_table:val:index")
  assert "none" == r.type("new_table:val:index")
end

test "110 UPDATE" do |r|
  r.changedb 0
  r.update("new_table", "val=two", "id = 2")
  assert "two" == r.select("val", "new_table", "id = 2")
end

test "111 DELETE" do |r|
  r.changedb 0
  r.delete("new_table", "id = 2")
  assert nil == r.select("id", "new_table", "id = 2")
end

test "120 DROP TABLE" do |r|
  r.changedb 0
  r.drop_table("new_table");
  assert "none" == r.type("new_table")
end
