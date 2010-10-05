require 'rubygems'
require 'redis'

r = Redis.new

begin
  r.drop_table("actionlist")
rescue StandardError => bang
end

r.create_table("actionlist", "(id INT PRIMARY KEY, user_id INT, timestamp INT, action TEXT)")

#USER 1
r.insert("actionlist", "(1,1,12345,account created")
r.insert("actionlist", "(2,1,12346,first login")
r.insert("actionlist", "(3,1,12347,became paid member")
r.insert("actionlist", "(4,1,12348,posted picture")
r.insert("actionlist", "(5,1,12349,filled in profile")
r.insert("actionlist", "(6,1,12350,signed out")

#USER 2
r.insert("actionlist", "(7,2,22345,signed in")
r.insert("actionlist", "(8,2,22346,updated picture")
r.insert("actionlist", "(9,2,22347,checked email")
r.insert("actionlist", "(10,2,22348,signed in")

#USER 3
r.insert("actionlist", "(11,3,32348,signed in")
r.insert("actionlist", "(12,3,32349,contacted customer care")
r.insert("actionlist", "(13,3,32350,upgraded account")
r.insert("actionlist", "(14,3,32351,uploaded video")

# select_store a query into many lists named "user_action_zset:$user_id" redspectively
puts "select user_id, timestamp, action FROM actionlist WHERE id BETWEEN 1 AND 20 STORE ZADD user_action_zset$ "
p r.select_store("user_id, timestamp, action", "actionlist", "id BETWEEN 1 AND 20", "ZADD",  "user_action_zset$")

# Get user 1's last two actions
puts "ZREVRANGE user_action_zset:1 0 1"
p r.zrevrange("user_action_zset:1", 0, 1)



