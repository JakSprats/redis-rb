require 'rubygems'
require 'redis'

r = Redis.new

if "table" == r.type("user")
  r.drop_table("user");
end
if "table" == r.type("user_address")
  r.drop_table("user_address");
end
if "table" == r.type("user_payment")
  r.drop_table("user_payment");
end

puts "First populate user:id:[name,age,status] ";
r.set("user:1:name", "bill");
r.set("user:1:age", "33");
r.set("user:1:status", "member");

puts "Then  populate user:id:address[street,city,zipcode] ";
r.set("user:1:address:street", "12345 main st");
r.set("user:1:address:city", "capitol city");
r.set("user:1:address:zipcode", "55566");

puts "Then  populate user:id:payment[type,account] ";
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

puts "Keys are now populated";
puts
puts "Finally search through all redis keys using ";
puts "  the primary wildcard:\"user\" ";
puts "  and then search through those results using:";
puts "    1.) the secondary wildcard: \"*:address\" ";
puts "    2.) the secondary wildcard: \"*:payment\" ";
puts "    3.) non matching stil match the primary wildcard ";
puts
puts "The 3 results will be normalised into the tables:";
puts "  1.) user_address";
puts "  2.) user_payment";
puts "  3.) user";

r.normalize("user", "address,payment");
puts
puts "SELECT user.pk,user.name,user.status,user_address.city,user_address.street,user_address.pk,user_address.zipcode FROM user,user_address WHERE user.pk=user_address.pk AND user.pk BETWEEN 1 AND 5"
p r.select("user.pk,user.name,user.status,user_address.city,user_address.street,user_address.pk,user_address.zipcode", "user,user_address", "user.pk=user_address.pk AND user.pk BETWEEN 1 AND 5")
puts
puts
puts

puts "If pure lookup speed of a SINGLE column is the dominant use case";
puts "We can now denorm the redisql tables into redis hash-tables";
puts "which are faster for this use-case";
puts
puts "denorm user \user:*";
r.denormalize("user", 'user:*')

puts "HGETALL user:1";
p r.hgetall("user:1")

puts
puts "denorm user_payment \user:*:payment";
r.denormalize("user_payment", 'user:*:payment');
puts "HGETALL user:2:payment";
p r.hgetall("user:2:payment")

puts 
puts "denorm user \user:*:address";
r.denormalize("user_address", 'user:*:address');
puts "HGETALL user:3:address";
p r.hgetall("user:3:address")
