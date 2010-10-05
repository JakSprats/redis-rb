require 'rubygems'
require 'redis'

require File.expand_path("./redisql_example_functions", File.dirname(__FILE__))

r = Redis.new
works(r)

#jstore_div_subdiv(r)
#jstore_worker_location_hash(r)
#jstore_worker_location_table(r)

