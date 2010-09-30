# encoding: UTF-8

require File.expand_path("./helper", File.dirname(__FILE__))
require "redis/distributed"

setup do
  log = StringIO.new
  init Redis::Distributed.new(NODES, :logger => ::Logger.new(log))
end

test "PING" do |r|
  assert ["PONG"] == r.ping
end

test "CHANGEDB" do |r|
  r.set "foo", "bar"

  r.changedb 14
  assert nil == r.get("foo")

  r.changedb 15

  assert "bar" == r.get("foo")
end

