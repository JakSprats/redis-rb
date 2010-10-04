require 'rubygems'
require 'redis'
require 'mysql'

# PURPOSE:
#  this script will backup ALL (except string) redis data objects into timestamped tables in the mysql DB "backupdb"
#
# USAGE FREQUENCY: daily
#
# REQUIREMENTS
#  1.) create database "backupdb"

r = Redis.new
time = Time.new
date_string = time.day.to_s() + "_" + time.month.to_s() + "_" + time.year.to_s()

con = Mysql.new('localhost', 'root', '', 'backupdb')

r.keys("*").each do |key|
  type = r.type(key)
  if type != "index" && type != "string"
    backup_table = "backup_" + key;
    puts "BACKUP: " + key + " TO REDISQL TABLE: " + backup_table
    begin
      r.drop_table(backup_table)
    rescue StandardError => bang
    end
    r.create_table_from_redis_object(backup_table, key)
    mysql_backup_table = "redis_backup_" + key + "_" + date_string
    puts "DUMP: " + backup_table + " TO MYSQL TABLE: " + mysql_backup_table

    tbl_contents = r.dump_to_mysql(backup_table, mysql_backup_table)
    begin
      tbl_contents.each do |mysql_cmd|
        con.query(mysql_cmd)
      end
    rescue StandardError => bang
      puts "mysql.query error: " + bang
    end
    r.drop_table(backup_table)
  end
end

con.close
