require 'rubygems'
require 'redis'
require 'mysql'

r = Redis.new

=begin
Requirements: 
  1.) Create Mysql database "test"
  2.) issue the following SQL commands
    A.) CREATE TABLE employee_data ( emp_id int unsigned not null auto_increment primary key, f_name varchar(20), l_name varchar(20), title varchar(30), age int, yos int, salary int, perks int, email varchar(60) );
    B.) INSERT INTO employee_data (f_name, l_name, title, age, yos, salary, perks, email) values ("Beth", "Smith", "CTO", 39, 1, 90000,  10000, "beth@bignet.com");
    C.) INSERT INTO employee_data (f_name, l_name, title, age, yos, salary, perks, email) values ("Bill", "Jones", "Manager", 29, 3, 100000,  20000, "jim@bignet.com");
=end

def import_from_mysql(r, con, tname)
  rs               = con.query("SHOW COLUMNS FROM " + tname)
  col_select       = ""
  col_defs         = ""
  tbl_has_date_col = 0
  i                = 0 
  rs.each_hash do |row|
    if i != 0
      col_select += ", "
      col_defs   += ", "
    end
    if row['Type'].casecmp("datetime") == 0 || # DATEs become INTs
       row['Type'].casecmp("timestamp") == 0
      tbl_has_date_col  = 1
      col_select       += "unix_timestamp(" + row['Field'] + ")"
      col_defs         += row['Field'] + " INT"
    else
      col_select += row['Field']
      col_defs   += row['Field'] + " " + row['Type']
    end
    i += 1
  end

  r.create_table(tname, col_defs)

  if tbl_has_date_col == 0
      # this avoids errors when columns have SQL KEYWORD names
      col_select = "*"
  end

  rs = con.query("SELECT " + col_select + " FROM " + tname)
  rs.each do |row|
    values_list = ""
    for j in 0..(i - 1)
      if j != 0
        values_list += ","
      end
      values_list += row[j].gsub(",", "\\,")
      begin
        r.insert(tname, values_list)
      rescue StandardError => bang # just ignore for now
      end
    end
  end
end

tname = "employee_data"
if "table" == r.type(tname)
  r.drop_table(tname);
end

con = Mysql.new('localhost', 'root', '', 'test')
import_from_mysql(r, con, tname)
con.close

p r.dump(tname)

# now that its a table, denormalise it into hashes -> you are schemaless
wildcard = tname + ':*';
r.denormalize(tname, wildcard);

hname = tname + ":1"
puts "HGETALL " + hname
p r.hgetall(hname)
