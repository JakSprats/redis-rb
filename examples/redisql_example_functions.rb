require 'rubygems'
require 'redis'

def init_external(r)
  r.create_table("external", "id int primary key, division int, health int, salary TEXT, name TEXT")
  r.create_index("external:division:index", "external", "division")
  r.create_index("external:health:index  ", "external", "health")
end
def init_healthplan(r)
  r.create_table("healthplan", "id int primary key, name TEXT")
end
def init_division(r)
  r.create_table("division", "id int primary key, name TEXT, location TEXT")
  r.create_index("division:name:index", "division", "name")
end
def init_subdivision(r)
  r.create_table("subdivision", "id int primary key, division int, name TEXT")
  r.create_index("subdivision:division:index", "subdivision", "division")
end
def init_employee(r)
  r.create_table("employee", "id int primary key, division int, salary TEXT, name TEXT")
  r.create_index("employee:name:index", "employee", "name")
  r.create_index("employee:division:index", "employee", "division")
end
def init_customer(r)
  r.create_table("customer", "id int primary key, employee int, name TEXT, hobby TEXT")
  r.create_index("customer:employee:index", "customer", "employee")
  r.create_index("customer:hobby:index   ", "customer", "hobby")
end
def init_worker(r)
  r.create_table("worker", "id int primary key, division int, health int, salary TEXT, name TEXT")
  r.create_index("worker:division:index", "worker", "division")
  r.create_index("worker:health:index  ", "worker", "health")
end

def insert_external(r)
  r.insert("external", "1,66,1,15000.99,marieanne")
  r.insert("external", "2,33,3,75000.77,rosemarie")
  r.insert("external", "3,11,2,55000.55,johnathan")
  r.insert("external", "4,22,1,25000.99,bartholemew")
end
def insert_healthplan(r)
  r.insert("healthplan", "1,none")
  r.insert("healthplan", "2,kaiser")
  r.insert("healthplan", "3,general")
  r.insert("healthplan", "4,extended")
  r.insert("healthplan", "5,foreign")
end
def insert_subdivision(r)
  r.insert("subdivision", "1,11,middle-management")
  r.insert("subdivision", "2,11,top-level")
  r.insert("subdivision", "3,44,trial")
  r.insert("subdivision", "4,44,research")
  r.insert("subdivision", "5,22,factory")
  r.insert("subdivision", "6,22,field")
end
def insert_division(r)
  r.insert("division", "11,bosses,N.Y.C")
  r.insert("division", "22,workers,Chicago")
  r.insert("division", "33,execs,Dubai")
  r.insert("division", "55,bankers,Zurich")
  r.insert("division", "66,janitors,Detroit")
  r.insert("division", "44,lawyers,L.A.")
end
def insert_employee(r)
  r.insert("employee", "1,11,10000.99,jim")
  r.insert("employee", "2,22,2000.99,jack")
  r.insert("employee", "3,33,30000.99,bob")
  r.insert("employee", "4,22,3000.99,bill")
  r.insert("employee", "5,22,5000.99,tim")
  r.insert("employee", "6,66,60000.99,jan")
  r.insert("employee", "7,77,7000.99,beth")
  r.insert("employee", "8,88,80000.99,kim")
  r.insert("employee", "9,99,9000.99,pam")
  r.insert("employee", "11,111,111000.99,sammy")
end
def insert_customer(r)
  r.insert("customer", "1,2,johnathan,sailing")
  r.insert("customer", "2,3,bartholemew,fencing")
  r.insert("customer", "3,3,jeremiah,yachting")
  r.insert("customer", "4,4,christopher,curling")
  r.insert("customer", "6,4,jennifer,stamps")
  r.insert("customer", "7,4,marieanne,painting")
  r.insert("customer", "8,5,rosemarie,violin")
  r.insert("customer", "9,5,bethany,choir")
  r.insert("customer", "10,6,gregory,dance")
end
def insert_worker(r)
  r.insert_and_return_size("worker", "1,11,2,60000.66,jim")
  r.insert_and_return_size("worker", "2,22,1,30000.33,jack")
  r.insert_and_return_size("worker", "3,33,4,90000.99,bob")
  r.insert_and_return_size("worker", "4,44,3,70000.77,bill")
  r.insert_and_return_size("worker", "6,66,1,10000.99,jan")
  r.insert_and_return_size("worker", "7,66,1,11000.99,beth")
  r.insert_and_return_size("worker", "8,11,2,68888.99,mac")
  r.insert_and_return_size("worker", "9,22,1,31111.99,ken")
  r.insert_and_return_size("worker", "10,33,4,111111.99,seth")
end

def initer(r)
  begin
    init_worker(r)
    init_customer(r)
    init_employee(r)
    init_division(r)
    init_subdivision(r)
    init_healthplan(r)
    init_external(r)
  rescue StandardError => bang
    puts "EXCEPTION IN INITER: " + bang
  end
end
def inserter(r)
  begin
    insert_worker(r)
    insert_customer(r)
    insert_employee(r)
    insert_division(r)
    insert_subdivision(r)
    insert_healthplan(r)
    insert_external(r)
  rescue StandardError => bang
    puts "EXCEPTION IN INSERTER: " + bang
  end
end

def selecter(r)
  p r.select("*", "division", "id = 22")
  p r.select("name, location", "division", "id = 22") 
  p r.select("*", "employee", "id = 2") 
  p r.select("name,salary", "employee", "id = 2") 
  p r.select("*", "customer", "id = 2") 
  p r.select("name", "customer", "id = 2")
  p r.select("*", "worker", "id = 7")   
  p r.select("name, salary, division", "worker", "id = 7")
  p r.select("*", "subdivision", "id = 2")  
  p r.select("name,division", "subdivision", "id = 2")
  p r.select("*", "healthplan", "id = 2")   
  p r.select("name", "healthplan", "id = 2")  
  p r.select("*", "external", "id = 3") 
  p r.select("name,salary,division", "external", "id = 3")
end

def updater(r)
  p r.select("*", "employee", "id = 1")
  p r.update("employee", "salary=50000,name=NEWNAME,division=66", "id = 1")
  p r.select("*", "employee", "id = 1")
  p r.update("employee", "id=100", "id = 1")
  p r.select("*", "employee", "id = 100")
end

def delete_employee(r)
  p r.select("name,salary", "employee", "id = 3")
  p r.delete("employee", "id = 3")
  p r.select("name,salary", "employee", "id = 3")
end
def delete_customer(r)
  p r.select("name, hobby", "customer", "id = 7")
  p r.delete("customer", "id = 7")
  p r.select("name, hobby", "customer", "id = 7")
end
def delete_division(r)
  p r.select("name, location", "division", "id = 33")
  p r.delete("division", "id = 33")
  p r.select("name, location", "division", "id = 33")
end
  
def deleter(r)
  delete_employee(r)
  delete_customer(r)
  delete_division(r)
end

def iselecter_division(r)
  p r.select("id,name,location", "division", "name BETWEEN a AND z")
end
def iselecter_employee(r)
  p r.select("id,name,salary,division", "employee", "division BETWEEN 11 AND 55")
end
def iselecter_customer(r)
  p r.select("hobby,id,name,employee", "customer", "hobby BETWEEN a AND z")
end
def iselecter_customer_employee(r)
  p r.select("employee,name,id", "customer", "employee BETWEEN 3 AND 6")
end
def iselecter_worker(r)
  p r.select("id,health,name,salary,division", "worker", "health BETWEEN 1 AND 3")
end
def iselecter(r)
  iselecter_division(r)
  iselecter_employee(r)
  iselecter_customer(r)
end
def iupdater_customer(r)
  p r.update("customer", "hobby=fishing,employee=6", "hobby BETWEEN v AND z")
end
def iupdater_customer_rev(r)
  p r.update("customer", "hobby=ziplining,employee=7", "hobby BETWEEN f AND g")
end
def ideleter_customer(r)
  p r.delete("customer", "employee BETWEEN 4 AND 5")
end


def join_div_extrnl(r)
  p r.select("division.name,division.location,external.name,external.salary", "division,external", "division.id=external.division AND division.id BETWEEN 11 AND 80")
end

def join_div_wrkr(r)
  p r.select("division.name,division.location,worker.name,worker.salary", "division,worker", "division.id = worker.division AND division.id BETWEEN 11 AND 33")
end

def join_wrkr_health(r)
  p r.select("worker.name,worker.salary,healthplan.name", "worker,healthplan", "worker.health = healthplan.id AND healthplan.id BETWEEN 1 AND 5")
  p r.select("healthplan.name,worker.name,worker.salary", "healthplan,worker", "healthplan.id=worker.health AND healthplan.id BETWEEN 1 AND 5")
end

def join_div_wrkr_sub(r)
  p r.select("division.name,division.location,worker.name,worker.salary,subdivision.name", "division,worker,subdivision", "division.id = worker.division AND division.id = subdivision.division AND division.id BETWEEN 11 AND 33")
end

def join_div_sub_wrkr(r)
  p r.select("division.name,division.location,subdivision.name,worker.name,worker.salary", "division,subdivision,worker", "division.id = subdivision.division AND division.id = worker.division AND division.id BETWEEN 11 AND 33")
end

def joiner(r)
  join_div_extrnl(r)
  join_div_wrkr(r)
  join_wrkr_health(r)
  join_div_wrkr_sub(r)
  join_div_sub_wrkr(r)
end


def works(r)
  initer(r)
  inserter(r)
  selecter(r)
  iselecter(r)
  updater(r)
  iselecter_employee(r)
  deleter(r)
  iselecter(r)
  iupdater_customer(r)
  iselecter_customer(r)
  ideleter_customer(r)
  iselecter_customer_employee(r)
  joiner(r)
end

def single_join_div_extrnl(r)
  init_division(r)
  insert_division(r)
  init_external(r)
  insert_external(r)
  join_div_extrnl(r)
end

def single_join_wrkr_health_rev(r)
  init_worker(r)
  insert_worker(r)
  init_healthplan(r)
  insert_healthplan(r)
  p r.select("healthplan.name,worker.name,worker.salary", "healthplan,worker", "healthplan.id=worker.health AND healthplan.id BETWEEN 1 AND 5")
end

def single_join_wrkr_health(r)
  init_worker(r)
  insert_worker(r)
  init_healthplan(r)
  insert_healthplan(r)
  p r.select("worker.name,worker.salary,healthplan.name", "worker,healthplan", "worker.health=healthplan.id AND healthplan.id BETWEEN 1 AND 5")
end

def single_join_sub_wrkr(r)
  init_division(r)
  insert_division(r)
  init_worker(r)
  insert_worker(r)
  init_subdivision(r)
  insert_subdivision(r)
  join_div_sub_wrkr(r)
end

def scan_external(r)
  p r.scanselect("name,salary", "external", "salary BETWEEN 15000.99 AND 25001.01")
end
def scan_healthpan(r)
   p r.scanselect("*", "healthplan", "name BETWEEN a AND k")
end

def istore_worker_name_list(r)
  p r.select_store("name", "worker", "division BETWEEN 11 AND 33", "RPUSH", "l_worker_name")
  p r.lrange("l_worker_name", 0 -1)
end

def istore_worker_hash_name_salary(r)
  p r.select_store("name,salary", "worker", "division BETWEEN 11 AND 33", "HSET", "h_worker_name_to_salary")
  p r.hgetall("h_worker_name_to_salary")
end

def jstore_div_subdiv(r)
  begin
    r.dropTable("normal_div_subdiv")
  rescue StandardError => bang
  end
  p r.select_store("subdivision.id,subdivision.name,division.name", "subdivision,division", "subdivision.division = division.id AND division.id BETWEEN 11 AND 44", "INSERT", "normal_div_subdiv")
  p r.dump("normal_div_subdiv")
end

def jstore_worker_location_hash(r)
  p r.select_store("external.name,division.location", "external,division", "external.division=division.id AND division.id BETWEEN 11 AND 80", "HSET", "worker_city_hash")
  p r.hgetall("worker_city_hash")
  puts
end

def jstore_worker_location_table(r)
  begin
    r.dropTable("w_c_tbl")
  rescue StandardError => bang
  end
  p r.select_store("external.name,division.location", "external,division", "external.division=division.id AND division.id BETWEEN 11 AND 80", "INSERT", "w_c_tbl")
  p r.dump("w_c_tbl")
  puts
end
