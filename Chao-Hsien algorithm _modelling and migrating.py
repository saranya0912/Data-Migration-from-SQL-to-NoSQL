# importing the libraries
import sys
import MySQLdb
from collections import defaultdict, OrderedDict
from pymongo import MongoClient
# connecting the MySQLdb database through mysql driver
db_connect = MySQLdb.connect(user='root', passwd='root', db='employees')
# registering the cursor
cursor = db_connect.cursor()
# creating the mongoclient for the port 27017
client = MongoClient('localhost:27017')
# creating the old_employee database
db = client.old_employees
# query for the referencing the table from information schema table
query2 = "select TABLE_NAME, REFERENCED_TABLE_NAME from information_schema.referential_constraints where CONSTRAINT_SCHEMA='employees';"

cursor.execute(query2)
# fetching records from the above query
data = cursor.fetchall()

fk = defaultdict(list)
# looping through the each table and storing the referencinng keys
for row in data:

    fk[row[0]].append(row[1])

# query for the  available table in the employees schema
query3 = "select table_name  from information_schema.tables where table_schema='employees';"

cursor.execute(query3)
# fetching all the records of above query
data = cursor.fetchall()

tab = {}
# looping through the each table and assigning with 0
for row in data:
    tab[row[0]] = 0
schema = []

# loop the tab list
for table in tab:

    ll = []
    # storing the schema string
    schema_str = ""
    # query for the column names for the particluar table
    query = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='employees' AND `TABLE_NAME`='" + table + "';"

    cursor.execute(query)
    # fetching the records
    data_1 = cursor.fetchall()
    # analysiing schema for each table
    for row_1 in data_1:
        ll.append(table + "." + row_1[0])

    schema_str += table
    # if the table has foreign keys, then it analysing th next table
    if len(fk[table]) >= 1:

        for i in range(len(fk[table])):

            query = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='employees' AND `TABLE_NAME`='" + \
                    fk[table][i] + "';"

            cursor.execute(query)

            data_2 = cursor.fetchall()

            for row_2 in data_2:
                ll.append(fk[table][i] + "." + row_2[0])

            schema_str += "+" + fk[table][i]

    schema.append(schema_str)

print "Below collections are created:"
i = 0
for name in schema:
    i = i + 1
    print i, ".)", name, " collections is created"
# collections created from the above list
collection = db['employee']

collection.drop()

query_1 = "select * from employees"

cursor.execute(query_1)

data_1 = cursor.fetchall()

emp_data = []

count = 0
# looping through the each employee data and storing the all details
for row in data_1:

    emp = OrderedDict()

    emp['emp_no'] = int(row[0])

    emp['birth_date']=row[1].strftime("%m-%d-%Y")

    emp['first_name']=row[2]

    emp['last_name']=row[3]

    emp['gender']=row[4]

    emp['hire_date']=row[5].strftime("%m-%d-%Y")

    emp_data.append(emp)

    count = count + 1
    # for every 15000 rows, storing the the data and creating collections
    if count % 15000 == 0:

        i = count / 15000

        result = collection.insert(emp_data)

        sys.stdout.write('\r')

        sys.stdout.write("[%-20s] %d%% migrating data to MongoDB" % ('=' * i, 5 * i))

        sys.stdout.flush()

        emp_data = []


result = collection.insert(emp_data)

sys.stdout.write('\n')

print "Employee Data has Successfully migrated"

collection = db['departments']

collection.drop()
# query about the departments
query_1 = "select * from departments"

cursor.execute(query_1)

data_1 = cursor.fetchall()

dep_data = []

i = 1
# looping through the each department
for row in data_1:

    dep = OrderedDict()

    dep['dept_no'] = row[0]

    dep['dept_name'] = row[1]

    dep_data.append(dep)

result = collection.insert(dep_data)

sys.stdout.write('\r')

sys.stdout.write("[%-20s] %d%% migrating data to MongoDB" % ('=' * 20, 100))

sys.stdout.flush()

sys.stdout.write('\n')

print "Department Data has Successfully migrated"

collection = db['emp_title']

collection.drop()
# querying the employee data
query_1 = "select * from employees"

cursor.execute(query_1)
#fetching the employee data
data_1 = cursor.fetchall()

emp_data = []

count = 0
# storing all the emp_data related
for row in data_1:

    emp = OrderedDict()

    emp['emp_no'] = int(row[0])

    emp['birth_date']=row[1].strftime("%m-%d-%Y")

    emp['first_name']=row[2]

    emp['last_name']=row[3]

    emp['gender']=row[4]

    emp['hire_date']=row[5].strftime("%m-%d-%Y")

    query_2 = "select title, from_date, to_date  from titles where emp_no=" + str(row[0])

    cursor.execute(query_2)

    title_data = cursor.fetchall()

    title_list = []
    # storing all the tilte's data for each employee

    for title_row in title_data:

        title = {}

        title['title'] = title_row[0]

        title['from_date'] = title_row[1].strftime("%m-%d-%Y")

        title['to_date'] = title_row[2].strftime("%m-%d-%Y")

        title_list.append(title)

    emp['titles'] = title_list

    emp_data.append(emp)

    count = count + 1
    # for every 15000, storing the data into MongoDB
    if count % 15000 == 0:

        i = count / 15000

        result = collection.insert(emp_data)

        sys.stdout.write('\r')

        sys.stdout.write("[%-20s] %d%% migrating data to MongoDB " % ('=' * i, 5 * i))

        sys.stdout.flush()

        emp_data = []
result = collection.insert(emp_data)

sys.stdout.write('\n')

print "Employee and title Data has Successfully migrated"

collection = db['emp_salaries']

collection.drop()

query_1 = "select * from employees"

cursor.execute(query_1)

data_1 = cursor.fetchall()

emp_data = []

count = 0
# looping through the each employee
for row in data_1:

    emp = OrderedDict()

    emp['emp_no'] = int(row[0])

    emp['birth_date']=row[1].strftime("%m-%d-%Y")

    emp['first_name']=row[2]

    emp['last_name']=row[3]

    emp['gender']=row[4]

    emp['hire_date']=row[5].strftime("%m-%d-%Y")
    # for each employee, querying the salary details
    query_3 = "select salary, from_date, to_date  from salaries where emp_no=" + str(row[0])

    cursor.execute(query_3)

    salary_data = cursor.fetchall()

    salary_list = []
    # for each employee, storing the salary details
    for salary_row in salary_data:

        salary = {}

        salary['salary'] = int(salary_row[0])

        salary['from_date'] = salary_row[1].strftime("%m-%d-%Y")

        salary['to_date'] = salary_row[2].strftime("%m-%d-%Y")

        salary_list.append(salary)

    emp['salaries'] = salary_list

    emp_data.append(emp)

    count = count + 1
    # for every 15000. storing the data in MOngoDB
    if count % 15000 == 0:

        i = count / 15000

        result = collection.insert(emp_data)

        sys.stdout.write('\r')

        sys.stdout.write("[%-20s] %d%% migrating data to MongoDB" % ('=' * i, 5 * i))

        sys.stdout.flush()

        emp_data = []
result = collection.insert(emp_data)

sys.stdout.write('\n')

print "Employee and salaries Data has Successfully migrated"


collection = db['emp_dep_emp']

collection.drop()

query_1 = "select * from employees"

cursor.execute(query_1)
# fetching the records of employee
data_1 = cursor.fetchall()

emp_data = []

count = 0
# storing the employee details
for row in data_1:

    emp = OrderedDict()

    emp['emp_no'] = int(row[0])

    emp['birth_date']=row[1].strftime("%m-%d-%Y")

    emp['first_name']=row[2]

    emp['last_name']=row[3]

    emp['gender']=row[4]

    emp['hire_date']=row[5].strftime("%m-%d-%Y")
    # for ech employee, querying the about the department details
    query_4 = "select s.dept_no,d.dept_name,s.from_date, s.to_date from dept_emp s,departments d where " \
              "s.dept_no=d.dept_no and s.emp_no=" + str(row[0])

    cursor.execute(query_4)

    dept_emp_data = cursor.fetchall()

    dept_emp_list = []
    # looping through the department's data
    for dept_emp_row in dept_emp_data:

        dept_emp = {}

        dept_emp['dept_no'] = dept_emp_row[0]

        dept_emp['dept_name'] = dept_emp_row[1]

        dept_emp['from_date'] = dept_emp_row[2].strftime("%m-%d-%Y")

        dept_emp['to_date'] = dept_emp_row[3].strftime("%m-%d-%Y")

        dept_emp_list.append(dept_emp)

    emp['dept_emp'] = dept_emp_list

    emp_data.append(emp)

    count = count + 1

    if count % 15000 == 0:

        i = count / 15000

        result = collection.insert(emp_data)

        sys.stdout.write('\r')

        sys.stdout.write("[%-20s] %d%% migrating data to MongoDB" % ('=' * i, 5 * i))

        sys.stdout.flush()


        emp_data = []
result = collection.insert(emp_data)
sys.stdout.write('\n')
print "Employee and department_emp Data has Successfully migrated"

collection = db['emp_dep_mgr']

collection.drop()

query_5 = "select e.emp_no from dept_manager s,departments d , employees e where s.dept_no=d.dept_no and s.emp_no=e.emp_no"

cursor.execute(query_5)

data_1 = cursor.fetchall()

emp_data = []
# looping through the department manager ans storing the employee details
for row_1 in data_1:

    query = "select * from employees where emp_no =" + str(row_1[0])

    cursor.execute(query)

    data_2 = cursor.fetchall()

    for row in data_2:

        emp = OrderedDict()

        emp['emp_no'] = int(row[0])

        emp['birth_date'] = row[1].strftime("%m-%d-%Y")

        emp['first_name'] = row[2]

        emp['last_name'] = row[3]

        emp['gender'] = row[4]

        emp['hire_date'] = row[5].strftime("%m-%d-%Y")
        # querying department term period for the employees
        query_5 = "select s.dept_no,d.dept_name,s.from_date, s.to_date from dept_manager s,departments d where s.dept_no=d.dept_no and " \
                  "s.emp_no=" + str(row[0])

        cursor.execute(query_5)

        dept_mgr_data = cursor.fetchall()

        dept_mgr_list = []

        for dept_mgr_row in dept_mgr_data:

            dept_mgr = {}

            dept_mgr['dept_no'] = dept_mgr_row[0]

            dept_mgr['dept_name'] = dept_mgr_row[1]

            dept_mgr['from_date'] = dept_mgr_row[2].strftime("%m-%d-%Y")

            dept_mgr['to_date'] = dept_mgr_row[3].strftime("%m-%d-%Y")

            dept_mgr_list.append(dept_mgr)

        emp['dept_mgr'] = dept_mgr_list

        emp_data.append(emp)

result = collection.insert(emp_data)

sys.stdout.write('\r')

sys.stdout.write("[%-20s] %d%% migrating data to MongoDB" % ('=' * 20, 100))

sys.stdout.flush()

sys.stdout.write('\n')

print "Employee and department_Manager Data has Successfully migrated"
# Un registering the cursor
cursor.close()
# closing the database connection
db_connect.close()