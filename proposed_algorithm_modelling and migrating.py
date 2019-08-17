# importing the libraries
import MySQLdb
from collections import defaultdict, OrderedDict
import sys
from pymongo import MongoClient
# MySql database connection using MySQLdb
db_connect = MySQLdb.connect(user='root', passwd='root', db='employees')
# registering the cursor with db
cursor = db_connect.cursor()
# creating the MOngoDb client with port 27017
client = MongoClient('localhost:27017')
# selecting the database as new_employees
db = client.new_employees
# querying for referenced keys for a table
query1 = "select REFERENCED_TABLE_NAME,TABLE_NAME from information_schema.referential_constraints where CONSTRAINT_SCHEMA='employees';"
# executing the above query with the cursor
cursor.execute(query1)
# fetching the records from the query result
data = cursor.fetchall()
# creating the dictionary of ref, to store the ref items
ref = defaultdict(list)
# looping  through the query result
for row in data:
    ref[row[0]].append(row[1])
# querying for foreign keys keys for a table
query2 = "select TABLE_NAME, REFERENCED_TABLE_NAME from information_schema.referential_constraints where CONSTRAINT_SCHEMA='employees';"
# executing the above query with the cursor
cursor.execute(query2)
# fetching the records from the query result
data = cursor.fetchall()
# creating the dictionary of fk, to store the ref items
fk = defaultdict(list)
# looping  through the query result
for row in data:
    fk[row[0]].append(row[1])
# querying for foreign keys keys for a table
query3 = "select table_name  from information_schema.tables where table_schema='employees';"
# executing the above query with the cursor
cursor.execute(query3)
# fetching the records from the query result
data = cursor.fetchall()
# creating the set of tab, to store the ref items
tab = {}
# looping  through the query result
for row in data:
    tab[row[0]] = 0

schema = []

no_of_times = 0

print "Showing the tables with satisfied conditions: \n"
# looping  through the each and every table
for table in tab:

    if table not in fk.keys():

        if table in ref.keys():

            schema.append(table)

            print table + " satisfies second condition"
        # second condition satisfies in the algorithm
        else:

            schema.append(table)

            print table + " satisfies first condition"
        # first condition satisfies in the algorithm
    else:

        if len(fk[table]) == 1:

            if table in ref.keys():

                table_str = table

                for i in range(1, len(ref[table]) + 1):

                    str = fk[ref[table][i - 1]]

                    for x in range(1, len(str) + 1):

                        if str[x - 1] != table:

                            table_str = "_".join((table_str, str[x - 1]))

                            table_str = "_".join((table_str, ref[table][i - 1]))

                collection = db[table_str]

                result = collection.insert_one({})

                collection.drop()

                print table," satisfies third condition"
            # third condition satisfies in the algorithm

            elif table not in ref.keys():

                if any(fk[table][0] in s for s in schema):

                    x = [schema.index(i) for i in schema if fk[table][0] in i]

                    schema[x[0]] = schema[x[0]] + "+" + table

                print table + " is one-way embeddding to " + fk[table][0]

            # fourth condition satisfies in the algorithm

        elif len(fk[table]) == 2:

            print table + " is Two-way embeddding to " + fk[table][0] + " and " + fk[table][1]

            for iterate in range(2):

                no_of_times += 1

                if any(fk[table][iterate] in s for s in schema):

                    x = [schema.index(i) for i in schema if fk[table][iterate] in i]

                    schema[x[0]] = schema[x[0]] + "+" + table

            if no_of_times == 4:

                schema[iterate - 1] = schema[iterate - 1] + "+departments"

                schema[iterate] = schema[iterate] + "+employees"

            # fifth condition satisfies in the algorithm

        elif len(fk[table]) >= 3:

            print "This " + table + " is Multi-way embeddding to below " + len(fk[table]) + " tables"

            for i in range(len(fk[table])):

                print fk[table][i]

            for iterate in range(len(fk[table])):

                if any(fk[table][iterate] in s for s in schema):

                    x = [schema.index(i) for i in schema if fk[table][iterate] in i]

                    if iterate == 0:

                        schema[x[0]] = schema[x[0]] + "+" + table + "+" + fk[table][iterate + 1] + "+" + fk[table][
                            iterate + 2]

                    elif iterate == 1:

                        schema[x[0] + 1] = schema[x[0] + 1] + "+" + table + "+" + fk[table][iterate - 1] + "+" + \
                                           fk[table][iterate + 1]

                    elif iterate == 2:

                        schema[x[0] + 1] = schema[x[0] + 1] + "+" + table + "+" + fk[table][iterate - 2] + "+" + \
                                           fk[table][iterate - 1]
                    # sixth condition satisfies in the algorithm

print "\n\n Below collections are created:\n"

i = 0
# schema storing the collections name fro MongoDB
for name in schema:

    i = i + 1
    # displaying the collection names
    print i, ".)", name, " collections is created"

    collection = db[name]
    # creating the collections
    result = collection.insert_one({})

    collection.drop()

# selecting the below collection
collection = db['emp_sal_tit_dep_mgr']
# removing the collection
collection.drop()
# query for selecting the employees
query_1 = "select * from employees"
# executing the above query
cursor.execute(query_1)
# fetching all the data
data_1 = cursor.fetchall()
# creating the emp_data lsit
emp_data = []

count = 10000
# looping the each column of the employee table
for row in data_1:
    # creating the ordered dictionary of emp
    emp = OrderedDict()

    count += 1

    emp['emp_no'] = int(row[0])

    emp['birth_date'] = row[1].strftime("%m-%d-%Y")

    emp['first_name'] = row[2]

    emp['last_name'] = row[3]

    emp['gender'] = row[4]

    emp['hire_date'] = row[5].strftime("%m-%d-%Y")

    query_2 = "select title, from_date, to_date  from titles where emp_no=" + str(row[0])
    # executing the above query to fetch all record of a particular employee
    cursor.execute(query_2)

    title_data = cursor.fetchall()

    title_list = []
    # looping through the employee titles and storing the title's from date and to date
    for title_row in title_data:

        title = {}

        title['title'] = title_row[0]

        title['from_date'] = title_row[1].strftime("%m-%d-%Y")

        title['to_date'] = title_row[2].strftime("%m-%d-%Y")

        title_list.append(title)

    emp['titles'] = title_list
    # executing the above query to fetch all the records of a particular employee
    query_3 = "select salary, from_date, to_date  from salaries where emp_no=" + str(row[0])

    cursor.execute(query_3)

    salary_data = cursor.fetchall()

    salary_list = []
    # looping through the employee salaries and storing the title's from date and to date
    for salary_row in salary_data:

        salary = {}

        salary['salary'] = int(salary_row[0])

        salary['from_date'] = salary_row[1].strftime("%m-%d-%Y")

        salary['to_date'] = salary_row[2].strftime("%m-%d-%Y")

        salary_list.append(salary)

    emp['salaries'] = salary_list
    # executing the above query to fetch all the records of a particular employee
    query_4 = "select s.dept_no,d.dept_name,s.from_date, s.to_date from dept_emp s,departments d where s.dept_no=d.dept_no and s.emp_no=" + str(
        row[0])

    cursor.execute(query_4)

    dept_emp_data = cursor.fetchall()

    dept_emp_list = []
    # looping through the department employee  and storing the title's from date and to date
    for dept_emp_row in dept_emp_data:

        dept_emp = {}

        dept_emp['dept_no'] = dept_emp_row[0]

        dept_emp['dept_name'] = dept_emp_row[1]

        dept_emp['from_date'] = dept_emp_row[2].strftime("%m-%d-%Y")

        dept_emp['to_date'] = dept_emp_row[3].strftime("%m-%d-%Y")

        dept_emp_list.append(dept_emp)

    emp['dept_emp'] = dept_emp_list
    # query for the depart managers for storing the department no and it's term period
    query_5 = "select s.dept_no,d.dept_name,s.from_date, s.to_date from dept_manager s,departments d where s.dept_no=d.dept_no and " \
              "s.emp_no=" + str(row[0])

    cursor.execute(query_5)

    dept_mgr_data = cursor.fetchall()

    dept_mgr_list = []
    #looping through the each depart manger and storing all related details
    for dept_mgr_row in dept_mgr_data:

        dept_mgr = {}

        dept_mgr['dept_no'] = dept_mgr_row[0]

        dept_mgr['dept_name'] = dept_mgr_row[1]

        dept_mgr['from_date'] = dept_mgr_row[2].strftime("%m-%d-%Y")

        dept_mgr['to_date'] = dept_mgr_row[3].strftime("%m-%d-%Y")

        dept_mgr_list.append(dept_mgr)

    emp['dept_mgr'] = dept_mgr_list

    emp_data.append(emp)
    # for every 15000 rows,data is migrated to the MongoDB
    if count % 15000 == 0:

        i = count / 15000

        result = collection.insert(emp_data)

        sys.stdout.write('\r')

        sys.stdout.write("[%-20s] %d%% migrating data to MongoDB" % ('=' * i, 5 * i))

        sys.stdout.flush()

        emp_data = []

result = collection.insert(emp_data)
# all the employee data is migrated successfully
print "\n emp_sal_tit_dep_mgr data migrated successfully\n"
# creating the dep_mgr_emp collection
collection=db['dep_mgr_emp']

collection.drop()
# query for the selecting the departments
query_11 = "select * from departments"

cursor.execute(query_11)

data_1 = cursor.fetchall()

dept = []

i = 1
# looping through the each department
for row in data_1:

    dept_data = OrderedDict()

    dept_data['dept_no'] = row[0]

    dept_data['dept_name'] = row[1]

    # query to the finding the employees for particular department
    query_2 = "select * from dept_emp d, employees e where d.emp_no=e.emp_no and d.dept_no='" + row[0] + "'"

    cursor.execute(query_2)

    dept_emp_data = cursor.fetchall()

    dept_emp_list = []

    for dept_emp_row in dept_emp_data:

        dept_emp = OrderedDict()

        dept_emp['emp_no'] = int(dept_emp_row[0])

        dept_emp['birth_date'] = dept_emp_row[5].strftime("%m-%d-%Y")

        dept_emp['first_name'] = dept_emp_row[6]

        dept_emp['second_name'] = dept_emp_row[7]

        dept_emp['gender'] = dept_emp_row[8]

        dept_emp['hire_date'] = dept_emp_row[9].strftime("%m-%d-%Y")

        dept_emp['from_date'] = dept_emp_row[2].strftime("%m-%d-%Y")

        dept_emp['to_date'] = dept_emp_row[3].strftime("%m-%d-%Y")

        dept_emp_list.append(dept_emp)

    dept_data['dept_emp'] = dept_emp_list
    # query to find the department manger data for a particular department
    query_3 = "select * from dept_manager d, employees e where d.emp_no=e.emp_no and d.dept_no='" + row[0] + "'"

    cursor.execute(query_3)

    dept_mgr_data = cursor.fetchall()

    dept_mgr_list = []

    # Storing all the related data of department manager
    for dept_mgr_row in dept_mgr_data:

        dept_mgr = OrderedDict()

        dept_mgr['emp_no'] = int(dept_mgr_row[0])

        dept_mgr['birth_date'] = dept_mgr_row[5].strftime("%m-%d-%Y")

        dept_mgr['first_name'] = dept_mgr_row[6]

        dept_mgr['second_name'] = dept_mgr_row[7]

        dept_mgr['gender'] = dept_mgr_row[8]

        dept_mgr['hire_date'] = dept_mgr_row[9].strftime("%m-%d-%Y")

        dept_mgr['from_date'] = dept_mgr_row[2].strftime("%m-%d-%Y")

        dept_mgr['to_date'] = dept_mgr_row[3].strftime("%m-%d-%Y")

        dept_mgr_list.append(dept_mgr)

    dept_data['dept_manager'] = dept_mgr_list

    dept.append(dept_data)
    i = i + 1
    # after each iteration, storing related data into the collection
    
    result = collection.insert(dept)

    dept = []

    sys.stdout.write('\r')

    sys.stdout.write("[%-20s] %d%% migrating data to MongoDB" % ('=' * 2 *i, 10 * i))

    sys.stdout.flush()

print "\n dep_mgr_emp data migrated successfully"
# Un registering the cursor
cursor.close()
# closing the database connection
db_connect.close()