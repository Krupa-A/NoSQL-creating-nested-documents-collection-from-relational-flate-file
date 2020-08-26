from pymongo import MongoClient
import csv
from pymongo.errors import DuplicateKeyError
import json
from xml.dom.minidom import parseString
from dicttoxml import dicttoxml


# creating connectioons for communicating with Mongo DB
client = MongoClient('localhost:27017')
db = client.DB2
db1 = client.project2


def projectDocument():

    rows = makeProjectDocument()
    data = {}
    data['Projects'] = []
    for eachline in rows:
        try:
            db1.projects.insert_one(
                eachline
            )
        except (DuplicateKeyError):
            print()

        data['Projects'].append(eachline)
    # Serializing json
    json_object = json.dumps(data, indent=4)

    # Writing to sample.json
    with open("output/Projects.json", "w") as outfile:
        outfile.write(json_object)

def employeeDocument():

    rows = makeEmployeeDocument()
    data = {}
    data['Employees'] = []
    for eachline in rows:
        try:
            db1.employees.insert_one(
                eachline
            )
        except (DuplicateKeyError):
            print()
        data['Employees'].append(eachline)
    # Serializing json
    json_object = json.dumps(data, indent=4)

    # Writing to sample.json
    with open("output/Employees.json", "w") as outfile:
        outfile.write(json_object)

def departmentsDocument():
    rows = makeDepartmentsDocument()
    data = {}
    data['Departments'] = []
    for eachline in rows:
        try:
            db1.departments.insert_one(
                eachline
            )
        except (DuplicateKeyError):
            print()
        data['Departments'].append(eachline)
    # Serializing json
    json_object = json.dumps(data, indent=4)

    # Writing to sample.json
    with open("output/Departments.json", "w") as outfile:
        outfile.write(json_object)

def employeeToXML():
    xml=[]
    rows = makeEmployeeDocument()
    my_item_func = lambda x: x[:-1]
    xml=( dicttoxml(rows,custom_root='Employees',attr_type=False,item_func= my_item_func))
    xml2 = parseString(xml)
    xml3 = xml2.toprettyxml()
    with open("output/Employees.xml", "w") as outfile:
        outfile.write(xml3)
    # print(xml3)

def projectToXML():
    xml = []
    rows = makeProjectDocument()
    my_item_func = lambda x: x[:-1]
    xml = (dicttoxml(rows, custom_root='Projects', attr_type=False, item_func=my_item_func))
    xml2 = parseString(xml)
    xml3 = xml2.toprettyxml()
    with open("output/Projects.xml", "w") as outfile:
        outfile.write(xml3)
    # print(xml3)


#this is the function to make PROJECT document
def makeProjectDocument():
#this fuction is used to join the project and department document to get the  project_name, project_number and department_name
    final=db.project.aggregate([
        {"$match": {}},
        {"$lookup": {
            "from": "department",
            "localField": "Dnumber",
            "foreignField": "Dnumber",
            "as": "R"
        }},
        {"$unwind": "$R"},

        {"$lookup": {
            "from": "works_on",
            "localField": "Pnumber",
            "foreignField": "Pnumber",
            "as": "S"
        }},
        {"$unwind": "$S"},
        {"$lookup": {
            "from": "employee",
            "localField": "S.ESSN",
            "foreignField": "SSN",
            "as": "Q"
        }},
        {"$unwind": "$Q"},

        {"$group": {"_id": "$Pnumber","Pname": {"$first":"$Pname"},"Dname": {"$first":"$R.Dname"},
                    "worker": {"$push": {"Fname": "$Q.Fname", "Lname": "$Q.Lname", "Hours": "$S.hours"}}}},
    ])
    return final

#this is the function to make EMPLOYEE document
def makeEmployeeDocument():
#this fuction is used to join the project and department document to get the  project_name, project_number and department_name
    final=db.employee.aggregate([
        {"$match": {}},
        {"$lookup": {
            "from": "department",
            "localField": "Dnumber",
            "foreignField": "Dnumber",
            "as": "R"
        }},
        {"$unwind": "$R"},

        {"$lookup": {
            "from": "works_on",
            "localField": "SSN",
            "foreignField": "ESSN",
            "as": "S"
        }},
        {"$unwind": "$S"},

        {"$lookup": {
            "from": "project",
            "localField": "S.Pnumber",
            "foreignField": "Pnumber",
            "as": "Q"
        }},
        {"$unwind": "$Q"},

        {"$group": {"_id": "$SSN","EMP_Fname": {"$first":"$Fname"},"EMP_Lname": {"$first":"$Lname"},"Dname": {"$first":"$R.Dname"},
                    "projects": {"$push": {"Pnumber": "$S.Pnumber", "Pname": "$Q.Pname", "Hours": "$S.hours"}}}},
    ])
    return final

def makeDepartmentsDocument():
#this fuction is used to join the project and department document to get the  project_name, project_number and department_name
    final=db.department.aggregate([
        {"$match": {}},
        {"$lookup": {
            "from": "employee",
            "localField": "mgr_ssn",
            "foreignField": "SSN",
            "as": "R"
        }},
        {"$unwind": "$R"},

        {"$lookup": {
            "from": "employee",
            "localField": "mgr_ssn",
            "foreignField": "Super_SSN",
            "as": "S"
        }},
        {"$unwind": "$S"},

        {"$group": {"_id": "$Dnumber","Dname": {"$first":"$Dname"},"MGR_Fname": {"$first":"$R.Fname"},"MGR_Lname": {"$first":"$R.Lname"},
                    "employeess": {"$push": {"EMP_Fname": "$S.Fname", "EMP_Lname": "$S.Lname", "Salary": "$S.Salary"}}}},

    ])

    return final

def performQuery():
    que1=db1.projects.find({'Dname': 'Software'},{"Dname":1,"worker":1})
    print("""que1=db1.projects.find({'Dname': 'Software'},{"Dname":1,"worker":1})""")
    for que in que1:
        print(que)
    print()
    que2 = db1.projects.find({'Dname': 'Hardware'},{"Pname":1,"Dname":1,"worker":{ "$slice": -1 }})
    print("""que2 = db1.projects.find({'Dname': 'Hardware'},{"Pname":1,"Dname":1,"worker":{ "$slice": -1 }})""")
    for que in que2:
        print(que)
    print()
    que3 = db1.employees.find({'EMP_Fname': 'Kim', "Dname": "Software"})
    print("""que3 = db1.employees.find({'EMP_Fname': 'Kim', "Dname": "Software"})""")
    for que in que3:
        print(que)
    print()
    que4 = db1.employees.find({'projects.Pname': 'MotherBoard', "projects.Hours":{"$gte": 5}})
    print("""que4 = db1.employees.find({'projects.Pname': 'MotherBoard', "projects.Hours":{"$gte": 5}})""")
    for que in que4:
        print(que)
    print()

def main():

    csvfile = open('Input/DEPARTMENT.txt', 'r')
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar="'",skipinitialspace = True)
    for eachline in csv_reader:
        try:
            db.department.create_index('Dnumber', unique=True)
            db.department.insert_one(
                {
                    "Dname": str(eachline[0]),
                    "Dnumber": int(eachline[1]),
                    "mgr_ssn": (eachline[2]),
                    "mgr_start_date": str(eachline[3])
                }
            )
        except (DuplicateKeyError):
            print()

    csvfile = open('Input/DEPT_LOCATIONS.txt', 'r')
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar="'",skipinitialspace = True)
    for eachline in csv_reader:
        try:
            db.depart_location.create_index([('Dnumber',1),('Dlocation',1)], unique=True)
            db.depart_location.insert_one(
                {
                    "Dnumber": int(eachline[0]),
                    "Dlocation": (eachline[1])
                }
            )
        except (DuplicateKeyError):
            print()

    csvfile = open('Input/EMPLOYEE.txt', 'r')
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar="'",skipinitialspace = True)
    for eachline in csv_reader:
        try:
            db.employee.create_index('SSN', unique=True)
            db.employee.insert_one(
                {
                    "Fname": (eachline[0]),
                    "Minit": (eachline[1]),
                    "Lname": (eachline[2]),
                    "SSN": (eachline[3]),
                    "Bdate": (eachline[4]),
                    "Address": (eachline[5]),
                    "Sex": (eachline[6]),
                    "Salary": int(eachline[7]),
                    "Super_SSN":(eachline[8]),
                    "Dnumber": int(eachline[9])

                }
            )
        except (DuplicateKeyError):
            print()

    csvfile = open('Input/PROJECT.txt', 'r')
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar="'",skipinitialspace = True)
    for eachline in csv_reader:
        try:
            db.project.create_index('Pnumber', unique=True)
            db.project.insert_one(
                {
                    "Pname": (eachline[0]),
                    "Pnumber": int(eachline[1]),
                    "Dlocation": (eachline[2]),
                    "Dnumber": int(eachline[3])

                }
            )
        except (DuplicateKeyError):
            print()

    csvfile = open('Input/WORKS_ON.txt', 'r')
    csv_reader = csv.reader(csvfile, delimiter=',', quotechar="'",skipinitialspace = True)
    for eachline in csv_reader:
        try:
            db.works_on.create_index([('ESSN',1),('Pnumber',1)], unique=True)
            db.works_on.insert_one(
                {
                    "ESSN": (eachline[0]),
                    "Pnumber": int(eachline[1]),
                    "hours":float(eachline[2])
                }
            )
        except (DuplicateKeyError):
            print()

    # krupa= db.works_on.find({"ESSN" :444444400})
    # for i in krupa:
    #     print(i)
    projectDocument()
    employeeDocument()
    departmentsDocument()
    projectToXML()
    employeeToXML()
    performQuery()


if __name__ == '__main__':
    main()


# one = db.project.find({}, {"Pname": 1, "Pnumber": 1, "Dnumber": 1, "_id": 0})

