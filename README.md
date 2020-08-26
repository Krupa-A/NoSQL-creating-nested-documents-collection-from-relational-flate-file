# NoSQL-creating-nested-documents-collection-from-relational-flate-file

In this project, you will learn to use MongoDB as an example of a document-oriented NOSQL system, and see how data is stored and queried in such a system. You will also learn about the difference between storing data in a flat (relational) format versus in a document (complex object) JSON or XML format.

The input to your program will be the same data files in flat relational format as in Project 1 for the COMPANY database You will need to design two document (complex object) schemas corresponding to this data:

 

The PROJECTS document will include the following data about each PROJECT object (document): PNAME, PNUMBER, DNAME (for the controlling DEPARTMENT), and a collection of the workers (EMPLOYEES) who work on the project. This will be nested within the PROJECT object (document) and will include for each worker: EMP_LNAME, EMP_FNAME, HOURS.
The EMPLOYEES document will include the following data about each EMPLOYEE object (document): EMP_LNAME, EMP_FNAME, DNAME (department where the employee works), and a collection of the projects that the employee works on. This will be nested within the EMPLOYEE object (document) and will include for each project: PNAME, PNUMBER, HOURS.

input files are attached into input folder as well as output files in Output folder.

Programming Language: Python
Database : MongoDB

IMPORTANT:
To run this project, your machine needs this requirements:
pymongo, json, dicttoxml, pymongo.errors


+How to excute the code?
=> Project2_DB2.py
=>It will automatically generate the output files in the output folder.
=>This project is generate two Databases.
	1.DB2 to store the collection for txt files.
	2.Project2 to store the new created PROJECTS documents collection and EMPLOYEES documents collection.
  
  
program will generate XML files also from Json format.
