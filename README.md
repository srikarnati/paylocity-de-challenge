# Paylocity Coding Challenge

Coding Challenge project for Data Engineer

### Summary
 Python program to get the final state based on the timestamp based DML operations 

### Problem Statement
Read JSON File with different models(Employee,Company,Position,Job) and identify the final state of record:
-   
-   source_table identifies model
-   Final state can identified using source_table,guid,action and timestamp


### Solution
Using command-line menu based approach, program will take user input of Employee's first + last name and number of dependents (first + last name) per employee.

Factoring in Employee + dependents' names (discount condition mentioned above), program calculates total cost of benefits for all employees + their dependents entered in session before exiting program.

### Explanation/Reasoning of Approach
I chose Pyspark as it can be scalable for a huge dataset on distributed cluster.
For smaller dataset it can be achieved using pandas

#### Planning
Divided the code into 3 parts 
-Reading Data
-Processing Data
-Printing Data in Required Format


## Environment SetupRequirements
-   Python 3.9
-   Spark 3.0
-   Java 8


## Installation

Use the package manager PIP to install required Python packages.

