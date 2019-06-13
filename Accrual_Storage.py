#!/usr/bin/env python
# coding: utf-8

# In[28]:


# Points Accrual and Storage System
# Make code python 2 and 3 compliant
from __future__ import print_function

import datetime
import pandas as pd

# Make things look pretty
from pprint import pprint

# For the database
import sqlite3


# In[16]:


class Db(object):    
    """The database class."""

    def __init__(self, database='fenix.db', statements=[]):
        """Initialize a new database or connect to an existing one."""

        self.database = database  #the database filename
        self.statement = ''       #holds incomplete statements
        
        self.connect()
        self.execute(statements)
        self.close()            

    def connect(self):
        """Connect to the database."""

        self.connection = sqlite3.connect(self.database)
        self.cursor = self.connection.cursor()
        self.connected = True
        self.statement = ''

    def close(self): 
        """Close the database."""

        self.connection.commit()
        self.connection.close()
        self.connected = False

    def incomplete(self, statement):
        """Concatenate clauses until a complete statement is made."""

        self.statement += statement
        if self.statement.count(';') > 1:
            self.statement = ''
            raise ValueError('One statement at a time, please!.')
            
        return not sqlite3.complete_statement(self.statement)
    
    def execute(self, statements):
        """Executes SQL statements. """

        queries = []
        close = False
        if not self.connected:                  
            self.connect() 
            close = True
        if type(statements) == str:
            statements = [statements]
        for statement in statements:
            if self.incomplete(statement):
                continue #the statement is complete
            try:
                statement = self.statement.strip() #reset the test statement
                self.statement = ''
                self.cursor.execute(statement) # Execute statement
                data = self.cursor.fetchall()
                if statement.upper().startswith('SELECT'):
                    queries.append(data)

            except sqlite3.Error as error:
                raise ValueError('An error occurred:', error.args[0])

        if close:
            self.close()   #Close database if opened here.
        
        if queries:
            return queries
        
        
    def from_csv(self,table,filename):
        """Imports data from a csv file to a selected table."""
        
        if not self.connected:                  
            self.connect() 
            close = True
            
        data = pd.read_csv(filename)
        data.to_sql(table,self.connection,if_exists='append',index=False)
        
        if close:
            self.close() 


# In[17]:


class Employee(object):
    """Defines an employee's required attributes."""
    
    def __init__(self, ID):
        """
            Create an instance of an employee.
            
            Arguments
            id - <int> - This is the employee's ID.
        """
        self.id = ID
        
    def get_id(self):
        return self.id
    
    def get_attribute(self,attribute):
        """Returns the selected attributed value."""
                  
        statement = ['SELECT {} FROM employees WHERE emp_id={};'.format(attribute, self.get_id())]
        attribute = db.execute(statement)[0][0][0] # What?!
        return attribute
    
    
    def request(self,amount,signoff=False):
        """Enables an employee to request points withdrawal."""
        
        # Check points balance
        statement = ['SELECT points FROM employees WHERE emp_id={};'.format(self.get_id())]
        points_balance = db.execute(statement)[0][0][0]
        
        if (points_balance > amount):
            if signoff:
                # Send expenses request to finance
                statement = ['INSERT INTO requests (emp_id,withdrawal) VALUES ({},{});'.format(self.get_id(),amount)]
                db.execute(statement)
                req_id = db.execute(['SELECT req_id FROM requests WHERE emp_id={} and withdrawal={} and status=0;'.format(self.get_id(),amount)])
                return req_id[0][0][0]
            else:
                print('Manager sign off required.')
        else:
            raise ValueError('Insufficient Points.')       


# In[25]:


class Accrual(object):
    """This class performs the points accrual and tracking."""
    
    def __init__(self):
        self.today = f"{datetime.date.today():%Y/%m/%d}"
        
        
    def points_multiplier(self,points,tenure):
        """Multiples the points as per the tenure"""
        if (tenure > 0):
            if   (0.0 < tenure < 2.0):
                points *= 1.0
            elif (2.0 <= tenure < 4.0):
                points *= 1.25
            else:
                points *= 1.5
        return points
            
    def seniority_points(self, seniority):
        """Returns the number of points per month given the seniority."""
        
        points =  None
        
        if seniority   == 'A':
            points=5
        elif seniority == 'B':
            points=10
        elif seniority == 'C':
            points=15
        elif seniority == 'D':
            points=20
        elif seniority == 'E':
            points=25
        else:
            points = 0
            
        return points
    
    def time_difference(self,start_date,end_date,flag=False):
        """
            Returns the number of months
            
            Arguments
            start_date - <string> - The date of reference at the start of the calculation period.
            end_date - <string> - The date of reference at the end of the calculation period
            
            flag - <bool> - if false, return number of months (tenure),
                            if true return number of firsts (first of each month)
        """
        
        start_date = datetime.datetime.strptime(str(start_date), '%Y/%m/%d')
        end_date = datetime.datetime.strptime(str(end_date), '%Y/%m/%d')
        months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
        
        # Is it the first of the month? How many firsts?
        if flag:
            if start_date.day == 1 & end_date.day > 1:
                months +=1
            elif start_date.day > 1 & end_date.day == 1:
                months -=1
            return months
        else:    
            return months
        
    def points_accrual(self,ID):
        """
            Calculates the number of points for a given employee as per today.
            
            Arguments
            id - <int> - The employee id
        """
        
        statement = ['SELECT start_date FROM history where emp_id={};'.format(ID)]
        results = db.execute(statement)[0]
        
        dates = []      # Unpack dates.
        for date in results:
            dates.append(date[0])
            
        dates.sort(key=lambda date: datetime.datetime.strptime(date,'%Y/%m/%d'))
        
        n = 0;
        points = [];
        
        while(n < (len(dates)-1)):
            
            months = self.time_difference(dates[n],dates[n+1],True)
            tenure1 = self.time_difference(dates[0],dates[n+1]) # Tenure as of dates[n+1]
            temp = self.calculator_helper(dates[n],ID, months,tenure1)
            points.append(temp)
            n = n+1
            
        # Points up to today.
        months = self.time_difference(dates[-1],self.today)
        tenure2 = self.time_difference(dates[0],self.today) # Tenure as of  today
        temp = self.calculator_helper(dates[-1],ID,months,tenure2)
        points.append(temp)
        
        points = sum(points) # Sum up the points (All Points)
        
        # Current points = All points - Used points
        statement = ['SELECT withdrawal FROM requests where emp_id={} and status={};'.format(ID,1)]
        withdrawals = db.execute(statement)[0]
        
        used_points = []
        
        # Unpack points
        for withdrawal in withdrawals:
            used_points.append(withdrawal[0])
            
        used_points = sum(used_points) # Sum of all approved withdrawals
        
        if used_points:
            points = points - used_points
            
        # Update employees table.
        statement = ['UPDATE employees SET points={} WHERE emp_id={};'.format(points,ID)]
        db.execute(statement)
            
        return points
    
    def calculator_helper(self,date,ID,months,tenure):
        """Calculates points per month"""
    
        statement = ['SELECT seniority FROM history where start_date=\'{}\' and emp_id={};'.format(date,ID)]
        seniority = db.execute(statement)[0][0][0]
        sen_points = self.seniority_points(seniority)
        
        points_per_month = sen_points*months
        
        # Calculate tenure
        tenure /= 12
        temp = self.points_multiplier(points_per_month,tenure)
        
        return temp
    
    def balance_deduction(self,ID,req_id,approved=False):
        """
            Deducts the used points, and updates the points balance when the request is approved,
            status is changed to 1 (True) in the database when approved and the used points are deducted.
            
            Arguments
            ID     - <int> - employee ID
            req_id - <int> - request id in the database
            
            Optional
            approved - <bool> - True when approved, false if pending,or 
                                not approved (trigger for the balance deduction).
        """
        
        if approved:
            statement = ['SELECT withdrawal FROM requests where emp_id={} and req_id={};'.format(ID,req_id)]
            deduction = db.execute(statement)[0][0][0]
            
            statement = ['SELECT points FROM employees WHERE emp_id={};'.format(ID)]
            points = db.execute(statement)[0][0][0]
            
            print(points)
            print(deduction)
            
            new_points = points - deduction
            # Update employees table.
            statement = ['UPDATE employees SET points={} WHERE emp_id={};'.format(new_points,ID),
                        'UPDATE requests SET status=1 WHERE emp_id={} and req_id={};'.format(ID,req_id)]
            db.execute(statement)
            
            # Expense request creation sent to finance
            
    def points_used(self, ID):
        """Check the number of points used for a given employee."""
        
        statement = ['SELECT withdrawal FROM requests where emp_id={} and status=1;'.format(ID)]       
        results = db.execute(statement)[0]
        
        withdrawals = []      # Unpack dates.
        for withdrawal in results:
            withdrawals.append(withdrawal[0])
            
        withdrawals = sum(withdrawals)
        
        return withdrawals


# In[19]:


# Create tables(run only once)
a = ('create table if not exists employees(emp_id INTEGER PRIMARY KEY, seniority TEXT,start_date DATE, points FLOAT DEFAULT 0.0 NOT NULL);') 
b = ('create table if not exists history(hist_id INTEGER PRIMARY KEY,emp_id INTEGER, start_date DATE,seniority TEXT, FOREIGN KEY(emp_id) REFERENCES employees (emp_id) ON DELETE CASCADE ON UPDATE NO ACTION);')
c = ('create table if not exists requests(req_id INTEGER PRIMARY KEY,emp_id INTEGER, withdrawal FLOAT,status INTEGER DEFAULT 0,FOREIGN KEY(emp_id) REFERENCES employees (emp_id) ON DELETE CASCADE ON UPDATE NO ACTION);')

statements = [a,b,c]
db = Db('Fenix.db',statements)


# In[6]:


# Reading from a csv file, Populating tables (run only once)
db.from_csv('history','History.csv') 
db.from_csv('employees','Employee.csv')


# In[7]:


# Reading data from the database
statement = ['SELECT * FROM employees;']
results = db.execute(statement)[0]
results


# In[26]:


# Points Accrual
acc = Accrual()
acc.points_accrual(1) # For Employee 1
acc.points_accrual(2) # For Employee 2
acc.points_accrual(3) # For Employee 3
acc.points_accrual(4) # For Employee 4
acc.points_accrual(5) # For Employee 5


# In[27]:


statement = ['SELECT * FROM employees;'] # Points per employee
results = db.execute(statement)[0]
results


# In[21]:


statement = ['SELECT * FROM requests;']
results = db.execute(statement)[0]
results


# In[11]:


# Instantiate Enployee
employee1 = Employee(1)


# In[12]:


# An employee privately querying their own points balance
employee1.get_attribute("points")


# In[13]:


# Withdrawal Request
req_id = employee1.request(10,True)
acc.balance_deduction(2,req_id)


# In[14]:


# Update in requests table
statement = ['SELECT * FROM requests;']
results = db.execute(statement)
results

