from flask import Flask, request, jsonify
import sqlite3
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy import inspect, create_engine
import os
import pandas as pd


app = Flask(__name__)

# Configures database file and folder for uploading csv files
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///migration.db'  # Change this to your actual database connection string
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = 'uploads'

db = SQLAlchemy(app)

# Creates tables with its atributes and relationships
class Department(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(255), nullable=True, unique=False)

class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(255), nullable=True, unique=False)

class Employee(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=True)
    datetime_todo_2 = db.Column(db.String(255), nullable=True)
    department_id = db.Column(db.Integer, db.ForeignKey('department.id'), nullable=True)
    job_id = db.Column(db.Integer, db.ForeignKey('job.id'), nullable=True)

# Checks if the tables exist in the database before creating them
with app.app_context():
    inspector = inspect(db.engine)
    for model in [Department, Job, Employee]:
        if not inspector.has_table(model.__tablename__):
            db.create_all()

# Hello World endpoint - test
@app.route('/hello', methods=['GET'])
def hello_world():
    return jsonify({'message': 'Hello, World!'})

#Uploads data to db
@app.route('/upload_data', methods=['POST'])
def upload_data():
    if 'file' not in request.files:
        return jsonify({'error': 'File not found'})

    file = request.files['file']

    if file.filename == '':
        return jsonify({'error': 'No selected file'})
    
    if file:
        # Saves the file to the uploads directory
        filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(filename)

    # Process the uploaded CSV file and insert into the database
        try:
            df = pd.read_csv(filename, sep=',', header=None)

            if 'departments' in filename:
                insert_departments(df)
            elif 'jobs' in filename:
                insert_jobs(df)
            elif 'employees' in filename:
                insert_employees(df)

            return jsonify({'success': 'Data uploaded and inserted successfully'})
        except Exception as e:
            return jsonify({'error': str(e)})

#For every function that inserts the file to its corresponding table,
#the headers are added to the csv files
def insert_departments(df):
    headers =  ["id", "department"]
    df.columns = headers
    #print(df)
    df.apply(lambda row: db.session.add(Department(department=row['department'])), axis=1)
    db.session.commit()

def insert_jobs(df):
    headers =  ["id", "job"]
    df.columns = headers
    df.apply(lambda row: db.session.add(Job(job=row['job'])), axis=1)
    db.session.commit()

def insert_employees(df):
    headers =  ["id", "name", "datetime_todo_2", "department_id", "job_id"]
    df.columns = headers

    # for foreign columns in employee table, we fill the blank values with zero
    df['department_id'] = df['department_id'].fillna(0).astype(int)
    df['job_id'] = df['job_id'].fillna(0).astype(int)

    batch_size = 1000
    rows_to_insert = []

    #inserts rows taking into account the limit of 1000 rows per request
    for _, row in df.iterrows():
        employee = Employee(name=row['name'], datetime_todo_2= row['datetime_todo_2'] , department_id=int(row['department_id']), job_id=int(row['job_id']))
        rows_to_insert.append(employee)

        if rows_to_insert:
            if len(rows_to_insert) <= batch_size:
                try:
                    db.session.add_all(rows_to_insert)
                    db.session.commit()
                except IntegrityError as e:
                    db.session.rollback()
                    raise e
                rows_to_insert = []

#RQ1 and RQ2 endpoints (SQL) 
@app.route('/rq1', methods=['GET'])
def rq1():

    db_url = 'sqlite:///C:/Users/garfe/Documents/gcc/instance/migration.db'
    engine = create_engine(db_url)
    try:
        # Connect to the SQLite database using SQLAlchemy
        connection = engine.connect()

        # Execute a select query and fetch the results into a DataFrame
        sql_query = """select 
                            department.department,
                            job.job,
                            count(CASE
                                WHEN strftime('%m', datetime_todo_2) BETWEEN '01' AND '03' THEN 'Q1'
                            END) as Q1,
                            count(CASE
                                WHEN strftime('%m', datetime_todo_2) BETWEEN '04' AND '06' THEN 'Q2'
                            END) as Q2,
                            count(CASE
                                WHEN strftime('%m', datetime_todo_2) BETWEEN '07' AND '09' THEN 'Q3'
                            END) as Q3,
                            count(CASE
                                WHEN strftime('%m', datetime_todo_2) BETWEEN '10' AND '12' THEN 'Q4'
                            END) as Q4
                        from employee
                        inner join job on employee.job_id = job.id
                        inner join department on employee.department_id = department.id
                        where 1=1
                        and strftime('%Y', datetime(datetime_todo_2)) = '2021'
                        group by department.id,
                                job.id
                        order by department.department,
                                job.job
                        """
        df = pd.read_sql_query(sql_query, connection) 

        print(df)
       
        if df.empty:
            return jsonify({'message' : 'No data available for given criteria'})

        # Convert the DataFrame to JSON and return
        result_json = df.to_json(orient='records')
        
        return jsonify({'success' : result_json})
    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        if connection:
            connection.close()
            print("The SQLite connection is closed")

@app.route('/rq2', methods=['GET'])
def rq2():

    db_url = 'sqlite:///C:/Users/garfe/Documents/gcc/instance/migration.db'
    engine = create_engine(db_url)
    try:
        # Connect to the SQLite database using SQLAlchemy
        connection = engine.connect()

        # Execute a select query and fetch the results into a DataFrame
        sql_query = """select department.id,
                            department.department,
                            count(employee.id) as hired
                        from employee
                        inner join department on employee.department_id = department.id
                        group by department.id,
                            department.department
                        having count(employee.id) > (select avg(number_empl) as Mean from (select department, 
                        count(employee.id) as number_empl
                        from employee
                        inner join department on employee.department_id = department.id
                        where 1=1
                        and strftime('%Y', datetime(datetime_todo_2)) = '2021'
                        group by department.id))
                        order by count(employee.id) desc
                        """
        df = pd.read_sql_query(sql_query, connection)

        print(df)
       
        if df.empty:
            return jsonify({'message' : 'No data available for given criteria'})
            
        # Convert the DataFrame to JSON and return
        result_json = df.to_json(orient='records')
        
        return jsonify({'success' : result_json})
    except Exception as e:
        return jsonify({'error': str(e)})

    finally:
        if connection:
            connection.close()
            print("The SQLite connection is closed")



if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)