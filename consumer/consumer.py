from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
import sys
from sqlalchemy import create_engine,MetaData, Table, Column, VARCHAR, Integer, BIGINT
import json

from sqlalchemy.sql.schema import ForeignKey


conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "foo",
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)
running = True

# adding data to mysqldb

engine = create_engine("mysql+mysqlconnector://yaambs:Qwert1234@0.0.0.0:3306/employee_db", echo = True)
meta = MetaData()
connection = engine.connect()

Table(
    'employee', meta,
    Column('emp_id', Integer, primary_key=True),
    Column('firstName', VARCHAR(50)),
    Column('lastName', VARCHAR(50)),
    Column('gender', VARCHAR(11)),
    Column('age', Integer),
    Column('streetAddress', VARCHAR(30)),
    Column('city', VARCHAR(20)),
    Column('state', VARCHAR(20)),
    Column('postalCode', BIGINT())
)
Table(
    'employee_contact', meta,
    Column('cont_id', Integer,primary_key=True, autoincrement=True),
    Column('emp_id', Integer, ForeignKey("employee.emp_id")),
    Column('type', VARCHAR(10)),
    Column('number', VARCHAR(10))
)
meta.create_all(engine)
    
def msg_process(msg):
    consumed_data = {}
    try:
        last_id_sql = connection.execute("select max(emp_id) from employee")
        last_id = last_id_sql.all()[0][0]
    except ConnectionError:
        print("unable to connect to database.")
    consumed_data[msg.key().decode('utf-8')] = msg.value().decode('utf-8')
    emp_details_dict = {}
    emp_phone_dict = {}
    if last_id is None:
        emp_details_dict['emp_id'] = 1
        emp_phone_dict['emp_id'] = 1
    else:
        print(emp_details_dict)
        last_id +=1
        emp_details_dict['emp_id'] = last_id
        emp_phone_dict['emp_id'] = last_id
    try:
        for k, v in json.loads(consumed_data['data']).items():
            if type(v)==dict:
                for key, value in v.items():
                    emp_details_dict[key]=value
            elif type(v)==list:
                continue
            else:
                if k=='firstName' and v=="":
                    return "First name cannot be empty."
                emp_details_dict[k]=v
    except AttributeError:
        print("String don't have items attribute.")
    details_columns = ', '.join("`"+str(x)+"`" for x in emp_details_dict.keys())
    details_values = ', '.join("'"+str(x)+"'" for x in emp_details_dict.values())
    insert_sql1 = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('employee', details_columns, details_values)
    connection.execute(insert_sql1)

    try:
        for k, v in json.loads(consumed_data['data']).items():
            if type(v)==list:
                for i in range(len(v)):
                    for key, value in v[i].items():
                        emp_phone_dict[key]=value
                    contact_columns = ', '.join("`"+str(x)+"`" for x in emp_phone_dict.keys())
                    contact_values = ', '.join("'"+str(x)+"'" for x in emp_phone_dict.values())
                    insert_sql2 = "INSERT INTO %s ( %s ) VALUES ( %s );" % ('employee_contact', contact_columns, contact_values)
                    connection.execute(insert_sql2)
    except AttributeError:
        print("String don't have items attribute.")

# query = connection.execute("select * from employee order by emp_id desc")
# print(query.all()[0])
# consuming data from kafka 
  
def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(1)
            if msg is None: break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    except KeyboardInterrupt as e:
        print(e)
    except KafkaException:
        print("Unknown partition provided")


if __name__ == "__main__":
    basic_consume_loop(consumer, ["employee"])