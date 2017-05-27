# kafka-python
## Producer
###### From standard input
    cd /kafka-python/producer
     
    # Script will send each line you type as message to topic 'customers'. type exit after last line. 
    python producer.py -c 
    
###### From file
    #  
    python producer.py -f /kafka-python/resources/customers

###### From database (Mysql)
    # assuming you have created database, table and have some data in there. check out sql file in resources
    # This will write all the customer names to kafka 'customers' topic. 
    python producer.py -d
    