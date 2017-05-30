# kafka-python
## Producer
###### From standard input
    cd /kafka-python/producer
     
    # Script will send each line you type as message to topic 'customers'. type exit after last line. 
    python producer.py -c 
    
###### From file
    # Read file line by line and send it to kafka 
    python producer.py -f /kafka-python/resources/customers

###### From database (Mysql)
    # assuming you have created database, table and have some data in there. check out sql file in resources
    # This will write all the customer names to kafka 'customers' topic. 
    python producer.py -d
    
    

## Consumer
###### To standard input
    cd /kafka-python/consumer
     
    # Read new messages from Kafka and print them on screen
    python producer.py -c 
    
###### To file
    # Read new messages from Kafka and append that to file.
    python producer.py -f

###### To database (Mysql)
    # Read new messages from Kafka and dump them to db
    python producer.py -d    