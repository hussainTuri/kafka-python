CREATE database kafka;

use kafka;

CREATE TABLE customers (
id INT(10) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
customer_name VARCHAR(255) NOT NULL,
reg_date TIMESTAMP
);
