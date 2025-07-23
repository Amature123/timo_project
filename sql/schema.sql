CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT PRIMARY KEY, --cccd/passport number
    full_name VARCHAR(50) NOT NULL, 
    year_of_birth INT NOT NULL CHECK (year_of_birth >= 1900 AND year_of_birth <= EXTRACT(YEAR FROM CURRENT_DATE)),
    phone VARCHAR(15) NOT NULL UNIQUE,
    email VARCHAR(100) UNIQUE,
    nation VARCHAR(50) NOT NULL CHECK (nation IN ('Vietnam', 'USA', 'Japan', 'Korea', 'China'))
);
CREATE TABLE IF NOT EXISTS accounts (
    account_id BIGINT PRIMARY KEY, 
    customer_id BIGINT NOT NULL,
    account_type VARCHAR(20) NOT NULL CHECK (account_type IN ('personal', 'business', 'savings')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    balance NUMERIC(12, 2) NOT NULL CHECK (balance >= 0),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS devices (
    device_id SERIAL PRIMARY KEY,
    os VARCHAR(30) NOT NULL,
    customer_id INT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL,
    device_id INT NULL,
    transaction_type VARCHAR(10) NOT NULL CHECK (transaction_type IN ('deposit', 'withdrawal')),
    transaction_log TEXT,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id) ON DELETE CASCADE,
    FOREIGN KEY (device_id) REFERENCES devices(device_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS authentication_logs( 
    log_id SERIAL PRIMARY KEY,
    otp VARCHAR(6) NOT NULL,
    customer_id INT NOT NULL,
    login_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    device_name VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS transaction_risks (
    risk_id SERIAL PRIMARY KEY,
    transaction_id INT NOT NULL,
    risk_score INT CHECK (risk_score BETWEEN 0 AND 100), 
    risk_level VARCHAR(10) CHECK (risk_level IN ('low', 'medium', 'high')),
    reason TEXT,
    flagged BOOLEAN DEFAULT FALSE,
    evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id) ON DELETE CASCADE
);
