-- End to end database
CREATE DATABASE ete;

-- Vehicles
CREATE TABLE IF NOT EXISTS ete.vehicles (
    id CHAR(100),
    lat FLOAT,
    lng FLOAT,
    at DATETIME
);

-- Operating periods
CREATE TABLE IF NOT EXISTS ete.operating_periods (
    id CHAR(100),
    start DATETIME,
    finish DATETIME
);

-- Events
CREATE TABLE IF NOT EXISTS ete.events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_type ENUM('create', 'update', 'delete', 'register', 'deregister'),
    entity_type ENUM('vehicle', 'operating_period'),
    at DATETIME,
    organization_id VARCHAR(50),
    vehicle_id CHAR(100),
    operating_period_id CHAR(100)
);