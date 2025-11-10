-- Crea base y usuario para el Metastore
CREATE DATABASE IF NOT EXISTS metastore_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'hive'@'%' IDENTIFIED BY 'hivepass';
GRANT ALL PRIVILEGES ON metastore_db.* TO 'hive'@'%';
FLUSH PRIVILEGES;
