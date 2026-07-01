#!/bin/bash
# Enable archivelog and supplemental logging for OLR.
# Runs during gvenzl/oracle-free container init.

sqlplus -S / as sysdba <<'SQL'
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER SYSTEM SET db_recovery_file_dest_size=10G;
ALTER SYSTEM SET log_archive_dest_1='LOCATION=/opt/oracle/oradata/FREE/archive' SCOPE=BOTH;
HOST mkdir -p /opt/oracle/oradata/FREE/archive

-- Grant flashback and v$database access for dblog chunk reads
ALTER SESSION SET CONTAINER=FREEPDB1;
GRANT SELECT ANY TABLE TO testuser;
GRANT FLASHBACK ANY TABLE TO testuser;
GRANT SELECT ON SYS.V_$DATABASE TO testuser;

-- Create test table
CREATE TABLE testuser.ORDERS (
    ID NUMBER(10) PRIMARY KEY,
    AMOUNT NUMBER(10,2),
    STATUS VARCHAR2(50)
);

-- Enable supplemental logging on the test table
ALTER TABLE testuser.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
SQL
