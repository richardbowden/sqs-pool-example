#!/bin/bash
psql -h $PG_HOST -p $PG_PORT -U $PG_SUPERUSER -c "CREATE DATABASE $DB_NAME;"

# Create the new role without superuser privileges
psql -h $PG_HOST -p $PG_PORT -U $PG_SUPERUSER -d $DB_NAME -c "CREATE ROLE $NEW_USERNAME WITH LOGIN PASSWORD '$NEW_USER_PASSWORD';"

# Grant superuser privileges to the new role
psql -h $PG_HOST -p $PG_PORT -U $PG_SUPERUSER -d $DB_NAME -c "ALTER ROLE $NEW_USERNAME WITH SUPERUSER;"

# Grant connect privileges to the database
psql -h $PG_HOST -p $PG_PORT -U $PG_SUPERUSER -d $DB_NAME -c "GRANT CONNECT ON DATABASE $DB_NAME TO $NEW_USERNAME;"

echo "User $NEW_USERNAME has been created with superuser privileges and can now connect to $DB_NAME."

psql $DATABASE_URL -f sql/schema.sql
