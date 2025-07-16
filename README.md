# postgresql-migration-script
A concise PostgreSQL database migration script written in Ruby, supporting parallel pg_dump/pg_restore and optional role (user) migration.
## how to use
ruby PG_SOURCE_PASSWORD="your_source_db_password" PG_TARGET_PASSWORD="your_target_db_password" ruby pg_migrate.rb
