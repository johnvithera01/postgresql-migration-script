require 'open3' # To execute external commands and capture output/errors
require 'etc'   # To detect local CPU cores

# --- Connection Configurations (ATTENTION: HANDLE PASSWORDS CAREFULLY!) ---
# IT IS HIGHLY RECOMMENDED TO USE ENVIRONMENT VARIABLES OR SECURE CONFIGURATION FILES
# Example environment variable: ENV['PG_SOURCE_PASSWORD']
SOURCE_HOST = '192.168.200.14'
SOURCE_PORT = 5432
SOURCE_USER = 'joaovictor'
SOURCE_PASSWORD = ENV['PG_SOURCE_PASSWORD'] 

TARGET_HOST = '192.168.200.7'
TARGET_PORT = 5432
TARGET_USER = 'joaovictor'
TARGET_PASSWORD = ENV['PG_TARGET_PASSWORD'] 

# --- Databases to Be Migrated ---
# List the names of the databases you want to migrate here.
# DO NOT include 'template0', 'template1', or 'postgres' (unless that's your specific goal).
DATABASES_TO_MIGRATE = [
  'dw_producao'
]

# --- Parallelism Configuration ---
# Set the number of parallel jobs for pg_dump/pg_restore.
# If set to nil, the script will try to use half of the detected CPU cores ON THE MACHINE WHERE THE SCRIPT IS RUNNING.
# Ex: PG_JOBS_OVERRIDE = 4  (will use 4 jobs)
# Ex: PG_JOBS_OVERRIDE = nil (will detect and use half the cores)
PG_JOBS_OVERRIDE = nil

# --- Role Import Option ---
# Set to true to import roles (users) from the source to the target.
# Set to false to skip role import. Useful if roles are managed separately or already exist.
IMPORT_ROLES = true

# --- Function to Execute Shell Commands and Log ---
def run_command(command, log_file = nil, env = {})
  puts "Executing: #{command}"
  File.open(log_file, 'a') { |f| f.puts "Executed command: #{command}\n" } if log_file

  Open3.popen3(env, command) do |stdin, stdout, stderr, wait_thr|
    stdout_reader = Thread.new {
      stdout.each_line do |line|
        puts "STDOUT: #{line}"
        File.open(log_file, 'a') { |f| f.puts "STDOUT: #{line}" } if log_file
      end
    }
    stderr_reader = Thread.new {
      stderr.each_line do |line|
        puts "STDERR: #{line}"
        File.open(log_file, 'a') { |f| f.puts "STDERR: #{line}" } if log_file
      end
    }

    stdout_reader.join
    stderr_reader.join

    exit_status = wait_thr.value
    unless exit_status.success?
      raise "Command failed with status #{exit_status.exitstatus}: #{command}"
    end
  end
end

puts "--- PostgreSQL Migration Start ---"
puts "Source Host: #{SOURCE_HOST}:#{SOURCE_PORT} (User: #{SOURCE_USER})"
puts "Target Host: #{TARGET_HOST}:#{TARGET_PORT} (User: #{TARGET_USER})"
puts "Databases to Migrate: #{DATABASES_TO_MIGRATE.join(', ')}"
puts ""

# Define a general log for role operations
general_log_file = "migration_general_log_#{Time.now.strftime('%Y%m%d_%H%M%S')}.log"

# --- Determine the number of parallel jobs ---
PG_JOBS = if PG_JOBS_OVERRIDE
            PG_JOBS_OVERRIDE
          else
            # Detect CPU cores on the machine where the script is running
            num_cores = Etc.nprocessors
            [1, (num_cores / 2).to_i].max # At least 1 job, or half the cores
          end
puts "Using #{PG_JOBS} parallel jobs for dump/restore."
File.open(general_log_file, 'a') { |f| f.puts "Using #{PG_JOBS} parallel jobs for dump/restore.\n" }

# --- 1. Copy Roles (Users) from Source (Optional) ---
if IMPORT_ROLES
  puts "## Copying Roles (Users) from Source Server..."
  begin
    # Configure environment variables for pg_dumpall connection
    env_vars_source_for_roles = {
      'PGPASSWORD' => SOURCE_PASSWORD
    }
    env_vars_target_for_roles = {
      'PGPASSWORD' => TARGET_PASSWORD
    }
    
    # Generate the role dump from the source (users/groups only, no passwords!)
    # The --no-role-passwords option is important for security.
    # You'll need to set new passwords for the users on the target.
    roles_dump_file = "roles_dump_#{SOURCE_HOST}.sql"
    command_dump_roles = "pg_dumpall -h #{SOURCE_HOST} -p #{SOURCE_PORT} -U #{SOURCE_USER} --roles-only --no-comments --no-role-passwords > #{roles_dump_file}"
    run_command(command_dump_roles, general_log_file, env_vars_source_for_roles)
    
    puts "Source roles saved to #{roles_dump_file}"
    File.open(general_log_file, 'a') { |f| f.puts "Source roles saved to #{roles_dump_file}\n" }

    # Restore roles on the target
    command_restore_roles = "psql -h #{TARGET_HOST} -p #{TARGET_PORT} -U #{TARGET_USER} -f #{roles_dump_file} postgres"
    run_command(command_restore_roles, general_log_file, env_vars_target_for_roles)
    puts "Roles restored on the target server."
    File.open(general_log_file, 'a') { |f| f.puts "Roles restored on the target server.\n" }

  rescue => e
    puts "ERROR copying/restoring roles: #{e.message}"
    File.open(general_log_file, 'a') { |f| f.puts "ERROR copying/restoring roles: #{e.message}\n" }
    exit 1
  ensure
    # Clean up the roles dump file
    File.delete(roles_dump_file) if File.exist?(roles_dump_file)
    puts "Roles dump file #{roles_dump_file} removed."
  end
else
  puts "## Skipping Role (User) Import as per configuration."
  File.open(general_log_file, 'a') { |f| f.puts "Skipping Role (User) Import as per configuration.\n" }
end

puts "\n--- Database Migration ---\n"

DATABASES_TO_MIGRATE.each do |db_name|
  puts "## Migrating database: #{db_name}"
  
  # Define file and directory names for the parallel dump
  dump_dir = "#{db_name}_dump_dir" # Directory pg_dump -Fd will create
  compressed_dump_file = "#{db_name}_dump.tar.gz" # .tar.gz file for transfer
  
  db_log_file = "#{db_name}_migration_log_#{Time.now.strftime('%Y%m%d_%H%M%S')}.log"

  # --- Configure environment variables for passwords ---
  env_vars_source = {
    'PGPASSWORD' => SOURCE_PASSWORD
  }
  env_vars_target = {
    'PGPASSWORD' => TARGET_PASSWORD
  }

  begin
    # --- 1. Create Database on Target (if not exists) ---
    puts "Checking/Creating database '#{db_name}' on the target..."
    File.open(db_log_file, 'a') { |f| f.puts "Starting migration for database: #{db_name}\n" }
    # Adding DROP DATABASE IF EXISTS for idempotency in case of re-execution
    # CAUTION: This will delete the database and all its content on the target before restoring.
    run_command("psql -h #{TARGET_HOST} -p #{TARGET_PORT} -U #{TARGET_USER} -c \"DROP DATABASE IF EXISTS #{db_name};\" postgres", db_log_file, env_vars_target)
    create_db_command = "psql -h #{TARGET_HOST} -p #{TARGET_PORT} -U #{TARGET_USER} -c \"CREATE DATABASE #{db_name};\" postgres"
    run_command(create_db_command, db_log_file, env_vars_target)
    puts "Database '#{db_name}' ready on the target."
    File.open(db_log_file, 'a') { |f| f.puts "Database '#{db_name}' ready on the target.\n" }

    # --- 2. Generate Dump from Source Database (with parallelism) ---
    puts "Generating dump of '#{db_name}' on #{SOURCE_HOST} with #{PG_JOBS} parallel jobs..."
    File.open(db_log_file, 'a') { |f| f.puts "Generating dump of '#{db_name}' with parallelism...\n" }
    
    # Remove previous dump directory if it exists
    run_command("rm -rf #{dump_dir}", db_log_file) if Dir.exist?(dump_dir)

    # pg_dump command with directory format (-Fd) and parallel jobs (-j)
    command_dump = "pg_dump -h #{SOURCE_HOST} -p #{SOURCE_PORT} -U #{SOURCE_USER} -Fd -j #{PG_JOBS} -f #{dump_dir} #{db_name}"
    run_command(command_dump, db_log_file, env_vars_source)
    puts "Dump of '#{db_name}' saved to directory #{dump_dir}"
    File.open(db_log_file, 'a') { |f| f.puts "Dump of '#{db_name}' saved to directory #{dump_dir}\n" }

    # --- 3. Compress the Dump Directory ---
    puts "Compressing dump directory #{dump_dir} to #{compressed_dump_file}..."
    File.open(db_log_file, 'a') { |f| f.puts "Compressing dump directory...\n" }
    # Remove previous .tar.gz file if it exists
    run_command("rm -f #{compressed_dump_file}", db_log_file) if File.exist?(compressed_dump_file)
    # Compress the directory
    run_command("tar -czf #{compressed_dump_file} #{dump_dir}", db_log_file)
    puts "Dump directory compressed."
    File.open(db_log_file, 'a') { |f| f.puts "Dump directory compressed.\n" }

    # --- 4. Restore Dump on Target Database (with parallelism) ---
    puts "Decompressing #{compressed_dump_file}..."
    File.open(db_log_file, 'a') { |f| f.puts "Decompressing dump...\n" }
    # Remove previous dump directory if it exists
    run_command("rm -rf #{dump_dir}", db_log_file) if Dir.exist?(dump_dir)
    # Decompress the file
    run_command("tar -xzf #{compressed_dump_file}", db_log_file)
    puts "Dump file decompressed to #{dump_dir}."
    File.open(db_log_file, 'a') { |f| f.puts "Dump file decompressed to #{dump_dir}.\n" }

    puts "Restoring dump of '#{db_name}' on #{TARGET_HOST} with #{PG_JOBS} parallel jobs. See progress in log: #{db_log_file}"
    File.open(db_log_file, 'a') { |f| f.puts "Restoring dump of '#{db_name}' with parallelism...\n" }
    # pg_restore command with parallel jobs (-j)
    command_restore = "pg_restore -h #{TARGET_HOST} -p #{TARGET_PORT} -U #{TARGET_USER} -j #{PG_JOBS} -d #{db_name} #{dump_dir}"
    run_command(command_restore, db_log_file, env_vars_target)
    puts "Database '#{db_name}' migrated successfully!"
    File.open(db_log_file, 'a') { |f| f.puts "Database '#{db_name}' migrated successfully!\n" }

  rescue => e
    puts "ERROR migrating database '#{db_name}': #{e.message}"
    File.open(db_log_file, 'a') { |f| f.puts "ERROR migrating database '#{db_name}': #{e.message}\n" }
    # In this example, it will continue to the next database, but you can change it to `exit 1` to stop everything.
  ensure
    # --- 5. Clean up temporary dump files and directories ---
    puts "Cleaning up temporary files for '#{db_name}'..."
    File.delete(compressed_dump_file) if File.exist?(compressed_dump_file)
    run_command("rm -rf #{dump_dir}", db_log_file) if Dir.exist?(dump_dir) # Remove the entire directory
    puts "Temporary files for '#{db_name}' removed."
    File.open(db_log_file, 'a') { |f| f.puts "Temporary files for '#{db_name}' removed.\n" }
  end
  puts "\n---\n"
end

puts "--- PostgreSQL Migration Completed! ---"
File.open(general_log_file, 'a') { |f| f.puts "--- PostgreSQL Migration Completed! ---\n" }