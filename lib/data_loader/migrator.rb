module DataLoader

  class Migrator
    def self.migrate(file, columns, table, separator = ',', conn = :root, local = false, row_sep = "\r\n")
      with_connection(conn) do
        create_schema(table, columns)
        puts "-- load_data('#{File.basename(file)}', :#{table.to_s})"
        load_data(file, table, local, separator, row_sep)
      end
    end

    # takes a column,type data structre and makes a table
    def self.create_schema(table_name, data_struct)
      ActiveRecord::Schema.define do
        create_table table_name, :force => true, :id => false do |t|
          data_struct.each do |column|
            t.column(column[:name], column[:type])
          end
        end
      end
    end

    # uses MySQL LOAD DATA to import the whole file, ignoring the header line
    def self.load_data(file, table_name, local, separator = ',', row_sep = "\r\n")
      local_txt = local ? "LOCAL" : ''
      sql = <<-SQL
        LOAD DATA #{local_txt} INFILE '#{file}' INTO TABLE #{table_name.to_s}
          FIELDS TERMINATED BY '#{separator}' ENCLOSED BY '"'
          LINES TERMINATED BY '#{row_sep}'
          IGNORE 1 LINES;
      SQL
      ActiveRecord::Base.connection.execute(sql)
    end

    # runs a block under a different connection from database.yml
    def self.with_connection(conn = :root)
      if Rails.env.development?
        yield
      else
        ActiveRecord::Base.establish_connection(conn)
        yield
        ActiveRecord::Base.establish_connection(RAILS_ENV)
      end
    end

    # a pretty table name
    def self.derive_table_name(file)
      name = File.basename(file, File.extname(file))  # just file
      name.underscore.sub(/[0-9_]*$/, '')      # remove trailing numbers
    end
  end
end
