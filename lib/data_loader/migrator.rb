module DataLoader

  class Migrator
    def self.migrate(file, columns, table, separator = ',', conn = :root, local = false, row_sep = "\r\n")
      with_connection(conn) do
        create_schema(table, columns)
        puts "-- load_data('#{File.basename(file)}', :#{table.to_s})"
        load_data(file, table, local, separator, row_sep)
        nullify_dates(table, columns)
        trim_strings(table, columns)
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

    # empty strings import as 0000-00-00 00:00:00, convert to nil
    def self.nullify_dates(table_name, data_struct)
      date_columns = data_struct.map {|column| column[:name] if [:datetime, :date].include?(column[:type]) }.compact
      date_columns.each do |column|
        sql = <<-SQL
          UPDATE #{table_name}
          SET #{column} = NULL
          WHERE #{column} = 0
        SQL
        ActiveRecord::Base.connection.execute(sql)
      end
    end


    def self.trim_strings(table_name, data_struct)
      # strings but not text
      string_columns = data_struct.map {|column| column[:name] if column[:type] == :string }.compact
      return if string_columns.empty?

      case_sql = string_columns.map do |column|
        %Q{
          `#{column}` = CASE WHEN CHAR_LENGTH(TRIM(`#{column}`)) = 0 THEN
            NULL
          ELSE
            TRIM(`#{column}`)
          END
        }
      end.join(', ')

      sql = <<-SQL
        UPDATE #{table_name}
        SET #{case_sql}
      SQL
      ActiveRecord::Base.connection.execute(sql)
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
