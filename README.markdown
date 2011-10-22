## Data Loader

Data Loader is a tool to load CSV files into a MySQL database. It was designed
to import raw data into tables that could then be manipulated with SQL.

Features:

* Uses MySQL LOAD DATA to efficiently load very large files
* Fastercsv is used to inspect the first few rows and choose datatypes
* Datatypes can be overridden (types are :text, :string, :datetime, :integer)
* Converts header row in to nice ruby-esque column names
* Builds a schema using ActiveRecord
* If table names are unspecified, they will be derived from the file name
* Will prefix table names to avoid collisions (it overwrites existing tables)
* Can run under a different connection, as defined in your database.yml
* Appends data structure to `data_loader.textile`. Put this file in version control to see the differences when your source CSV files change.

### Usage

    # Configure (everything has defaults, see loader.rb)
    loader = DataLoader::Loader.new do |config|
      config.table_prefix = :import
      config.folder = 'path/to/csv/files/'
      config.inspect_rows = 10
      config.connection = :development
      config.separator = ','
      config.default_ext = 'csv'
      config.use_local = true
    end

    # Load data
    loader.load 'my_csv_file', :my_table, :cancel_at => :datetime


### TODO

* A task to clean up all these temporary tables when we're done.

* Broader support for Rubies, Databases, and ORM/tools for building the schema.

* More options for the log file (txt vs textile, filename).

* Better tests!
