require 'fastercsv'
require 'active_support'

# FasterCSV will auto-detect the line separator, which we'd like to pass to MySQL
class FasterCSV
  attr_reader :row_sep
end

module DataLoader

  class Inspector
    class << self
      attr_reader :row_sep      # set after inspect_file
    end

    # read a csv and return the columns and types in an ordered array
    def self.inspect_file(file, separator = ',', inspect_rows = 10, hints = {})
      fields = nil
      FasterCSV.open(file,
        :col_sep => separator,
        :converters => [:date_time, :integer],    # :integer, :float, :date, :date_time
        :headers => true,
        :header_converters => lambda {|h| h.underscore.gsub(/[^a-z0-9_]/, ' ').strip.gsub(' ', '_').squeeze('_') },
        :skip_blanks => true) do |csv|
          @row_sep = csv.row_sep
          fields = scan_rows(csv, inspect_rows, hints)
      end
      fields
    end

    # scan a few rows to determine data types
    def self.scan_rows(csv, inspect_rows, hints = {})
      first_row = nil
      columns = {}  # unordered hash containing data types for each header

      1.upto(inspect_rows) do
        begin
          row = csv.gets
          break unless row
          row.each do |header, value|
            columns[header] = promote_type(columns[header], dbtype(value))
          end
          first_row ||= row # save for later
        rescue FasterCSV::MalformedCSVError => boom
          # Don't care about the error but let's retry, since fastercsv will skip this line
          retry
        end
      end

      # form an ordered array based on the first row read:
      fields = []
      first_row.each do |header, value|
        data_type = columns[header]
        fields << {:name => header, :type => data_type}
      end

      # validate hints
      hints.stringify_keys!
      invalid_columns = hints.keys - fields.map {|f| f[:name]}
      puts "Warning: hint column(s) not found: #{invalid_columns.join(', ')}" unless invalid_columns.empty?
      invalid_types = hints.values - [:text, :string, :datetime, :date, :integer]
      abort "Error: hint types(s) are invalid: #{invalid_types.join(', ')}" unless invalid_types.empty?

      fields.each do |field|
        name, field_type = field[:name], field[:type]
        # override columns with hints
        if hints.has_key?(name)
          hint_type = hints[name].to_sym
          if field_type.nil?
            puts "Note: undertermined type for #{name} hinted as #{hint_type}."
          elsif hint_type != field_type
            puts "Note: overriding type #{field_type} for #{name} with #{hint_type}."
          end
          field[:type] = hint_type
        end
        # default to :string if everything was nil (and no hint)
        if field[:type].nil?
          puts "Warning: type could not be determined for #{name}, defaulting to string."
          field[:type] = :string
        end
      end

      fields
    end

    # determine what datatype is most suitable for the value
    def self.dbtype(value)
      if value.is_a?(Fixnum)
        :integer
      elsif value.is_a?(DateTime)
        :datetime
      elsif value.is_a?(String)
        if value.blank?
          nil
        else
          :text
        end
      elsif value.nil?
        nil
      else
        raise 'Unknown type'
      end
    end

    # given two datatypes choose what fits them both
    def self.promote_type(*types)
      types.compact!
      if types.empty?
        nil
      elsif (types - [:text, :string, :datetime, :integer, :date]).length > 0 # unknown types
        raise 'Unknown type'
      elsif Set.new(types).length == 1  # one type
        types.first
      elsif types.include?(:text)
        :text
      else
        :string
      end
    end
  end

end
