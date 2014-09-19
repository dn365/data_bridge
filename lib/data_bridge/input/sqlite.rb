# require "jdbc/sqlite3"
require File.expand_path("../../jar/sqlite-jdbc-3.8.5-pre1.jar",__FILE__)
# require "sequel"
module DataBridge
  class Sqlite
    def initialize(options={},cache_memory=true)
      if cache_memory
        @db = Sequel.connect("jdbc:sqlite::memory:", :logger => options["logfile"])
        #@db = Sequel.connect("jdbc:sqlite::memory:")
      else
        @db = Sequel.connect("sqlite://#{options["database"]}", :logger => options["logfile"])
      end
    end

    def create_table(table_name,columns)
      @db.run "CREATE TABLE #{table_name} (#{columns.to_a.map{|i| i.join(" ")}.join(", ")})"
    end

    def alert_table(table_name,fix_column)
      @db << "ALTER TABLE #{table_name} ADD COLUMN #{fix_column.to_a.join(" ")}"
    end

    def drop_table(table_name)
      @db.drop_table(table_name.to_sym)
    end

    def insert(table_name,values)
      items = @db[table_name.to_sym]
      #database transaction
      @db.transaction do
        values.each do |v|
          items.insert(v)
        end
      end
    end

    def select(sql)
      query = @db[sql]
      query
    end

    def discontent
      @db.disconnect
    end

    def table?(table_name)
      @db.table_exists?(:"#{table_name}") ? true : false
    end

  end
end
