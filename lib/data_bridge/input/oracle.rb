require File.expand_path("../../jar/ojdbc6.jar",__FILE__)
module DataBridge
  class Oracle

    def initialize options = {}
      @db = Sequel.connect("jdbc:oracle:thin:#{options["username"]}/#{options["password"]}@#{options["host"]}:#{options["port"].to_s || "1521"}#{options["version"].eql?("12c") ? "/" : ":"}#{options["database"]}",:max_connections => options["max_connection"] || 10, :pool_timeout => options["pool_timeout"] || 10, :login_timeout=> options["login_timeout"] || 5, :logger => options["logfile"])
    end

    def select(sql)
      query = @db[sql]
      discontent
      query
    end

    def discontent
      @db.disconnect
    end

  end

end
