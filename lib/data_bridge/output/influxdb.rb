require "influxdb"
module DataBridge
  class Influxdb

    def initialize options={}
      @influxdb = InfluxDB::Client.new options["database"], hosts: options["host"], port: options["port"] || 8086, username: options["user"], password: options["password"]
    end

    def write_point(tname,data)
      @influxdb.write_point(tname,data)
    end

    def query sql
      @influxdb.query sql
    end

    def is_hash?(data)
      data.is_a?(Hash)
    end

  end
end
