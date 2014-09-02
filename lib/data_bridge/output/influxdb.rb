require "influxdb"
module DataBridge
  class Influxdb

    def initialize options={}
      if options.any?
        begin
          @influxdb = InfluxDB::Client.new options["database"], hosts: options["host"], username: options["user"], password: options["password"]
        rescue => e
          raise e
          exit 1
        end
      else
        print "Not Found options .\n"
        exit 1
      end
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
