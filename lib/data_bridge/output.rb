# require File.expand_path("../base", __FILE__)
require File.expand_path("../output/influxdb", __FILE__)
module DataBridge
  class Output < DataBridge::Base

    def initialize(conf_file,logfile)
      @logger = DataBridge::Logfile.new(logfile)
      @conf = DataBridge::LoadConfig.new(conf_file)
      @output_content = Hash.new
      output_content
    end

    def output_content
      if @conf.is_output?
        @conf.output.each do |conf|
          @output_content[conf["adapter"]] = content_type(conf) unless @output_content[conf["adapter"]]
        end
      end
      @output_content
      # @logger.debug("Put out #{@output_content}")
    end

    def content_type options
      case options["adapter"]
      when "influxdb"
        options["host"] = options["host"].split(",")
        return DataBridge::Influxdb.new options
      else
        nil
      end
    end

    def output(tabname,data={},conf_option = {})
      return @logger.error("Input interface data is empty.") if data.nil? || data.empty? || (data.size.eql?(1) && data[:time])
      if @output_content.empty?
        @logger.error("Did not get to the output interface.")
        @logger.info("Event: " << data.to_json.to_s)
      else
        @output_content.each do |okey,ocontent|
          case okey.to_s
          when "influxdb"
            if conf_option[:runtime] && (conf_option[:runtime]["output_timestamp"] || conf_option[:runtime]["update"]) && !conf_option[:runtime]["multiline"]
              #判断数据输出是的时间字段是否需要格式化
              if conf_option[:runtime]["output_timestamp"]
                out_time_format = conf_option[:runtime]["output_timestamp"].split(":")
                ntime = if out_time_format[0] == "start"
                  Time.at(data[:time]) - conf_option[:runtime]["interval"].to_i - conf_option[:runtime]["delay_time"].to_i
                else
                  Time.at(data[:time]) - conf_option[:runtime]["delay_time"].to_i
                end
                data[:time] = Time.parse(ntime.strftime(out_time_format[1])).to_i
              end

              #判断数据是否循环更新
              if conf_option[:runtime]["update"]

                  influxdb_query = ocontent.query("select * from #{tabname} where time > #{data[:time] - 1}s")
                  sequence_data = influxdb_query.any? ? influxdb_query.values.flatten![0]["sequence_number"] : nil
                  data[:sequence_number] = sequence_data if sequence_data
              end

            elsif conf_option[:runtime]["multiline"] && conf_option[:runtime]["update"]
              #多行数据的sequence_number获取,数据更新
              influxdb_query = ocontent.query("select * from #{tabname} where time > #{data.map{|i| i[:time]}.min - 60}s")
              influxdb_query.values.flatten.each do |inf|
                time = inf["time"]
                sequence_data = inf["sequence_number"]
                updb = data.select{|i| i[:time] == time}[0]
                updb[:sequence_number] = sequence_data if updb
              end
            end
            ocontent.write_point(tabname,data)
            if conf_option[:runtime]["multiline"]
              data.each do |d|
                @logger.info("SeriesName: #{tabname}, #{"Description: " << conf_option[:desc].to_s if conf_option[:desc]}, Created at #{Time.at(d[:time])}, Multi Line Event: #{d.to_json.to_s}")
              end
            else
              @logger.info("SeriesName: #{tabname}, #{"Description: " << conf_option[:desc].to_s if conf_option[:desc]}, #{data[:sequence_number] ? "Updated" : "Created"} at #{Time.at(data[:time])}, Event: #{data.to_json.to_s}")
            end
          end
        end
      end
    end

  end
end
