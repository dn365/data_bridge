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
      when "graphite_influxdb"
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
            out_influxdb(ocontent,tabname,data,conf_option)
          when "graphite_influxdb"
            out_influxdb_graphite_api(ocontent,tabname,data,conf_option)
          end
        end
      end
    end

    def cache_output(data=[])
      return @logger.error("Input interface data is empty.") if data.nil? || data.empty?
      if @output_content.empty?
        @logger.error("Did not get to the output interface.")
        @logger.info("Event: " << data.to_json.to_s)
      else
        @output_content.each do |okey,ocontent|
          case okey.to_s
          when "influxdb"
            data.each do |d|
              if d[:data].is_a?(Array)
                d[:data].each{|pd| ocontent.write_point(d[:series_name], pd)}
              else
                ocontent.write_point(d[:series_name],d[:data])
              end
            end
            @logger.info("Cache Data, Event: #{data.to_json.to_s}")
          end
        end
      end
    end

    def output_time(time,conf_option)
      return false if conf_option[:runtime].nil? or conf_option[:runtime]["output_timestamp"].nil?
      out_time_format = conf_option[:runtime]["output_timestamp"].split(":")
      ntime = if out_time_format[0] == "start"
        Time.at(time) - conf_option[:runtime]["interval"].to_i - conf_option[:runtime]["delay_time"].to_i
      else
        Time.at(time) - conf_option[:runtime]["delay_time"].to_i
      end
      Time.parse(ntime.strftime(out_time_format[1])).to_i
    end

    def out_influxdb_graphite_api(odb,tabname,data,conf_option)
      format_data = []
      if conf_option[:runtime]["multiline"]
        tmp_format_data = []
        data.each do |d|
          time = d[:time]
          d.map{|k,v| tmp_format_data << {s_name:"#{tabname}.#{k}", out_value: {time:time,value: v}} if k.to_s != "time"}
        end
        tmp_format_data.group_by{|i| i[:s_name]}.map{|k,v| format_data << {s_name: k, out_value: v.map{|vv| vv[:out_value]}}}
      else
        time = data[:time]
        data.each{|k,v| format_data << {s_name: "#{tabname}.#{k}", out_value:{time:time, value: v}} if k.to_s != "time" }
      end

      if conf_option[:runtime] && (conf_option[:runtime]["output_timestamp"] || conf_option[:runtime]["update"]) && !conf_option[:runtime]["multiline"]
        #判断数据输出是的时间字段是否需要格式化
        out_time = output_time(data[:time],conf_option)
        if out_time
          time = out_time
          format_data.each{|fd| fd[:out_value][:time] = time }
        end

        #判断数据是否循环更新
        if conf_option[:runtime]["update"]
          format_data.each do |fdb|
            begin
              influxdb_query = odb.query("select * from #{fdb[:s_name]} where time > #{time - 1}s")
            rescue => e
              @logger.error(e.to_s)
              influxdb_query = []
            end
            sequence_data = influxdb_query.any? ? influxdb_query.values.flatten![0]["sequence_number"] : nil
            fdb[:out_value][:sequence_number] = sequence_data if sequence_data
          end
        end
      end

      if conf_option[:runtime]["multiline"] && conf_option[:runtime]["update"]
        #多行数据的sequence_number获取,数据更新
        min_time = data.collect{|i| i[:time]}.min - 10

        format_data.each do |k|
          begin
            influxdb_query = odb.query("select * from #{k[:s_name]} where time > #{min_time}s")
          rescue => e
            @logger.error(e.to_s)
            influxdb_query = {}
          end
          if influxdb_query.any?
            influxdb_query.values.flatten.each do |inf|
              time = inf["time"]
              sequence_data = inf["sequence_number"]
              updb = k[:out_value].select{|i| i[:time] == time}[0]
              updb[:sequence_number] = sequence_data if updb
            end
          end
        end
      end

      if conf_option[:runtime]["multiline"]
        format_data.each do |fd|
          fd[:out_value].each{|d| odb.write_point(fd[:s_name],d)}
        end
      else
        format_data.each do |fd|
          odb.write_point(fd[:s_name],fd[:out_value])
        end
      end
      @logger.info("Output graphite format data to influxdb, Data: #{format_data.to_json}")
    end

    def out_influxdb(odb,tabname,data,conf_option)
      if conf_option[:runtime] && (conf_option[:runtime]["output_timestamp"] || conf_option[:runtime]["update"]) && !conf_option[:runtime]["multiline"]
        #判断数据输出是的时间字段是否需要格式化
        out_time = output_time(data[:time],conf_option)
        data[:time] = out_time if out_time
        #判断数据是否循环更新
        if conf_option[:runtime]["update"]
          begin
            influxdb_query = odb.query("select * from #{tabname} where time > #{data[:time] - 1}s")
          rescue => e
            @logger.error(e.to_s)
            influxdb_query = []
          end
          sequence_data = influxdb_query.any? ? influxdb_query.values.flatten![0]["sequence_number"] : nil
          data[:sequence_number] = sequence_data if sequence_data
        end
      end
      if conf_option[:runtime]["multiline"] && conf_option[:runtime]["update"]
        #多行数据的sequence_number获取,数据更新
        begin
          influxdb_query = odb.query("select * from #{tabname} where time > #{data.collect{|i| i[:time]}.min - 10}s")
        rescue => e
          @logger.error(e.to_s)
          influxdb_query = {}
        end
        if influxdb_query.any?
          influxdb_query.values.flatten.each do |inf|
            time = inf["time"]
            sequence_data = inf["sequence_number"]
            updb = data.select{|i| i[:time] == time}[0]
            updb[:sequence_number] = sequence_data if updb
          end
        end
      end
      if conf_option[:runtime]["multiline"]
        data.each{|d| odb.write_point(tabname,d)}
        @logger.info("SeriesName: #{tabname}, #{"Description: " << conf_option[:desc].to_s if conf_option[:desc]}, Multi Line Event: #{data.to_json.to_s}")
      else
        odb.write_point(tabname,data)
        @logger.info("SeriesName: #{tabname}, #{"Description: " << conf_option[:desc].to_s if conf_option[:desc]}, #{data[:sequence_number] ? "Updated" : "Created"} at #{Time.at(data[:time])}, Event: #{data.to_json.to_s}")
      end
    end

  end
end
