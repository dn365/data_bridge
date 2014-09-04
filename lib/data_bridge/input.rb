# require File.expand_path("../base", __FILE__)
require File.expand_path("../input/oracle", __FILE__)
require "sequel"
module DataBridge

  class Input < DataBridge::Base

    def initialize(conf_file,logfile)
      @logfile = logfile
      @conf_file = conf_file
      @logger = DataBridge::Logfile.new(logfile)
      @content_pool = Hash.new
      @series_cron = Hash.new
      content_pool_and_series_cron_initial
    end

    def content_pool_and_series_cron_initial
      conf = DataBridge::LoadConfig.new(@conf_file)
      if conf.is_input?
        conf.input.each do |c|
          @series_cron[c["series_name"]] = c["cron"]

          c["hosts"].each do |host|
            unless @content_pool["#{host["host"]}"]
              options = delete_hash(host,["sql_group","default_options"])
              options["logfile"] = DataBridge::Logfile.new(File.dirname(@logfile).to_s << "/#{host["host"]}.log")
              @content_pool["#{host["host"]}"] = db_content(options)
            end
          end
        end
      end
      # @logger.debug(@content_pool.to_s)
      # @logger.debug(@series_cron.to_s)
    end

    def db_content(options)
      case options["adapter"]
      when "oracle"
        return DataBridge::Oracle.new options
      when "sqlite"
        nil
      end
    end

    #获取配置中input数据
    def series_execute_initial
      conf = DataBridge::LoadConfig.new(@conf_file)
      series_execute = Hash.new

      if conf.is_input?
        conf.input.each do |c|
          series_execute[c["series_name"]] = Hash.new
          series_execute[c["series_name"]][:hosts] = Hash.new
          series_execute[c["series_name"]][:config] = {desc: c["desc"], runtime: c["runtime"]}

          c["hosts"].each do |host|
            series_execute[c["series_name"]][:hosts][host["host"]] = {
              sql_group: host["sql_group"],
              default_option: host["default_options"]
            }
          end
        end
      end
      series_execute
    end

    #重组sql语句字符串为可执行语句
    def series_execute_format(series_key,t)
      series_data = series_execute_initial[series_key]
      hosts = Hash.new

      conf = series_data[:config]
      hosts_group = series_data[:hosts]

      hosts_group.each do |host_key,sql_query|
        default_conf = sql_query[:default_option]
        sqls = sql_query[:sql_group]
        sql_arr = Array.new
        if sqls.respond_to?(:each)
          sqls.each do |sql|
            options = sql["options"] || default_conf || Hash.new
            opt_format = at(options,t,conf[:runtime])

            new_sql = {sql: gsub_replace(sql["sql"],opt_format)}
            sql.each{|k,v| new_sql[k.to_sym] = v unless ["sql","options"].include?(k) }

            sql_arr << new_sql
          end
        end
        hosts[host_key] = sql_arr
      end

      series_data[:hosts] = hosts
      series_data
    end

    #轮询检查任务数组
    def task(t = Time.now)
      series_array = @series_cron.select{|series_name,cron| tick?(cron,t)}.keys.map{|i| i.to_s } if @series_cron.respond_to?(:each)
      return series_array if series_array && !series_array.empty?
      false
    end

    #执行单个series，获取执行结果
    def execute(series_key,t)
      series_execute = series_execute_format(series_key,t)
      series_value_hash = {time: (t - t.sec).to_i}
      hosts = series_execute[:hosts]

      hosts.each do |host_name,host_array|
        db = @content_pool[host_name]
        host_array.each do |hsql|
          sql_value = select_sql_value(db,hsql[:sql], hsql[:custom_key_and_value_column] || {})
          if hsql[:column_set]
            if (fix_column = hsql[:column_set] - sql_value.keys.map{|i| i.to_s}).any?
              fix_column.collect{|k| sql_value[k.to_sym] = 0 }
            end
          end
          series_value_hash = series_value_hash.merge(sql_value)
        end
      end
      {data: series_value_hash, config: series_execute[:config]}
    end

    #辅助函数，对执行过程和执行结果进行格式化
    def select_sql_value(db,sql_string,custom_key_and_value_column = {})
      new_value = Hash.new
      begin
        svalue = db.select(sql_string)
        svalue.each do |row|
          if custom_key_and_value_column.any?
            ckey = row[custom_key_and_value_column["key"].to_sym]
            cvalue = row[custom_key_and_value_column["value"].to_sym]
            new_value[ckey.downcase.to_sym] = data_type_format(cvalue)
          else
            row.each do |k,v|
              new_value[k.to_s.downcase.to_sym] = data_type_format(v)
            end
          end
        end
      rescue Sequel::DatabaseConnectionError => e
        @logger.error("#{e.to_s}, Execute SQL: #{sql_string}")
      rescue Sequel::DatabaseError => e
        @logger.error("#{e.to_s}, Execute SQL: #{sql_string}")
      end
      new_value
    end


  end
end

### test
#
# input = DataBridge::Input.new("conf/config_jf_test.yml","logs/info.log")
# p input.series_execute_format("cb_process_perf.1d",Time.now)
# 3.times do
#   p input.series_execute_format("cb_process_perf.1d",Time.now)
#   sleep 5
# end
