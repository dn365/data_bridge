# require File.expand_path("../base", __FILE__)
require File.expand_path("../input/oracle", __FILE__)
require "sequel"
module DataBridge

  class Input < DataBridge::Base

    def initialize(conf_file,logfile)
      @logfile = logfile
      # @conf_file = conf_file
      @logger = DataBridge::Logfile.new(logfile)
      @conf = DataBridge::LoadConfig.new(conf_file)
      @content_pool = Hash.new
      @series_cron = Hash.new
      @series_execute = Hash.new
      # content_pool_and_series_cron_initial
      content_pool_and_series_cron_and_series_execute_initial
    end

    def content_pool_and_series_cron_and_series_execute_initial
      # conf = DataBridge::LoadConfig.new(@conf_file)
      if @conf.is_input?
        @conf.input.each do |c|
          @series_cron[c["series_name"]] = c["cron"]

          @series_execute[c["series_name"]] = Hash.new
          @series_execute[c["series_name"]][:hosts] = Hash.new
          @series_execute[c["series_name"]][:config] = {desc: c["desc"], runtime: c["runtime"]}

          c["hosts"].each do |host|
            # series execute hash data
            @series_execute[c["series_name"]][:hosts][host["host"]] = {
              sql_group: host["sql_group"],
              default_option: host["default_options"]
            }

            # content_pool hash data
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


    #重组sql语句字符串为可执行语句
    def series_execute_format(series_key,t)
      # Variable deep copy
      series_execute_dupm = Marshal.load(Marshal.dump(@series_execute))[series_key]
      series_data = series_execute_dupm
      new_hosts = Hash.new
      config = series_data[:config]

      series_data[:hosts].each do |host_key,sql_query|
        # default_conf = sql_query[:default_option]
        # sqls = sql_query[:sql_group]
        sql_arr = Array.new
        if sql_query[:sql_group].is_a?(Array)
          sql_query[:sql_group].each do |sql|
            options = sql["options"] || sql_query[:default_option] || {}
            opt_format = at(options,t,config[:runtime])

            new_sql = {sql: gsub_replace(sql["sql"],opt_format)}
            delete_hash(sql,["sql","options"]).each{|k,v| new_sql[k.to_sym] = v}
            sql_arr << new_sql
          end
        end
        new_hosts[host_key] = sql_arr
      end
      series_data[:hosts] = new_hosts
      series_data
    end

    #轮询检查任务数组
    def task(t = Time.now)
      series_array = @series_cron.select{|series_name,cron| tick?(cron,t)}.keys.map{|i| i.to_s } if @series_cron.respond_to?(:each)
      return series_array if series_array && !series_array.empty?
      false
    end

    #补录数据得到series_array
    def fill_task
      series_array = @series_cron.map{|s,c| s }
      return series_array if series_array && series_array.any?
      false
    end

    def default_value_set(value)
      return 0 if value.nil?
      return nil if value.eql?("null")
      value
    end

    #执行单个series，获取执行结果
    def execute(series_key,t)
      series_execute = series_execute_format(series_key,t)
      series_value_hash = {time: (t - t.sec).to_i}
      series_value_hash = [] if series_execute[:config][:runtime]["multiline"]

      # default_value = series_execute[:config][:runtime]["default_value"].eql?("null") ? nil : (series_execute[:config][:runtime]["default_value"] || 0)
      default_value = default_value_set(series_execute[:config][:runtime]["default_value"])

      time_column_key = series_execute[:config][:runtime]["time_column_key"]

      hosts = series_execute[:hosts]

      hosts.each do |host_name,host_array|
        db = @content_pool[host_name]
        host_array.each do |hsql|
          #fix: add sql default value
          default_value = default_value_set(hsql[:default_vaule]) if hsql[:default_vaule]

          if series_value_hash.is_a?(Array)
            sql_value = select_sql_value_multi_line(db,hsql[:sql],hsql[:column_set],time_column_key,default_value,hsql[:custom_key_and_value_column] || {},hsql[:key_prefix])
            series_value_hash += sql_value
          else
            sql_value = select_sql_value(db,hsql[:sql],hsql[:column_set],hsql[:custom_key_and_value_column] || {}, hsql[:column_function_set] || {}, default_value, hsql[:key_prefix])
            series_value_hash = series_value_hash.merge(sql_value)
          end
        end
      end

      if series_value_hash.is_a?(Array)
        series_value_hash = group_by_array_to_hash(series_value_hash,time_column_key)
      end
      {data: series_value_hash, config: series_execute[:config]}
    end

    def case_funtion(fun,values)
      case fun
      when "sum"
        fun_sum(values)
      when "avg"
        fun_avg(values)
      else
        return nil
      end
    end

    #辅助函数，对执行过程和执行结果进行格式化, SQL执行结果为单行
    def select_sql_value(db,sql_string,column_set,custom_key_and_value_column,function_column_set,default_value,key_prefix)
      new_value = Hash.new
      begin
        svalue = db.select(sql_string)
        if svalue.to_a.any?
          svalue.each do |row|
            if custom_key_and_value_column.any?
              #fix custom column array
              if custom_key_and_value_column["key"].is_a?(String) || custom_key_and_value_column["value"].is_a?(String)
                ckey = row[custom_key_and_value_column["key"].to_sym]
                ckey = replace_string(ckey)
                ckey = "#{key_prefix}.#{ckey}" if key_prefix
                cvalue = row[custom_key_and_value_column["value"].to_sym]
                new_value[ckey.downcase.to_sym] = data_type_format(cvalue)

              elsif custom_key_and_value_column["key"].size == 1 && custom_key_and_value_column["value"].size == 1
                ckey = row[custom_key_and_value_column["key"][0].to_sym]
                ckey = replace_string(ckey)
                ckey = "#{key_prefix}.#{ckey}" if key_prefix

                cvalue = row[custom_key_and_value_column["value"][0].to_sym]
                new_value[ckey.downcase.to_sym] = data_type_format(cvalue) if ckey
              else

                base_ckey = custom_key_and_value_column["key"].map{|i| replace_string(row[i.to_sym].to_s)}.join(".")
                base_ckey = "#{key_prefix}.#{base_ckey}" if key_prefix

                custom_key_and_value_column["value"].each do |i|
                  ckey = "#{base_ckey}.#{i}"
                  cvalue = row[i.to_sym]
                  new_value[ckey.downcase.to_sym] = data_type_format(cvalue) if ckey
                end
              end
            else
              row.each do |k,v|
                new_value[k.to_s.downcase.to_sym] = data_type_format(v)
              end
            end
          end
        end
      rescue Sequel::DatabaseConnectionError => e
        @logger.error("#{e.to_s}, Execute SQL: #{sql_string}")
      rescue Sequel::DatabaseError => e
        @logger.error("#{e.to_s}, Execute SQL: #{sql_string}")
      end
      if column_set
        fix_column = column_set.map{|i| i.downcase.to_s } - new_value.keys.collect{|i| i.to_s}
        fix_column.each{|k| new_value[k.to_sym] = default_value }  if fix_column.any?
      end

      # add colument function set
      if function_column_set.any?
        fun_hash = Hash.new
        fset_column_keys = Array.new
        function_column_set["function_set"].each do |fset|
          # k_name = fset["column_name"]
          fset_column_keys += fset["column_key"].map{|i| i.downcase.to_sym}
          values = fset["column_key"].map{|i| new_value[i.downcase.to_sym]}
          value = case_funtion(fset["function"],values)
          fun_hash[fset["column_name"].downcase.to_sym] = value
        end

        new_value = new_value.merge(fun_hash)
        unless function_column_set["merger"]
          delete_hash(new_value,fset_column_keys.uniq)
        end
      end
      new_value
    end

    #辅助函数，对执行过程和执行结果进行格式化, 输出多行结果格式化
    def select_sql_value_multi_line(db,sql_string,column_set,time_column_key,default_value,custom_key_and_value_column,key_prefix)
      new_value = Array.new
      begin
        svalue = db.select(sql_string)
        if (sv_arr = svalue.to_a).any?
          # 增加程序逻辑对custom_key_and_value_column 多时间行的数据格式支持
          if custom_key_and_value_column.any?
            multi_svalue = svalue.to_a.group_by{|i| i[time_column_key.to_sym] }
            if custom_key_and_value_column["key"].is_a?(String) || custom_key_and_value_column["value"].is_a?(String)

              multi_svalue.each do |time, sv|
                multi_sv = {}
                multi_sv[time_column_key.to_sym] = time
                sv.each do |row|
                  ckey = replace_string(row[custom_key_and_value_column["key"].to_sym])
                  ckey = "#{key_prefix}.#{ckey}" if key_prefix

                  cvalue = row[custom_key_and_value_column["value"].to_sym]
                  multi_sv[ckey.downcase.to_sym] = data_type_format(cvalue)
                end
                new_value << multi_sv
              end
            elsif custom_key_and_value_column["key"].size == 1 && custom_key_and_value_column["value"].size == 1
              multi_svalue.each do |time, sv|
                multi_sv = {}
                multi_sv[time_column_key.to_sym] = time
                sv.each do |row|
                  ckey = replace_string(row[custom_key_and_value_column["key"][0].to_sym])
                  ckey = "#{key_prefix}.#{ckey}" if key_prefix
                  cvalue = row[custom_key_and_value_column["value"][0].to_sym]
                  multi_sv[ckey.downcase.to_sym] = data_type_format(cvalue)
                end
                new_value << multi_sv
              end
            else
              multi_svalue.each do |time, sv|
                multi_sv = {}
                multi_sv[time_column_key.to_sym] = time
                sv.each do |row|
                  base_ckey = custom_key_and_value_column["key"].map{|i| replace_string(row[i.to_sym].to_s)}.join(".")
                  base_ckey = "#{key_prefix}.#{base_ckey}" if key_prefix

                  custom_key_and_value_column["value"].each do |i|
                    ckey = "#{base_ckey}.#{i}"
                    cvalue = row[i.to_sym]
                    multi_sv[ckey.downcase.to_sym] = data_type_format(cvalue)
                  end
                end
                new_value << multi_sv
              end
            end
          else
            # svalue.to_a.each do |sv|
            #   fix_column = (column_set + [time_column_key]) - sv.keys.map{|i| i.to_s}
            #   if fix_column.any?
            #     fix_column.each do |k|
            #       sv[k.to_s.downcase.to_sym] = default_value
            #     end
            #   end
            # end
            # svalue.to_a.each do |sv|
            #   sv_tmp = {}
            #   sv.each do |sk,ssv|
            #     sv_tmp[sk.downcase.to_sym] = data_type_format(ssv)
            #   end
            #   new_value << sv_tmp
            # end
            new_value = svalue.to_a
          end
          # new_value += sv_arr
        end
      rescue Sequel::DatabaseConnectionError => e
        @logger.error("#{e.to_s}, Execute SQL: #{sql_string}")
      rescue Sequel::DatabaseError => e
        @logger.error("#{e.to_s}, Execute SQL: #{sql_string}")
      end

      if column_set
        new_value.each do |v|
          fix_column = (column_set + [time_column_key]) - v.keys.map{|i| i.downcase.to_s }
          if fix_column.any?
            fix_column.each do |fc|
              v[fc.to_s.downcase.to_sym] = default_value
            end
          end
        end
      end
      new_value
    end

  end
end
