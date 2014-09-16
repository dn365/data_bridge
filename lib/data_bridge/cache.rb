# require File.expand_path("../base", __FILE__)
require File.expand_path("../input/oracle", __FILE__)
require File.expand_path("../input/sqlite", __FILE__)
require "sequel"
module DataBridge
  class Cache < DataBridge::Base
    def initialize(conf_file,logfile)
      @logfile = logfile
      @logger = DataBridge::Logfile.new(logfile)
      @conf = DataBridge::LoadConfig.new(conf_file)
      @content_pool = Hash.new
      @series_cron = Hash.new
      @series_execute = Hash.new
      # content_pool_and_series_cron_initial
      content_pool_and_series_cron
      @sqlite = @content_pool["cache_sqlite"]
    end

    def content_pool_and_series_cron
      if @conf.is_input?
        @conf.input.each do |c|
          @series_cron[c["host"]] = c["cron"]
          @series_execute[c["host"]] = {
            config:{runtime:c["runtime"],desc: c["desc"]},
            sql_group: c["sql_group"]
          }
          unless @content_pool["#{c["host"]}"]
            options = delete_hash(c,["sql_group","default_options"])
            options["logfile"] = DataBridge::Logfile.new(File.dirname(@logfile).to_s << "/#{c["host"]}.log")
            @content_pool["#{c["host"]}"] = db_content(options)
          end
        end
        @content_pool["cache_sqlite"] = db_content({"logfile"=>DataBridge::Logfile.new(File.dirname(@logfile).to_s << "/cache.log"),"adapter"=>"sqlite"})
      end
    end

    def db_content(options)
      case options["adapter"]
      when "oracle"
        return DataBridge::Oracle.new options
      when "sqlite"
        return DataBridge::Sqlite.new options
      end
    end

    def series_execute_format(host_key,t)
      series_execute_dupm = Marshal.load(Marshal.dump(@series_execute))[host_key]
      series_execute_dupm[:sql_group].each do |s|
        options = at(s["options"] || {},t,series_execute_dupm[:config][:runtime])
        s["sql"] = gsub_replace(s["sql"],options)
        delete_hash(s,"options")
      end
      series_execute_dupm
    end

    def execute(host_key,t)
      all_data = Array.new
      db = @content_pool[host_key]
      series_execute_format(host_key,t)[:sql_group].each do |h|
        sql_data = db.select(h["sql"])
        cache_data = cache_execute(h["cache_set"],sql_data.to_a,t)
        all_data += cache_data
      end
      all_data
    end

    #轮询检查任务数组
    def task(t = Time.now)
      series_array = @series_cron.select{|series_name,cron| tick?(cron,t)}.keys.map{|i| i.to_s } if @series_cron.respond_to?(:each)
      return series_array if series_array && !series_array.empty?
      false
    end

    def cache_execute(options,sql_data,t)
      tname = options["tname"]
      field_set = options["field_set"]
      timestamp = (t - t.sec).to_i
      random_tname = tname << rand(1000).to_s

      random_tname = random_tname << "a_#{rand(1000)}" if @sqlite.table?(random_tname)
      @sqlite.create_table(random_tname,field_set)
      @sqlite.insert(random_tname,sql_data)

      cache_data = Array.new
      options["cache_sql"].each do |c|
        sql = c["sql"].gsub("$tname",random_tname)
        query = @sqlite.select(sql).to_a
        query.collect{|q| q[:time] = timestamp}
        cache_data << {series_name: c["series_name"],data: query}
      end
      @sqlite.drop_table(random_tname)
      cache_data
    end


  end
end
