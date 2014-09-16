require File.expand_path("../base", __FILE__)
require File.expand_path("../cache", __FILE__)
require File.expand_path("../output", __FILE__)
module DataBridge
  class CacheRun

    def self.run(options={},sleep_time = 1)
      data_bridge = self.new(options[:config_file],options[:log_file],options[:max_thread] || 10,options[:thread_timeout] || 1800)
      loop do
        data_bridge.execute Time.now
        sleep sleep_time
      end
    end


    def initialize(conf_file,log_file,max_thread,thread_timeout)
      @max_thread = max_thread
      @thread_timeout = thread_timeout
      @cache = DataBridge::Cache.new(conf_file,log_file)
      @output = DataBridge::Output.new(conf_file,log_file)
      @logger = DataBridge::Logfile.new(log_file)
    end

    def execute(t)
      if task = @cache.task(t)
        if thread_available?
          task.each do |host_name|
            Thread.new do
              Thread.current[:name] = "#{host_name}_#{t.strftime("%Y%m%d%H%M%S")}"
              Thread.current[:time] = t
              #input
              data = @cache.execute(host_name,t)
              #output
              @output.cache_output(data)
            end
          end
        else
          thread_timeout
        end
      end
    end


    def thread_available?
      Thread.list.select{|t| t[:name]}.count < @max_thread
      # @logger.debug(Thread.list.select{|t| t[:name]}.count)
    end

    def thread_timeout
      Thread.list.each do |t|
        if t[:time] && (Time.now - Time.parse(t[:time].to_s) > @thread_timeout)
          t.kill
          @logger.info("Warning: Thread Name #{t[:name]} has been closed because of timeout.")
        end
      end
    end

  end
end
