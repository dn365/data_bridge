require File.expand_path("../base", __FILE__)
require File.expand_path("../input", __FILE__)
require File.expand_path("../output", __FILE__)
module DataBridge
  class FillRun

    def self.run(options={},sleep_time = 1)
      data_bridge = self.new(options[:config_file],options[:log_file],options[:max_thread] || 10,options[:thread_timeout] || 1800)
      date_time = options[:date] ? Time.parse(options[:date]) : Time.now
      data_bridge.execute date_time

    end


    def initialize(conf_file,log_file,max_thread,thread_timeout)
      @max_thread = max_thread
      @thread_timeout = thread_timeout
      @input = DataBridge::Input.new(conf_file,log_file)
      @output = DataBridge::Output.new(conf_file,log_file)
      @logger = DataBridge::Logfile.new(log_file)
    end

    def execute(t)
      # if task = @input.task(t)
      #   if thread_available?
      #     task.each do |series_name|
      #       Thread.new do
      #         Thread.current[:name] = "#{series_name}_#{t.strftime("%Y%m%d%H%M%S")}"
      #         Thread.current[:time] = t
      #         #input
      #         data = @input.execute(series_name,t)
      #         #output
      #         @output.output(series_name,data[:data],data[:config])
      #       end
      #     end
      #   else
      #     thread_timeout
      #   end
      # end

      if task = @input.fill_task

        task.each do |series_name|

          # Thread.new do
          #   Thread.current[:name] = "#{series_name}_#{t.strftime("%Y%m%d%H%M%S")}"
          #   Thread.current[:time] = t
          #   #input
          #   puts t
            puts series_name
            data = @input.execute(series_name,t)
            #output
            @output.output(series_name,data[:data],data[:config])
          # end
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
