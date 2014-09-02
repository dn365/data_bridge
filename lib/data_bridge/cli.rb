require 'optparse'
require File.expand_path("../version", __FILE__)

module DataBridge
  class CLI
    def self.read(arguments=ARGV)
      options = Hash.new
      optparse = OptionParser.new do |opts|
        opts.on('-h', '--help', 'Display this message') do
          puts opts
          exit
        end
        opts.on('-v', '--version', 'Display version') do
          print "DataBridge v" << DataBridge::VERSION << "\n"
          exit
        end

        opts.on('-c', '--config FILE', 'Data bridge yaml config FILE.') do |file|
          options[:config_file] = file
        end

        opts.on('-l', '--log_file FILE', 'Log Files .') do |log|
          options[:log_file] = log
        end

        opts.on('-t', '--max_thread', 'The maximum number of threads running program, default value 10') do |thread|
          options[:max_thread] = thread || 10
        end

        opts.on('-o', '--thread_timeout', 'Thread timeout value, default value 1800 Seconds.') do |thread_timeout|
          options[:thread_timeout] = thread_timeout || 1800
        end
        # opts.on('-b', '--background', 'Fork into the background') do
        #   options[:daemonize] = true
        # end
        # opts.on('-n', '--app_name', 'daemon name conf') do |app_name|
        #   options[:app_name] = app_name
        # end
        # opts.on('-p', '--pid_file FILE', 'Write the PID to a given FILE') do |file|
        #   options[:pid_file] = file
        # end
      end
      optparse.parse!(arguments)
      options
    end
  end
end
