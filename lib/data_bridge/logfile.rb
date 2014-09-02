require "logger"
module DataBridge
  class Logfile

    def initialize(log_file,files_quantity=10)
      #日志文件目录是否存在
      Dir.mkdir(file_path) unless Dir.exists?(File.dirname(log_file))
      @logger = Logger.new(log_file,files_quantity)
    end

    def info(event)
      @logger.info(event)
    end

    def error(event)
      @logger.error(event)
    end

    def debug(event)
      @logger.debug(event)
    end
  end
end
