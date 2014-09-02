require "yaml"

module DataBridge
  class LoadConfig

    def initialize(file)
      if File.exists?(file)
        begin
          @conf = YAML.load_file(file)
        rescue => e
          raise e
          exit 1
        end
      else
        print "#{file}, This file does not exist.\n"
        exit 1
      end
    end

    def input
      @conf["input"]
    end

    def is_input?
      return true if @conf["input"] && @conf["input"].any?
      false
    end

    def output
      @conf["output"]
    end

    def is_output?
      return true if @conf["outputa"] && @conf["output"].any?
      false
    end

  end
end

###test###
# 
# conf = DataBridge::LoadConfig.new("conf/config_jf_test.yml")
# puts conf.is_input?
# puts conf.input
