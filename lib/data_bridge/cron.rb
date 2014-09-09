require "time"

module DataBridge
    class Cron
    attr_accessor :date, :minute, :hour, :day, :month, :week

    def initialize(cron_str, date=nil)
      return nil if cron_str.nil? or  cron_str.empty?
      init_data(cron_str)
      @date = date || Time.now
    end

    def check_time?
      check_minute? && check_hour? && check_day? && check_month? && check_week? && check_second?
    end

    def check_minute?
      relatively(@date.strftime("%M"), @minute)
    end

    def check_hour?
      relatively(@date.strftime("%H"), @hour)
    end

    def check_day?
      relatively(@date.strftime("%d"), @day)
    end

    def check_month?
      relatively(@date.strftime("%m"), @month)
    end

    def check_week?
      relatively(@date.strftime("%w"), @week)
    end

    def check_second?
      relatively(@date.strftime("%S"), "0")
    end

    private
    def init_data(cron_str)
      cron_arr = cron_str.to_s.split(' ')
      return if cron_arr.size < 5

      @minute = cron_arr[0]
      @hour   = cron_arr[1]
      @day    = cron_arr[2]
      @month  = cron_arr[3]
      @week   = cron_arr[4]
    end

    def relatively(num,arr_str)
      return true if arr_str == '*'
      # 1. ','
      if arr_str.index ','
        return arr_str.split(',').collect{|i| i.to_i}.include? num.to_i
      end
      # 2. '-'
      if arr_str.index('-')
        return (arr_str.split('-')[0]..arr_str.split('-')[1]).collect{|i| i.to_i}.include? num.to_i
      end

      # 0. Integer
      if is_integer?(arr_str)
        return arr_str.to_i == num.to_i
      end

      return true
    end

    def is_integer?(str)
      begin
        return str.to_i.is_a? Integer
      rescue Exception => e
        return false
      end
      false
    end
  end
end
