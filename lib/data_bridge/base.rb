require File.expand_path("../load_config", __FILE__)
require File.expand_path("../logfile", __FILE__)
require File.expand_path("../cron", __FILE__)
module DataBridge
  class Base

    def delete_hash(hash,delete_key)
      if hash.is_a?(Hash)
        delete_key.map{|i| hash.delete(i) } if delete_key.is_a?(Array)
        hash.delete(delete_key) if delete_key.is_a?(String) || delete_key.is_a?(Symbol)
        return hash
      end
      hash
    end

    def tick?(cron,t)
      DataBridge::Cron.new(cron,t).check_time?
    end

    def fun_sum(array=[])
      return 0 if array.empty?
      array.map(&:to_f).reduce(:+)
    end

    def fun_avg(array=[])
      return 0 if array.empty?
      fun_sum(array) / array.size
    end


    def at(options,t, conf)
      if options.respond_to?(:each)
        interval = conf["interval"] || 60
        delay_time = conf["delay_time"] || 0

        options = options.collect do |k,vtime|
          case k
          when "$end"
            [k, delay_time ? (t - delay_time.to_i).strftime(vtime) : t.strftime(vtime)]
          when "$begin"
            [k,delay_time ? (t - interval.to_i - delay_time.to_i).strftime(vtime) : (t - interval.to_i).strftime(vtime)]
          else
            time = delay_time ? (t - interval.to_i - delay_time.to_i) : (t - interval.to_i)
            [k, time.strftime(vtime)]
          end
        end
        options = Hash[options]
      end
      options
    end

    def gsub_replace(str,replace)
      new_str = nil
      return str unless replace.any?
      replace.each do |rk,rv|
        str = str.gsub(rk.to_s,rv.to_s)
        new_str = str
      end
      new_str
    end

    def data_type_format(data)
      return data.to_f.round(3) if data.nil? || data.is_a?(Numeric) || data.is_a?(BigDecimal)
      if data.to_i.to_s == data
        data.to_i
      elsif data.to_f.to_s == data
        data.to_f.round(3)
      elsif data.empty?
        data.to_i
      else
        data.to_s
      end
    end

    def group_by_array_to_hash(array,group_key)
      new_array = Array.new
      array.group_by{|i| i[group_key.downcase.to_sym]}.each do |k,v_arr|
        time = Time.parse(k.to_s)
        new_hash = {time: (time - time.sec).to_i}
        v_arr.each do |v|
          v = delete_hash(v,group_key.downcase.to_sym)
          v.each{|tk,tv| new_hash[tk.downcase.to_sym] = data_type_format(tv)}
        end
        new_array << new_hash
      end
      new_array
    end

  end
end
