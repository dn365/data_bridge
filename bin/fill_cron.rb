#!/usr/bin/env jruby
require "time"

from_time = "2015-01-31 00:00:00"
end_time = "2015-02-05 00:00:00"

loop do
  time = Time.parse(from_time)

  if time > Time.parse(end_time)
    exit 1
  end
  print "Run Form Time #{time} \n"
  `bin/fill_run -c conf_base/config_fill_update.yml -l logs/fill_logs/run.log -d #{time.strftime("%Y%m%d%H%M").to_s}`

  from_time = (time + 86400).to_s
end
