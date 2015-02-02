## Data Bridge

  基于Jruby 和 Java Jdbc 库获取自定义数据库接口的Sql数据源, 输出到指定接口的中间桥接程式, 主要用于轮询结果时间线序输出和转存.

####需求环境

  * JRuby 1.7.10 或者更高版本
  * Oracle-jdbc6.jar
  * sqlite-jdbc-3.8.5-pre.jar
  * Sequel-4.13.0

#### 当前版本  

  * v0.2 功能重构版

#### 修复Bug&更新内容
  * 2015-02-02 添加对Key-Value标记的多行时间序列数据做update操作,更新版本号0.2.8.1  
  * 更新sqlite-jdbc版本到3.8.5-pre  
  * 新增sqlite memory 做中间cache,对数据多次处理模式  
  * 新增参数default_value初始值,multiline结果是否多行,time_column_key设置时间序列字段  
  * 目前只支持数据库类型为 sqlite, oracle, influxdb  

#### 执行参数说明  

  +  -h 执行参数帮助
  +  -v 程序版本信息
  +  -c 指定执行配置文件路径
  +  -l 指定程序日志生成路径
  +  -t 最大线程数指定，默认为10
  +  -o 线程超时时间设定，默认1800 单位秒

#### 配置文件参数说明
  + 模版 config.yml

  ```
  #输出接口配置
  output:
    - adapter: influxdb
      host: 127.0.0.1 #多host用","分隔. 示例: 192.168.1.2,182.168.1.3
      port: 80 #default 8086, 可不填
      username: root
      password: root
      use_ssl: false #可不填，默认为false
      database: test_data

  #输入接口配置
  input:
    - series_name: test_data.demo #唯一标识名称,用于输出数据唯一ID字段标识
      desc: 测试数据样例  #简单说明信息
      cron: "0,10,20,30,40,50 * * * *" #循环执行时间,unix cron时间格式,支持区间格式标识如"10-30, * * * *"(表示每小时的10-30分钟区间内每分钟执行)
      runtime: #定义其他可用参数
        interval: 60   #设定Sql selete语句按时间段检索的时间间隔, 默认 秒单位, 如果没有则安轮询时间执行结果为准
        delay_time: 120  #设定时间段延时推移, 默认 秒单位
        output_timestamp: "start:%Y%m%d" #默认"end:%Y%m%d%H%M%S"
        update: true  #最后一次结果是否需要轮询更新
        default_value: 0 #默认填入初始值,数字或字符串, 特殊字符串"null"表示空
        multiline: true #布尔值,标识结果是否是多列输出
        time_column_key: created_at #如果多行结果输出,需要标识时间序列的字段,用于多组数据时间序列拼接结果
      hosts:
        - adapter: oracle #链接数据类型
          host: localhos
          port: 1521 #默认端口 1521
          #version: 12c #特殊数据库版本标记, jdbc链接方式
          username: admin
          password: admin
          database: test_data
          default_options:   #指定全局特定变量替换设定,只针对没有特殊options设定的Sql语句
            $beginmonth: "%Y%m"
            $beginday: "%Y%m%d"
            $begin: "%Y%m%d%H%M"
            $end: "%Y%m%d%H%M"
          sql_group:  #Sql语句集合
            - sql: "select a.a_test1 test1, a.test2 test2 FROM test_table_$beginmonth where created_at >= '$begin' AND created_at < '$end'"
              #指定数据中字段输出值为key和value,成Hash组合
              custom_key_and_value_column:
                key: [column_k1,column_k2,column_k3] #字段名称, 此字段的结果为keys
                value: [column_v1,column_v2,column_v3] #字段名称, 此字段的结果为values
              column_set: ["test1","test2","test3"] #设定字段必须有值输出,如果获取为空则填入默认值
              options:  #对Sql语句中的一下特定变量进行替换
                $beginmonth: "%Y%m"
                $beginday: "%Y%m%d"
                $begin: "%Y%m%d"
                $end: "%Y%m%d"

  ```

#### 执行示例

  ```
  cd data_bridge
  bin/db_run -c conf/config.yml -l logs/demo/run.log -t 20 -o 1200  
  ```
