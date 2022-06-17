# frozen_string_literal: true
require 'fluent/plugin/filter'

module Fluent::Plugin
  class ThrottleFilter < Filter
    Fluent::Plugin.register_filter('throttle', self)

    desc <<~DESC
      Used to group logs. Groups are rate limited independently.
    DESC
    config_param :groups_config, :hash, :default => {
      "default" => {
        "max_rate" => 10,
        "window_size" => 5,
        "slide_interval" => 1,
        "drop_logs" => true
      }
    }

    desc <<~DESC
      Group key
    DESC
    config_param :group_key, :string, :default => "kubernetes.pod_name"

    desc <<~DESC
      Config key
    DESC
    config_param :config_key ,:string, :default => "kubernetes.labels.app"

    desc <<~DESC
      Group warning rate
    DESC
    config_param :warning_rate, :integer, :default => -1

    desc <<~DESC
      SNS topic to notify to when rate when rate limit is exceeded.
    DESC
    config_param :rate_limit_exceeded_sns_topic, :string, :default => ""


    Ticker = Struct.new(
      :group,
      :done,
      :seconds
    )

    ThrottlePane = Struct.new(
      :timestamp,
      :counter
    )

    ThrottleWindow = Struct.new(
      :current_ts,
      :size,
      :total,
      :result_mutex,
      :max_index,
      :table # []throttle_pane
    )

    Group = Struct.new(
      :max_rate,
      :window_size,
      :slide_interval,
      :hash, # throttle_window
      :last_warning,
      :key,
      :last_event_ts
    )


    def window_create(size)
      # not needed
    end


    def window_get(tw, ts)
      index = -1
      size = tw.size
      for i in (0...size) do
        if tw.table[i].timestamp.to_i == ts.to_i
          index = i
          return index
        end
        index = i
      end
      return -1
    end


    def window_add(tw , ts, val)
      i = -1
      index = -1
      size = -1
      sum = 0
      tw.current_ts = ts
      size = tw.size
      index = window_get(tw, ts)
      if index == -1
        if size - 1 == tw.max_index
          tw.max_index = -1
        end
        tw.max_index += 1
        tw.table[tw.max_index].timestamp = ts
        tw.table[tw.max_index].counter = val
      
      else
        tw.table[index].counter += val
      end
      for c in (0...size)
        sum += tw.table[c].counter
      end
      tw.total = sum
    end


    def gc_groups
      while true
        now = Time.now
        delete_keys = []
        @groups.each do |key,value|
          if now.to_i - value.last_event_ts.to_i >= 60
            delete_keys.append(key)
            @slide_intervals[value["slide_interval"]].delete(value)
          end
        end
        delete_keys.each do |key|
          @groups.delete(key)
        end
        sleep(30)
      end
    end


    def time_ticker
      while true
        now = Time.now
        @slide_intervals.each do |key, value|
          if @ticker_counter % key == 0
            value.each do |group|
              window_add(group.hash, now, 0)
              group.hash.current_ts = now
            end
          end
        end
        @ticker_counter += 1
        sleep(1)
      end
    end


    def configure(conf)
      super
      @groups = {}
      @ticker_threads = {}
      @slide_intervals = {}
      @ticker_counter = 1
      @totalrec = {}
      @droppedrec = {}
      @counters = {}
      ticker_thread = Thread.new(self, &:time_ticker)
      ticker_thread.abort_on_exception = true
      cleanup_thread = Thread.new(self, &:cleanup_groups)
      cleanup_thread.abort_on_exception = true
    end


    def start
      super
    end


    def shutdown
      super
    end


    def filter(tag, time, record)
      apps_label = extract_group(record, @config_key)
      pod_name = extract_group(record, @group_key)
      
      now = Time.now
      if groups_config.key?(apps_label)
        if @totalrec.key?(pod_name) ==  false
          @totalrec[pod_name] = 0
          @droppedrec[pod_name] = 0
          value = groups_config[apps_label]
          @groups[pod_name] = Group.new(value["max_rate"], value["window_size"], value["slide_interval"], ThrottleWindow.new(0, value["window_size"], 0, -1, -1, Array.new(value["window_size"], ThrottlePane.new(0, 0))), nil, pod_name, now)
          
          if @slide_intervals.key?(value["slide_interval"]) == false
            @slide_intervals[value["slide_interval"]] = []
          end

          @slide_intervals[value["slide_interval"]].append(@groups[pod_name])

        end

        @totalrec[pod_name] += 1

        log.info("Pod name => #{pod_name}, Total records => #{@totalrec[pod_name]}, Dropped records => #{@droppedrec[pod_name]}, Approx Rate => #{@groups[pod_name].hash.total.to_f / @groups[pod_name].hash.size}")

        rate_limit_exceeded = @groups_config[apps_label]["drop_logs"] ? nil : record

        if @groups[pod_name].hash.total / @groups[pod_name].hash.size >= @groups[pod_name].max_rate
          log.warn("Pod name => #{pod_name}, message => Rate limit exceeded.")
          @droppedrec[pod_name] += 1
          return rate_limit_exceeded
        end

        @groups[pod_name].last_event_ts = now
        window_add(@groups[pod_name].hash, @groups[pod_name].hash.current_ts, 1)
      end
      record
    end

    private

    def extract_group(record, key)
      record[key]
      # @group_key_paths.map do |key_path|
      #   record.dig(*key_path) || record.dig(*key_path.map(&:to_sym))
      # end
    end
  end
end