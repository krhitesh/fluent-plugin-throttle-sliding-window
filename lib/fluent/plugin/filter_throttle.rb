# frozen_string_literal: true
require 'fluent/plugin/filter'

module Fluent::Plugin
  class ThrottleFilter < Filter
    Fluent::Plugin.register_filter('throttle', self)

    desc "Used to group logs. Groups are rate limited independently."
    config_param :groups_config, :hash, :default => {
      "default" => {
        "max_rate" => 10,
        "window_size" => 5,
        "slide_interval" => 1,
        "drop_logs" => true
      },
      "answr-be-goapp" => {
        "max_rate" => 2,
        "window_size" => 5,
        "slide_interval" => 1,
        "drop_logs" => true
      }
    }

    # desc <<~DESC
    #   This is the period of of time over which group_bucket_limit applies
    # DESC
    # config_param :group_bucket_period_s, :integer, :default => 60

    # desc <<~DESC
    #   Maximum number logs allowed per groups over the period of
    #   group_bucket_period_s
    # DESC
    # config_param :group_bucket_limit, :integer, :default => 6000

    # desc "Whether to drop logs that exceed the bucket limit or not"
    # config_param :group_drop_logs, :bool, :default => true

    # desc <<~DESC
    #   After a group has exceeded its bucket limit, logs are dropped until the
    #   rate per second falls below or equal to group_reset_rate_s.
    # DESC
    # config_param :group_reset_rate_s, :integer, :default => nil

    # desc <<~DESC
    #   When a group reaches its limit and as long as it is not reset, a warning
    #   message with the current log rate of the group is emitted repeatedly.
    #   This is the delay between every repetition.
    # DESC
    # config_param :group_warning_delay_s, :integer, :default => 10

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

    # One Group defined for kubernetes.labels.app == "answr-be-goapp" only.
    Group = Struct.new(
      :max_rate,
      :window_size,
      :slide_interval,
      :hash, # throttle_window

      # :rate_count,
      # :rate_last_reset,
      # :aprox_rate,
      # :bucket_count,
      # :bucket_last_reset,
      :last_warning)

    def window_create(size)
      # not needed
    end

    def window_get(tw, ts)
      index = -1
      size = tw.size
      # log.debug("window_get size #{size}")
      for i in (0...size) do
        # log.debug("window get index loop #{i}")
        if tw.table[i].timestamp.to_i == ts.to_i
          index = i
          return index
        end
        index = i
      end

      return -1 # not found
    rescue StandardError => e
      log.debug("Encountered error #{e}") 
    end

    def window_add(tw , ts, val)
      
      i = -1
      index = -1
      size = -1
      sum = 0

      tw.current_ts = ts
      size = tw.size
      index = window_get(tw, ts)

      # log.debug("window_Add index => #{index}")

      # log.debug("ticker index: #{ts}")

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
      # log.debug("window_Add total => #{sum}")
    rescue StandardError => e
      log.debug("Encountered error #{e}")
    end

    def time_ticker
      key = Thread.current["key"]
      log.info("Started time_ticker looper on thread => #{key}")
      while @tickers[key].done != true
        now = Time.now
        # log.debug("group hash: #{@group.hash}")
        window_add(@groups[key].hash, now, 0)
        @tickers[key].group.hash.current_ts = now
        

        # log.debug("ticker loop: #{now}")
        sleep(@tickers[key].seconds) # sleep every second

      end
    rescue StandardError => e
      log.debug("Encountered error #{e}")
    end

    def configure(conf)
      super

      # log.debug("configuring plugin: filter_throttle")

      # @group_key_paths = group_key.map { |key| key.split(".") }

      # raise "group_bucket_period_s must be > 0" \
       # unless @group_bucket_period_s > 0

     # @group_gc_timeout_s = 2 * @group_bucket_period_s

      #raise "group_bucket_limit must be > 0" \
      #  unless @group_bucket_limit > 0

     # @group_rate_limit = (@group_bucket_limit / @group_bucket_period_s)

     # @group_reset_rate_s = @group_rate_limit \
        #if @group_reset_rate_s == nil

     # raise "group_reset_rate_s must be >= -1" \
      #  unless @group_reset_rate_s >= -1
      #raise "group_reset_rate_s must be <= group_bucket_limit / group_bucket_period_s" \
      #  unless @group_reset_rate_s <= @group_rate_limit

     # raise "group_warning_delay_s must be >= 1" \
     #  unless @group_warning_delay_s >= 1

      # configure the group
      now = Time.now
      @groups = {}
      @tickers = {}
      @ticker_threads = {}
      @slide_intervals = {}
      # log.debug("current time #{now}")

     # log.debug("Groups config => #{@groups_config}")

      @groups_config.each do |key, value|
        #log.debug("Groups key,value => #{key} #{value}")
        @groups[key] = Group.new(value["max_rate"], value["window_size"], value["slide_interval"], ThrottleWindow.new(0, value["window_size"], 0, -1, -1, Array.new(value["window_size"], ThrottlePane.new(0, 0))), nil)
        @slide_intervals[value["slide_interval"]] = @groups[key]

        @tickers[key] =  Ticker.new(@groups[key], false, @groups[key].slide_interval)
        @ticker_threads[key] = Thread.new(self, &:time_ticker)
        @ticker_threads[key]["key"] = key
        @ticker_threads[key].abort_on_exception = true

        # log.debug("Groups => #{@groups}")
      
      end



     # @group = Group.new(10, 5, 1, ThrottleWindow.new(0, 5, 0, -1, -1, Array.new(5, ThrottlePane.new(0, 0))), 0, now, 0, 0, now, nil)
      # # log.debug("group: #{@group}")
      # # @group.hash = window_create(@group.window_size) # throttle_window added in Group.new instantiation
     # @ticker = Ticker.new(@group, false, @group.slide_interval)
      # log.debug("ticker: #{@ticker}")
     # @ticker_thread = Thread.new(self, &:time_ticker)
     # @ticker_thread.abort_on_exception = true
     # # log.debug("configure complete")
    rescue StandardError => e
      log.debug("Encountered error #{e}")
    end

    def start
      super
      @totalrec = {}
      @droppedrec = {}
      @counters = {}
      # log.debug("counters summary: #{@counters}")
    end

    def shutdown
      # log.debug("counters summary: #{@counters}")
      super
    end

    def filter(tag, time, record)

      apps_label = extract_group(record)
      
      now = Time.now

      

      if groups_config.key?(apps_label)

        if @totalrec.key?(apps_label) ==  false
          @totalrec[apps_label] = 0
          @droppedrec[apps_label] = 0
        end

        @totalrec[apps_label] += 1

        log.info("\n[#{apps_label}] Total records => #{@totalrec[apps_label]}\n[#{apps_label}] Dropped records => #{@droppedrec[apps_label]}\n[#{apps_label}] Approx. Rate => #{@groups[apps_label].hash.total.to_f / @groups[apps_label].hash.size}")

        rate_limit_exceeded = @groups_config[apps_label]["drop_logs"] ? nil : record # return nil on rate_limit_exceeded to drop the record

        if @groups[apps_label].hash.total / @groups[apps_label].hash.size >= @groups[apps_label].max_rate
          log.warn("[#{apps_label}] Rate limit exceeded.")
          @droppedrec[apps_label] += 1

          return rate_limit_exceeded
        end

        window_add(@groups[apps_label].hash, @groups[apps_label].hash.current_ts, 1)
      end

      
      # if apps_label == @app_name_key
      #  if @group.hash.total / @group.hash.size >= @group.max_rate
      #    log.debug("Rate limit exceeded.")
      #    @droppedrec += 1
      #    return rate_limit_exceeded
      #  end

      #  # log.debug("\n@group.hash => #{@group.hash}\n@group.hash.current_ts => #{@group.hash.current_ts}")
      #  window_add(@group.hash, @group.hash.current_ts, 1)
      #end
      
      record
    end

    private

    def extract_group(record)
      record["kubernetes.labels.app"]
      # @group_key_paths.map do |key_path|
      #   record.dig(*key_path) || record.dig(*key_path.map(&:to_sym))
      # end
    end

    def log_rate_limit_exceeded(now, group, counter)
      emit = counter.last_warning == nil ? true \
        : (now - counter.last_warning) >= @group_warning_delay_s
      if emit
        log.warn("rate exceeded", log_items(now, group, counter))
        counter.last_warning = now
      end
    end

    def log_rate_back_down(now, group, counter)
      log.info("rate back down", log_items(now, group, counter))
    end

    def log_items(now, group, counter)
      since_last_reset = now - counter.bucket_last_reset
      rate = since_last_reset > 0 ? (counter.bucket_count / since_last_reset).round : Float::INFINITY
      aprox_rate = counter.aprox_rate
      rate = aprox_rate if aprox_rate > rate

      {'group_key': group,
       'rate_s': rate,
       'period_s': @group_bucket_period_s,
       'limit': @group_bucket_limit,
       'rate_limit_s': @group_rate_limit,
       'reset_rate_s': @group_reset_rate_s}
    end
  end
end