# frozen_string_literal: true

module ElasticAPM
  # @api private
  class TimedWorker
    include Log

    SLEEP_INTERVAL = 0.1

    # @api private
    class StopMsg
    end

    # @api private
    class RequestMsg
      def initialize(path, payload)
        @path = path
        @payload = payload
      end

      attr_reader :path, :payload
    end

    # @api private
    class ErrorMsg
      def initialize(error)
        @error = error
      end

      attr_reader :error
    end

    def initialize(config, messages, batched_transactions, adapter)
      @config = config
      @adapter = adapter
      @messages = messages
      @batched_transactions = batched_transactions

      @last_sent_transactions = Time.now.utc

      @serializers = Struct.new(:transactions, :errors).new(
        Serializers::Transactions.new(config),
        Serializers::Errors.new(config)
      )
    end

    attr_reader :config, :messages, :batched_transactions

    def run_forever
      loop do
        run_once
        sleep SLEEP_INTERVAL
      end
    end

    def run_once
      process_messages

      flush_transactions if should_flush_transactions?
    end

    private

    # rubocop:disable Metrics/MethodLength
    def process_messages
      while (msg = messages.pop(true))
        case msg
        when RequestMsg
          process_request msg
        when ErrorMsg
          process_error msg
        when StopMsg
          flush_transactions

          Thread.exit
        end
      end
    rescue ThreadError
      # queue empty
    end
    # rubocop:enable Metrics/MethodLength

    def process_request(req)
      @adapter.post(req.path, req.payload)
    rescue ::Exception => e
      fatal 'Failed posting: %s', e.inspect
      debug e.backtrace.join("\n")
      nil
    end

    def flush_transactions
      return if batched_transactions.empty?

      transactions = collect_batched_transactions

      payload = @serializers.transactions.build_all(transactions)
      msg = RequestMsg.new('/v1/transactions', payload)
      process_request msg
    end

    def collect_batched_transactions
      batch = []

      begin
        while (transaction = batched_transactions.pop(true))
          batch << transaction
        end
      rescue ThreadError
        # queue empty
      end

      batch
    end

    def should_flush_transactions?
      interval = config.flush_interval

      return true if interval.nil?
      return true if batched_transactions.length >= config.max_queue_size

      Time.now.utc - @last_sent_transactions >= interval
    end

    def process_error(msg)
      payload = @serializers.errors.build_all([msg.error])
      messages.push RequestMsg.new('/v1/errors', payload)
    end
  end
end
