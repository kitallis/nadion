module Nadion
  require 'securerandom'
  require 'fileutils'

  VERSION = '0.0.1'.freeze

  class Util
    class << self
      def round_up_to_power_of_2(i)
        raise ArgumentError.new('Must be a positive integer!') if i < 0 or i.nil?
        2 ** Math.log2(i).ceil
      end
    end
  end

  class Executor
    class << self
      def cached
        java.util.concurrent.Executors.newCachedThreadPool()
      end

      def fixed(size, auto_terminate = true)
        valid_argument?(size)
        Concurrent::FixedThreadPool.new(size, auto_terminate: auto_terminate)
      end

      def single_threaded(auto_terminate = true)
        Concurrent::SingleThreadExecutor.new(auto_terminate: auto_terminate)
      end

      private

      def valid_argument?(size)
        raise ArgumentError.new('size must be a positive integer!') unless Integer === size
        raise ArgumentError.new('size must be a positive integer!') unless size > 0
      end
    end
  end

  class Disruptor
    DEFAULT_BUFFER_SIZE = 4096

    class << self
      def start!(queue_size, executor)
        queue_size ||= DEFAULT_BUFFER_SIZE
        buffer_size = Util.round_up_to_power_of_2(queue_size)
        buffer_factory = proc { Event.new }

        wait_strategy = ::Disruptor::BlockingWaitStrategy.new
        producer_type = ::Disruptor::DSL::ProducerType::MULTI
        @disruptor = ::Disruptor::DSL::Disruptor.new(buffer_factory, buffer_size, executor, producer_type, wait_strategy)

        @disruptor.start
      end
    end
  end

  class Event < Struct.new(:id, :message, :buffer);
  end

  class WorkerPool
    class << self
      def start!(worker_count, buffer_ring, executor)
        event_factory = proc { Event.new }
        exception_handler = ::Disruptor::FatalExceptionHandler.new
        workers = worker_count.times.map { Worker.new(buffer_ring) }.to_java(::Disruptor::WorkHandler)
        @pool = ::Disruptor::WorkerPool.new(event_factory, exception_handler, workers)

        @pool.start(executor)
      end
    end
  end

  class Producer
    attr_reader :executor, :buffer_ring, :worker_ring

    def initialize(worker_count = nil, queue_size = nil)
      worker_count ||= Concurrent.processor_count

      @executor = Executor.cached
      @buffer_ring = Disruptor.start!(queue_size, @executor)
      @worker_ring = WorkerPool.start!(worker_count, @buffer_ring, @executor)

      @should_drop_on_reject = true
    end

    def perform_async(msg)
      id = get_next

      if id
        event = worker_ring.get(id)
        event.id = id
        event.message = msg
        event.buffer = buffer_ring.next

        worker_ring.publish(id)
      end
    end

    private

    def get_next
      if @should_drop_on_reject
        begin
          worker_ring.tryNext
        rescue ::Disruptor::InsufficientCapacityException => e
          puts 'Queue full, dropping event.'
        end
      else
        worker_ring.next
      end
    end
  end

  class Worker
    include com.lmax.disruptor.WorkHandler

    attr_reader :buffer_ring

    def initialize(buffer_ring)
      @buffer_ring = buffer_ring
    end

    def on_event(event)
      path = 'test'
      ext = '.md'
      store_path = path + '.' + (0...50).map { ('a'..'z').to_a[rand(26)] }.join + ext

      write_file_with_lock(store_path) do
        File.open(store_path, 'a') { |f| f.puts(event.message) }
      end
    ensure
      @buffer_ring.publish(event.buffer) if event.buffer
    end

    private

    def write_file_with_lock(store_path)
      File.open(store_path, 'w:UTF-8').flock(File::LOCK_EX)
      yield
      File.open(store_path, 'w:UTF-8').flock(File::LOCK_UN)
    end
  end
end
