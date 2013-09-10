# -*- coding: utf-8 -*-
require_relative './object_buffered_output'
require_relative '../dns_resolver'

module Fluentd
  module Plugin
    class ForwardOutput < ObjectBufferedOutput
      Plugin.register_output('forward', self)

      config_param :send_timeout, :time, :default => 60
      config_param :ipv6, :bool, :default => false
      config_param :expire_dns_cache, :time, :default => nil # 0 means disalbe cache, nil means cache infinitely

      config_param :keepalive, :bool, :default => false

      config_param :heartbeat_type, :default => :udp do |val|
        case val.downcase
        when 'none' then :none
        when 'tcp' then :tcp
        when 'udp' then :udp
        else
          raise ConfigError, "forward output heartbeat type should be 'tcp', 'udp' or 'none'"
        end
      end
      config_param :heartbeat_interval, :time, :default => 1

      config_param :failure_threshold, :integer, :default => 2
      config_param :recover_threshold, :integer, :default => 3

      attr_reader :nodes

      def initialize
        super

        require 'socket'
        require 'ipaddr'
        require 'resolv'
      end

      def configure(conf)
        super

        @nodes = conf.elements.select{|e| e.name == 'server'}.map{|e| Node.new(e, self)}
        @nodes.each do |node|
          node.address # check not to raise ResolveError
        end

        @mutex = Mutex.new
      end

      def start
        @rand_seed = Random.new.seed
        @weight_array = rebuild_balancing(@nodes)

        ##################
        ###
        @nodes.each do |node|
          node.start(actor)
        end

        ###TODO: multi process ?
        @usock = Fluentd.socket_manager.listen_udp(@bind, @port)
        @usock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
        actor.watch_io(@usock, &method(:on_heartbeat_readable))

        super
      end

      def on_heartbeat_readable(sock)
        begin
          msg, addr = sock.recvfrom(1024)
          ###TODO
          # specify addr -> node
          # update node as available
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
          return
        end
      end

      def write_objects(tag, chunk)
        return if chunk.empty?

        error = nil

        wlen = @weight_array.length
        wlen.times do
          @rr = (@rr + 1) % wlen
          node = @weight_array[@rr]

          if node.available?
            begin
              node.send_data(tag, chunk)
              return
            rescue => e
              # for load balancing during detecting crashed servers
              error = e
            end
          end
        end

        if error
          raise error
        else
          raise "no nodes are available"
        end
      end

      def rebuild_balancing(nodes)
        standby_nodes, using_nodes = nodes.partition {|n|
          n.standby?
        }

        lost_weight = 0
        using_nodes.each do |n|
          lost_weight += n.weight unless n.available?
        end

        if lost_weight > 0
          standby_nodes.each do |n|
            next unless n.available?

            using_nodes << n
            lost_weight -= n.weight
            break if lost_weight <= 0
          end
        end

        weight_array = []
        # Integer#gcd => gcd: greatest common divisor
        gcd = using_nodes.map {|n| n.weight }.inject(0) {|r,w| r.gcd(w) }
        using_nodes.each do |n|
          (n.weight / gcd).times do
            weight_array << n
          end
        end

        # for load balancing during detecting crashed servers
        coe = (using_nodes.size * 6) / weight_array.size
        weight_array *= coe if coe > 1

        r = Random.new(@rand_seed)
        weight_array.sort_by{ r.rand }
      end

      class Node
        attr_reader :name, :host, :port, :weight, :standby, :keepalive, :ipv6
        attr_accessor :available

        def self.parse_bool(conf, name, default)
          if conf.has_key?(name)
            conf[name].nil? || ['true', 'yes'].include?(conf[name].downcase)
          else
            default
          end
        end

        def initialize(config, parent)
          @host = config['host']
          @port = config['port'].to_i
          @name = config['name'] || "#{@host}:#{@port}"

          @weight = config.has_key?('weight') ? config['weight'].to_i : 60

          @standby = self.class.parse_bool(config, 'standby', false)
          @keepalive = self.class.parse_bool(config, 'keepalive', parent.keepalive)

          @proto = self.class.parse_bool(config, 'ipv6', parent.ipv6) ? :ipv6 : :ipv4

          @ipaddr_refresh_interval = parent.expire_dns_cache
          @ipaddr = nil # unresolved
          @ipaddr_expires = Time.now - 1

          @available = true
        end

        def standby? ; @standby ; end
        def keepalive? ; @keepalive ; end
        def available? ; @available ; end

        def address
          # 0 means disalbe cache, nil means cache infinitely
          return @ipaddr if @ipaddr && (@ipaddr_expires.nil? || Time.now < @ipaddr_expires)

          @ipaddr = DNSResolver.new(@proto).resolve(@host)
          if @ipaddr_refresh_interval && @ipaddr_refresh_interval > 0
            @ipaddr_expires = Time.now + @ipaddr_refresh_interval
          end
          @ipaddr
        end

        ####################TODO ...
        # https://gist.github.com/frsyuki/6191818
        def start(actor)
          # 2. execute tcp/udp hearbeat sender
          # 3. connect to node if keepalive

        # actor.listen_tcp(@bind, @port) do |sock|
        #   h = Handler.new(sock, method(:on_message))
        #   actor.watch_io(sock, h.method(:on_readable))
        # end

          actor.create_tcp_thread_server(@bind, @port, &method(:client_thread))

          def client_thread(sock)
            msg = sock.read
            sock.write msg
          ensure
            sock.close
          end

          actor.every @interval do
            collector.emit("ping", Time.now.to_i, {"ping"=>1})
          end
        end

        # バックグラウンドでずっと能動的に動かしておきたいものactor#backgroundを使うといいらしい！

        def connection
          # with or without keepalive
        end

        def send_data(tag, chunk)
        end

        def send_heartbeat
        end

        def send_tcp_heartbeat
        end

        def send_udp_heartbeat
          begin
            usock.send "\0", 0, host, port
          rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
          end
        end

        def recv_udp_heartbeat # ?
        end
      end

      ############################### in_forward code ##################
      # message Entry {
      #   1: long time
      #   2: object record
      # }
      #
      # message Forward {
      #   1: string tag
      #   2: list<Entry> entries
      # }
      #
      # message PackedForward {
      #   1: string tag
      #   2: raw entries  # msgpack stream of Entry
      # }
      #
      # message Message {
      #   1: string tag
      #   2: long? time
      #   3: object record
      # }
      def on_message(msg)
        if msg.nil?
          # for future TCP heartbeat_request
          return
        end

        # TODO format error
        tag = msg[0].to_s
        entries = msg[1]

        if entries.class == String
          # PackedForward
          es = MessagePackEventCollection.new(entries, @cached_unpacker)
          collector.emits(tag, es)

        elsif entries.class == Array
          # Forward
          es = MultiEventCollection.new
          entries.each {|e|
            time = e[0].to_i
            time = (now ||= Time.now.to_i) if time == 0
            record = e[1]
            es.add(time, record)
          }
          collector.emits(tag, es)

        else
          # Message
          time = msg[1]
          time = Time.now.to_i if time == 0
          record = msg[2]
          collector.emit(tag, time, record)
        end
      end

      class Handler
        def initialize(io, on_message)
          @io = io
          if @io.is_a?(TCPSocket)
            opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
            @io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
          end
          $log.trace { "accepted fluent socket object_id=#{self.object_id}" }
          @on_message = on_message
          @buffer = ''
        end

        def on_readable
          begin
            data = @io.read_nonblock(32*1024, @buffer)
          rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
            return
          end
          on_read(data)
        end

        def on_read(data)
          first = data[0]
          if first == '{' || first == '['
            m = method(:on_read_json)
            @y = Yajl::Parser.new
            @y.on_parse_complete = @on_message
          else
            m = method(:on_read_msgpack)
            @u = MessagePack::Unpacker.new
          end

          (class << self; self; end).module_eval do
            define_method(:on_read, m)
          end
          m.call(data)
        end

        def on_read_json(data)
          @y << data
        rescue
          $log.error "forward error: #{$!.to_s}"
          $log.error_backtrace
          close
        end

        def on_read_msgpack(data)
          @u.feed_each(data, &@on_message)
        rescue
          $log.error "forward error: #{$!.to_s}"
          $log.error_backtrace
          close
        end

        def on_close
          $log.trace { "closed fluent socket object_id=#{self.object_id}" }
        end
      end
    end

  end
end
