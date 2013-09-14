require_relative './input'

module Fluentd
  module Plugin
    class DummyEmit < Input
      Plugin.register_input('dummy_emit', self)

      config_param :interval, :time, :default => 5
      config_param :tag, :string, :default => 'dummy'
      config_param :data, :hash

      def configure(conf)
        super
      end

      def start
        super

        @running = true
        actor.every(@interval) do
          Fluentd.log.warn "hooooooooooooooooooooooo"
          next unless @running
          collector.emit(@tag, Time.now.to_i, @data)
        end

        actor.run
      end

      def stop
        @running = false
        super
      end
    end
  end
end
