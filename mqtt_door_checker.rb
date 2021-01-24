#!/usr/bin/ruby
# vim: ts=2 sw=2 et si ai :

require 'pp'
require 'mqtt'
require 'logger'
require 'yaml'
require 'ostruct'
require 'json'
require 'date'

def usage
  $stderr.puts "usage : #{File.basename($0)} config.yaml"
  exit(1)
end

$prev_hallic = nil
$last_received_time = Time.now
$watchdog_thread = nil

$stdout.sync = true
Dir.chdir(File.dirname($0))
$current_dir = Dir.pwd

$log = Logger.new(STDOUT)
$log.level = Logger::DEBUG

usage if ARGV.size == 0

$conf = OpenStruct.new(YAML.load_file(ARGV[0]))

$conn_opts = {
  remote_host: $conf.mqtt_host,
  client_id: $conf.mqtt_client_id
}

if !$conf.mqtt_port.nil?
  $conn_opts["remote_port"] = $conf.mqtt_port
end

if $conf.mqtt_use_auth == true
  $conn_opts["username"] = $conf.mqtt_username
  $conn_opts["password"] = $conf.mqtt_password
end

def main_loop
  $log.info "connecting..."
  MQTT::Client.connect($conn_opts) do |c|
    $log.info "connected!"

    c.subscribe($conf.subscribe_topic)

    $watchdog_thread = Thread.start do
      loop do
        sleep($conf.battery_check_interval)
        if Time.now - $last_received_time > $conf.battery_low_timeout
          h = {}
          h["status"] = "unknown"
          h["last_updated_time"] = DateTime.now.iso8601(0)
          h["battery"] = "ng"
          h["target_topic"] = $conf.subscribe_topic

          $log.info "publish: topic=#{$conf.publish_topic}, message=#{h.to_json}"
          c.publish($conf.publish_topic, h.to_json, true)
        end
      end
    end

    c.get do |topic, message|
      $log.info "recv: topic=#{topic}, message=#{message}"
      $last_received_time = Time.now

      json = JSON.parse(message)
      now_hallic = json["HALLIC"]
      now_hallic -= 128 if now_hallic >= 128

      if $prev_hallic != nil && now_hallic != $prev_hallic
        h = {}
        h["status"] = now_hallic == 0 ? "open" : "close"
        h["last_updated_time"] = DateTime.now.iso8601(0)
        h["battery"] = "ok"
        h["target_topic"] = $conf.subscribe_topic

        $log.info "publish: topic=#{$conf.publish_topic}, message=#{h.to_json}"
        c.publish($conf.publish_topic, h.to_json, true)

      end
      $prev_hallic = now_hallic
    end
  end
end

loop do
  begin
    main_loop
  rescue Exception => e
    $log.debug(e.to_s)
  end

  begin
    Thread.kill($watchdog_thread) if $watchdog_thread.nil? == false
    $watchdog_thread = nil
  rescue
  end

  $log.info "waiting 5 seconds..."
  sleep 5
end
