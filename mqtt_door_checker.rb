#!/usr/bin/ruby
# vim: ts=2 sw=2 et si ai :

require 'pp'
require 'mqtt'
require 'logger'
require 'yaml'
require 'ostruct'
require 'json'
require 'date'

$prev_hallic = nil

$stdout.sync = true
Dir.chdir(File.dirname($0))
$current_dir = Dir.pwd

$log = Logger.new(STDOUT)
$log.level = Logger::DEBUG

$conf = OpenStruct.new(YAML.load_file("config.yaml"))

$conn_opts = {
  remote_host: $conf.mqtt_host
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

    c.get do |topic, message|
      $log.info "recv: topic=#{topic}, message=#{message}"
      json = JSON.parse(message)
      now_hallic = json["HALLIC"]
      now_hallic -= 128 if now_hallic >= 128

      if $prev_hallic != nil && now_hallic != $prev_hallic
        h = {}
        h["is_opened"] = now_hallic == 0 ? true : false
        h["last_updated_time"] = DateTime.now.iso8601(0)

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

  $log.info "waiting 5 seconds..."
  sleep 5
end
