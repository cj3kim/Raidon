require "rest_client"
require "net/http"
require "json"
require "hashie"
require 'awesome_print'
require_relative './helpers'


#"127.0.0.1", 8091
#"127.0.0.1", 8092
#"127.0.0.1", 8093
#"127.0.0.1", 8094


TEST = Proc.new do
  r = Raidon.new
  result = JSON.parse r.map_reduce(JSMAP, JSRED, "test")
  ap result
  result
end

TEST2 = Proc.new do
  r = Raidon.new
  result = r.r_map(JSMAP, "test")
  ap result
  result
end

TEST3 = Proc.new do
  r = Raidon.new
  result = r.r_reduce(JSRED, "test")
  ap result
end

JSMAP = <<JS
  function(riakObject) {
    var m =  riakObject.values[0].data;
    ary = [];

    if (m.match(/:/g) == null) {
      ary.push(m)
    } else {
      ary.push(JSON.parse(m));
    }
    return ary;
  }
JS

JSRED = <<JS
 function reducey(values) {
    var key, val;
    var str = "";

    for(val in values) {
      if (values[val].constructor == String) {
        str += values[val];
      }
    }
    return [str];
  }
JS


class Raidon
  include Helpers::InstanceMethods
  attr_accessor :db_uri

  def initialize(scheme="", host="", port="", path="", query="")
    #TODO For ease of testing in irb. Create a proc to do it for you.
    if scheme == "" && host == "" && port == ""
      self.db_uri = URI.parse("http://127.0.0.1:8091/riak/test?returnbody=true")
    else
      self.db_uri = URI.parse("#{scheme}://#{host}:#{port}#{path}#{query}")
    end
  end

  def connect
    response = RestClient.get(self.db_uri.to_s)
    ap response

    mash_it_up(response)
  end

  def get_key(key)
    uri = self.key_uri(key)

    res = RestClient.get(uri)
    res = assign_vclock(res)
  end

  def set_key(key, value)
    uri = self.key_uri(key)

    RestClient.post(
      uri,
      {key => value }.to_json,
      :content_type => :json
    )
  end

  def delete_key(key)
    uri = self.key_uri(key)

    RestClient.delete(uri)
  end

  def map_reduce(map, reduce, bucket)
    json = {
      :inputs => bucket,
      :query => [
        {:map => {:language => "javascript", :source => "#{map}" }},
        {:reduce => {:language => "javascript", :source => "#{reduce}" }}
      ]
    }.to_json

    ap reduce

    RestClient.post(self.mapred_uri, json, :content_type => :json)
  end

  def r_map (map, bucket)
    json = {
      :inputs => bucket,
      :query => [
        {:map => {:language => "javascript", :source => "#{map}" }}
      ]
    }.to_json

    JSON.parse RestClient.post(self.mapred_uri, json, :content_type => :json)

  end

  def r_reduce(reduce, bucket)
    json = {
      :inputs => bucket,
      :query => [
        {:reduce => {:language => "javascript", :source => "#{reduce}" }}
      ]
    }.to_json

    JSON.parse RestClient.post(self.mapred_uri, json, :content_type => :json)
  end
end
