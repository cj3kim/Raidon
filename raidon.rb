require "rest_client"
require "net/http"
require "json"
require "hashie"
require 'awesome_print'

Mash = Hashie::Mash

#"127.0.0.1", 8091
#"127.0.0.1", 8092
#"127.0.0.1", 8093
#"127.0.0.1", 8094


Test = Proc.new do
  r = Raidon.new
  result = JSON.parse r.map_reduce(JSMAP, JSRED, "test")
  ap result
end

JSMAP = <<JS
  function(riakObject) {
    var m =  riakObject.values[0].data;
    ary = [];

    if (m != null) {
      ary.push(m)
    }
    return ary;
  }
JS

JSRED = <<JS
  function(values, args) {
    return [];
  }
JS


class Raidon
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

  def set_port(port)
    self.db_uri.port = port
  end

  def set_db(db)
    path = self.pathify
    path[0] = db

    new_path = self.stringify(path)
    self.db_uri.path = new_path
  end

  def set_bucket(bucket)
    path = self.pathify
    path[1] = bucket

    new_path = self.stringify(path)
    self.db_uri.path = new_path
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

  def mash_it_up(elem)
    if elem.class == String
      hash = JSON.parse(elem)
      mash = Mash.new(hash)
    else
      mash = Mash.new(elem)
    end
  end

  def pathify
    self.db_uri.path.split("/").reject! { |s| s.empty? }
  end

  def stringify(ary)
    ary.reject! { |s| s.empty? }
    ary.inject("") {|memo, str| memo << "/#{str}" }
  end

  def keyless_path
    path = self.pathify
    path[2] = ""
    self.db_uri.path = self.stringify(path)
  end

  def key_uri(key)
    self.db_uri.path << "/#{key}"
    uri = self.db_uri.to_s
    self.keyless_path
    uri
  end

  def assign_vclock(res)
    vclock = res.headers[:x_riak_vclock]
    mres = self.mash_it_up(res.body)
    mres.x_riak_vclock = vclock
    mres
  end

  def map_reduce(map, reduce, bucket)
    json = {
      :inputs => bucket,
      :query => [
        {:map => {:language => "javascript", :source => "#{map}" }},
        {:reduce => {:language => "javascript", :source => "#{reduce}" }}
      ]
    }.to_json

    uri = self.db_uri
    mapred_uri = "http://#{uri.host}:#{uri.port}/mapred"

    RestClient.post(mapred_uri, json, :content_type => :json)
  end

  def r_map (map, bucket)
  end

  def r_reduce(reduce, bucket)

  end
end
