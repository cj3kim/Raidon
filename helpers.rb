module Helpers
  module ClassMethods
  end

  module InstanceMethods
    def self.included(base)
    end

    def set_port(port)
      self.db_uri.port = port
    end

    def set_db(db)
      path = pathify
      path[0] = db

      new_path = stringify(path)
      self.db_uri.path = new_path
    end

    def set_bucket(bucket)
      path = pathify
      path[1] = bucket

      new_path = stringify(path)
      self.db_uri.path = new_path
    end

    def pathify
      self.db_uri.path.split("/").reject! { |s| s.empty? }
    end

    def stringify(ary)
      ary.reject! { |s| s.empty? }
      ary.inject("") {|memo, str| memo << "/#{str}" }
    end

    def keyless_path
      path = pathify
      path[2] = ""
      self.db_uri.path = stringify(path)
    end

    def key_uri(key)
      self.db_uri.path << "/#{key}"
      uri = self.db_uri.to_s
      keyless_path
      uri
    end

    def assign_vclock(res)
      vclock = res.headers[:x_riak_vclock]
      mres = mash_it_up(res.body)
      mres.x_riak_vclock = vclock
      mres
    end

    def mash_it_up(elem)
      if elem.class == String
        hash = JSON.parse(elem)
        mash = Hashie::Mash.new(hash)
      else
        mash = Hashie::Mash.new(elem)
      end
    end

    def mapred_uri
      uri = self.db_uri
      "http://#{uri.host}:#{uri.port}/mapred"
    end
  end

end
