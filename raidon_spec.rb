require_relative "./raidon"

describe "Raidon" do

  let!(:r_instance) do
     Raidon.new("http", "www.fun.com", "3000", "/db/bucket/key", "?pika=chu")
  end

  describe "#initialize" do
    it "creates a db_uri" do
      r_instance.db_uri.to_s.should == "http://www.fun.com:3000/db/bucket/key?pika=chu"
    end
  end

  describe "#set_db" do
    it "sets the db for Raidon" do
      r_instance.set_db("new_db")

      r_instance.db_uri.path.should == "/new_db/bucket/key"
    end
  end

  describe "#set_bucket" do
    it "sets the bucket for Raidon" do
      r_instance.set_bucket("new_bucket")

      r_instance.db_uri.path.should == "/db/new_bucket/key"
    end

  end

  describe "#connect" do
    it "connects briefly to the db" do
      uri = r_instance.db_uri.to_s
      RestClient.should_receive(:get).with(uri)
      r_instance.should_receive(:mash_it_up)

      r_instance.connect
    end
  end

  describe "#mash_it_up" do
    it "takes a string or hash and turns it into a mash" do
      hash = { :key => "value" }
      json = hash.to_json

      r_instance.mash_it_up(json).should == Hashie::Mash.new(JSON.parse(json))
      r_instance.mash_it_up(hash).should == Hashie::Mash.new(hash)
    end
  end

  describe "#pathify" do
    it "splits the path into array elements" do
      r_instance.pathify.should == ["db", "bucket", "key"]
    end
  end

  describe "#stringify" do
    it "concatenates path array elements into a string" do
      path = r_instance.pathify
      r_instance.stringify(path).should == "/db/bucket/key"

      path[2] = ""
      r_instance.stringify(path).should == "/db/bucket"
    end
  end

  describe "keyless_path" do
    it "points the uri to the bucket" do
      r_instance.keyless_path
      r_instance.db_uri.path.should == "/db/bucket"
    end
  end

end
