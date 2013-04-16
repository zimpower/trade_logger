require 'test/unit'
require './fx_feeds'

describe FX, "#spot" do

  it "knows what are valid pairs" do
    fx = FX.new
    fx.should be_valid_pair("USDJPY")
  end

  it "returns an exchange rate for a valid pair" do
    fx = FX.new
    spot = fx.spot("USDJPY")
    # puts spot
    spot.should be_a(Float) 
  end

  it "returns fails to return an exchange rate for an invalid pair" do
    fx = FX.new
    spot = fx.spot("FOOBAR")
    spot.should be_nil
  end

  it "should work for inverted pairs" do
    fx = FX.new
    spot = fx.spot("JPYUSD")
    spot.should be_a(Float) 
  end

  it "inverted pair should equal 1/pair" do
    fx = FX.new
    spot = fx.spot("JPYUSD")
    inverted_spot = fx.spot("USDJPY")
    spot.should be == 1/inverted_spot 
  end

  it "should return a valid cross rate" do
    fx = FX.new
    usdjpy = fx.spot("USDJPY")
    eurusd = fx.spot("EURUSD")
    eurjpy = fx.spot("EURJPY")
    eurjpy.should be == eurusd * usdjpy 
  end

  it "should have a refresh time attribute" do
    fx = FX.new
    fx.timeout = 30
    fx.timeout.should == 30
  end

  it "should have a timestamp for each pair" do
    fx = FX.new
    timestamp = fx.timestamp("USDJPY")
    timestamp.should be_nil
    spot = fx.spot("USDJPY")
    timestamp = fx.timestamp("USDJPY")
    timestamp.should be_a(Time)
  end

  it "should only refresh spot rates after the timeout has expiried" do
    fx = FX.new
    fx.timeout = 5

    spot = fx.spot("USDJPY")
    timestamp1 = fx.timestamp("USDJPY")
    spot = fx.spot("USDJPY")
    timestamp2 = fx.timestamp("USDJPY")
    sleep(10)
    spot = fx.spot("USDJPY")
    timestamp3 = fx.timestamp("USDJPY")

    timestamp1.should == timestamp2
    timestamp1.should_not == timestamp3
  end
end
