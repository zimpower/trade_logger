# http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG&f=nsl1op&e=.csv
# 
# Start
# http://download.finance.yahoo.com/d/quotes.csv?s=
# 
# 
# 
# IDs
# Now, you have to set the IDs you want to receive. Every stock, index or currency has their own ID. If you want to get values of more than one ID, separate them with ","
# 
# You also have to convert special characters into the correct URL format
# 
# http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG
# 
# 
# 
# Properties
# For the different property tags look at the property list. Use the tag f.
# 
# I will use here the tags of name(n), symbol(s), the latest value(l1), open(o) and the close value of the last trading day(p)
# 
# http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG&f=nsl1op
# 
# 
# 
# Static part
# At the end add the following to the URL.
# 
# http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG&f=nsl1op&e=.csv

# http://download.finance.yahoo.com/d/quotes.csv?e=.csv&f=sl1d1t1&s=USDINR=X
#
# returns csv text : "USDINR=X",54.635,"4/15/2013","5:55pm" 

require 'test/unit'
require "open-uri"
require "csv"


HEADERS = %w(TICKER RATE DATE TIME)
CCYS = %w(USD EUR GBP AUD NZD JPY SEK NOK CHF CAD
          SAR KWD AED
          INR KRW HKD CNY CNH PHP MYR SGD IDR TWD THB
          CZK PNL HUF RUB RON
          ZAR
          BRL MXN CLP ARS COP PEN VEN)
FX_FEED_BASE = 'http://download.finance.yahoo.com/d/quotes.csv?'
FX_FEED_OPTS = 'f=sl1d1t1'
FX_FEED_EXT = 'e=.csv'

class FX
  attr_accessor :timeout

  def initialize
    @ccys = {"USD" => {'rate' => 1.0, 'timestamp' => Time.now + (10*365*24*60*60)}}    
    @timeout = 15*60   # => 15 mins default timeout
  end

  # returns the fx rate for a given pair
  def spot(pair)
    return nil  unless valid_pair?(pair) 

    und = pair[0..2].upcase
    acc = pair[3..5].upcase
    [ und, acc ].each do |ccy|

      if @ccys.has_key?(ccy)
        # check timestamp
        time_lapse = Time.now.utc - @ccys[ccy]['timestamp']
        update_ccy_rate(ccy)  if time_lapse > @timeout
      else
        update_ccy_rate(ccy)
      end

    end
    return @ccys[acc]['rate'] / @ccys[und]['rate']    
  end
  
  def timestamp(pair)
    return nil  unless valid_pair?(pair) 
    
    und = pair[0..2].upcase
    acc = pair[3..5].upcase
    
    return nil unless @ccys.has_key?(und) && @ccys.has_key?(acc)
    
    # return the stalest timestamp of both ccys
    return @ccys[und]['timestamp'] < @ccys[acc]['timestamp'] ? @ccys[und]['timestamp'] : @ccys[acc]['timestamp']
  end

  def update_ccy_rate(ccy)
    return nil  unless valid_ccy?(ccy) 

    pair = "USD#{ccy.upcase}"
    # puts "Fetching pair: #{pair}"
    csv_str = HEADERS.to_csv + open(pair_url(pair)).read.delete("\n").delete("\r")     
    csv_obj = CSV::parse(csv_str, :headers => :first_row, :return_headers => true)

    ccy_hash = {}
    ccy_hash['timestamp'] = Time.now.utc
    ccy_hash['rate'] = csv_obj[1]["RATE"].to_f
    ccy_hash['date'] = csv_obj[1]["DATE"]
    ccy_hash['time'] = csv_obj[1]["TIME"]
    
    # puts "Received data: #{ccy_hash}"
    
    @ccys[ccy.upcase] = ccy_hash

  end

  def valid_ccy?(ccy)
    return false  unless ccy.is_a?(String)
    return false  unless ccy.length == 3
    return false  unless CCYS.include?(ccy)
    return true
  end

  def valid_pair?(pair)
    return false  unless pair.is_a?(String)
    return false  unless pair.length == 6
    return false  unless CCYS.include?(pair[0..2]) && CCYS.include?(pair[3..5]) 
    return true
  end

  def pair_url(pair)
    return nil  unless valid_pair?(pair) 
    return "#{FX_FEED_BASE}&#{FX_FEED_OPTS}&#{FX_FEED_EXT}&s=#{pair}=X"
  end
end
