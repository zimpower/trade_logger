#
#  Parse the trade events and extract useful information and saves it to a file in YAML format

require 'rss'  # Provides RSS parsing capabilities
require 'csv'  # Provide CSV parsing capabilities
require 'yaml' # Provide YAML parsing capabilities

# the RSS FEED we are parsing
RSS_FEED = "https://kgc0418-tdw-data-0.s3.amazonaws.com/slices/FOREX_RSS_FEED.rss"

# Pre-defined format of RSS feed
COLUMNS = ["DISSEMINATION_ID", "ORIGINAL_DISSEMINATION_ID", "ACTION", "EXECUTION_TIMESTAMP", 
  "CLEARED", "INDICATION_OF_COLLATERALIZATION", "INDICATION_OF_END_USER_EXCEPTION", 
  "INDICATION_OF_OTHER_PRICE_AFFECTING_TERM", "BLOCK_TRADES_AND_LARGE_NOTIONAL_OFF-FACILITY_SWAPS", 
  "EXECUTION_VENUE", "EFFECTIVE_DATE", "END_DATE", "DAY_COUNT_CONVENTION", "SETTLEMENT_CURRENCY", 
  "ASSET_CLASS", "SUB-ASSET_CLASS_FOR_OTHER_COMMODITY", "TAXONOMY", "PRICE_FORMING_CONTINUATION_DATA", 
  "UNDERLYING_ASSET_1", "UNDERLYING_ASSET_2", "PRICE_NOTATION_TYPE", "PRICE_NOTATION",
  "ADDITIONAL_PRICE_NOTATION_TYPE", "ADDITIONAL_PRICE_NOTATION", "NOTIONAL_CURRENCY_1",
  "NOTIONAL_CURRENCY_2", "ROUNDED_NOTIONAL_AMOUNT_1", "ROUNDED_NOTIONAL_AMOUNT_2",
  "PAYMENT_FREQUENCY_1", "PAYMENT_FREQUENCY_2", "RESET_FREQUENCY_1", "RESET_FREQUENCY_2",
  "EMBEDED_OPTION", "OPTION_STRIKE_PRICE", "OPTION_TYPE", "OPTION_FAMILY", "OPTION_CURRENCY",
  "OPTION_PREMIUM", "OPTION_LOCK_PERIOD", "OPTION_EXPIRATION_DATE",
  "PRICE_NOTATION2_TYPE", "PRICE_NOTATION2", "PRICE_NOTATION3_TYPE", "PRICE_NOTATION3"
]


# 1- Helper function for write yaml
# add the deep_stringify_keys, open the Hash class
class Hash
  def deep_stringify_keys
    new_hash = {}
    self.each do |key, value|
      new_hash.merge!(key.to_s => (value.is_a?(Hash) ? value.deep_stringify_keys : value))
    end
  end
end

# 2- Helper function for write yaml
# Pretty Yaml file writing
def write(filename, hash)
  File.open(filename, "a") do |f|
    f.write(yaml(hash))
  end
end

# 3- Helper function for write yaml
def yaml(hash)
  method = hash.respond_to?(:ya2yaml) ? :ya2yaml : :to_yaml
  string = hash.deep_stringify_keys.send(method)
#  string.gsub("!ruby/symbol ", ":").sub("---","").split("\n").map(&:rstrip).join("\n").strip
end


csv_header = COLUMNS.to_csv
trades = {}

# keep poling the RSS feed - only extract items that we DO NOT already have
loop do
  # Read the feed into rss_content
  rss_content = ""
  open(RSS_FEED) do |f|
    rss_content = f.read
  end

  # First parse RSS with validation, and if it is not valid parse with non-validation.
  rss = nil
  begin
    rss = RSS::Parser.parse(rss_content)
  rescue RSS::InvalidRSSError
    rss = RSS::Parser.parse(rss_content, false)
  end

  # Loop over each item on the feed
  rss.channel.items.each do |item|

    # Extract the numeric GUID from thelong form guid
    # https://kgc0418-tdw-data-0.s3.amazonaws.com/slices/FOREX_RSS_FEED.rss#935534398</guid>
    # we just want the number at the end = 935534398 to use as our unique hash key
    guid_str = "#{item.guid}"
    re = /rss#(.*)<\/guid>$/
    guid = guid_str.match re

    if trades[guid[1]].nil?
      
      # This is a new trade - create its own hash and start filling in some fields
      trade = {}
      trade[:title] = item.title
      trade[:pub_date] = item.date

      # item.description contains a CSV row with all the trade data
      
      # Add a header row and then use the CSV class to help us simply parse using the headers as keys
      csv_str = csv_header + item.description.gsub("\n","")  + item.description.gsub("\n","")
      csv = CSV::parse(csv_str, :headers => :first_row, :return_headers => true)

      # Common fields
      trade[:id] = csv['DISSEMINATION_ID'][1]
      trade[:asset] = csv['ASSET_CLASS'][1]
      trade[:taxonomy] = csv['TAXONOMY'][1]
      trade[:time_stamp] = csv['EXECUTION_TIMESTAMP'][1]
      trade[:status] = csv['PRICE_FORMING_CONTINUATION_DATA'][1]
      trade[:und] = csv['NOTIONAL_CURRENCY_1'][1]
      trade[:acc] = csv['NOTIONAL_CURRENCY_2'][1]
      trade[:und_not] = csv['ROUNDED_NOTIONAL_AMOUNT_1'][1]
      trade[:acc_not] = csv['ROUNDED_NOTIONAL_AMOUNT_2'][1]

      if item.title == "ForeignExchange:NDF"
        # do something with NDFs

      elsif ( item.title == "ForeignExchange:VanillaOption" || 
        item.title == "ForeignExchange:NDO" || 
        item.title == "ForeignExchange:SimpleExotic:Barrier" ||
        item.title == "ForeignExchange:ComplexExotic" )

        # do something with vanilla options, NDOs 
        trade[:expiry]  = Date.parse( csv['OPTION_EXPIRATION_DATE'][1] )
        trade[:strike]  = csv['OPTION_STRIKE_PRICE'][1]
        trade[:type]  = csv['OPTION_TYPE'][1].gsub('-','')
        trade[:prem_ccy]  = csv['OPTION_CURRENCY'][1]
        trade[:prem]  = csv['OPTION_PREMIUM'][1]

      end

      # add the trade to our trades hash and write to disk
      trades[guid[1]] = true
      trade_hash = {trade[:id] => trade}
      
      puts "New Trade added: #{trade.inspect}"
      puts yaml(trade_hash)

      write("trade_log.yaml",trade_hash)
      
    end
  end
end