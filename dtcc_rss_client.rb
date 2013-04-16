#
#  Parse the trade events and extract useful information and saves it to a file in YAML format

require 'rss'  # Provides RSS parsing capabilities
require 'csv'  # Provide CSV parsing capabilities
require 'logger'
require 'time'
require './trade' # representation of a trade
require './time_hash'

$LOG = Logger.new('/usr/local/var/log/dtcc_logger/dtcc_logger.log', 'daily')

# the RSS FEED we are parsingrequire "dtcc_trade_logger"

RSS_FEED = "https://kgc0418-tdw-data-0.s3.amazonaws.com/slices/FOREX_RSS_FEED.rss"

# Pre-defined format of RSS feed
COLUMNS = %w[DISSEMINATION_ID ORIGINAL_DISSEMINATION_ID ACTION EXECUTION_TIMESTAMP 
  CLEARED INDICATION_OF_COLLATERALIZATION INDICATION_OF_END_USER_EXCEPTION 
  INDICATION_OF_OTHER_PRICE_AFFECTING_TERM BLOCK_TRADES_AND_LARGE_NOTIONAL_OFF-FACILITY_SWAPS 
  EXECUTION_VENUE EFFECTIVE_DATE END_DATE DAY_COUNT_CONVENTION SETTLEMENT_CURRENCY 
  ASSET_CLASS SUB-ASSET_CLASS_FOR_OTHER_COMMODITY TAXONOMY PRICE_FORMING_CONTINUATION_DATA 
  UNDERLYING_ASSET_1 UNDERLYING_ASSET_2 PRICE_NOTATION_TYPE PRICE_NOTATION
  ADDITIONAL_PRICE_NOTATION_TYPE ADDITIONAL_PRICE_NOTATION NOTIONAL_CURRENCY_1
  NOTIONAL_CURRENCY_2 ROUNDED_NOTIONAL_AMOUNT_1 ROUNDED_NOTIONAL_AMOUNT_2
  PAYMENT_FREQUENCY_1 PAYMENT_FREQUENCY_2 RESET_FREQUENCY_1 RESET_FREQUENCY_2
  EMBEDED_OPTION OPTION_STRIKE_PRICE OPTION_TYPE OPTION_FAMILY OPTION_CURRENCY
  OPTION_PREMIUM OPTION_LOCK_PERIOD OPTION_EXPIRATION_DATE
  PRICE_NOTATION2_TYPE PRICE_NOTATION2 PRICE_NOTATION3_TYPE PRICE_NOTATION3
]


class DTCC_listener
  def initialize(feed = RSS_FEED)
    @feed = feed
    @csv_header = COLUMNS.to_csv
  end


  # Open the RSS_FEED and consume as much as they'll send
  def self.read_rss_feed
    # Read the feed into rss_content
    rss_content = ""
    begin
      open(RSS_FEED) do |f|
        rss_content = f.read
      end
    rescue
      $LOG.error "Problem connecting to #{RSS_FEED}"
      return nil
    end

    $LOG.debug "Received #{rss_content} raw data. Parsing as rss..."

    # First parse RSS with validation, and if it is not valid parse with non-validation.
    rss = nil
    begin
      rss = RSS::Parser.parse(rss_content)
    rescue RSS::InvalidRSSError
      rss = RSS::Parser.parse(rss_content, false)
    end

    # did we get any raw rss trades?
    return nil if rss.nil?
    return nil if rss.channel.items.count == 0

    $LOG.debug "Received #{rss.channel.items.count} raw rss trades"

    raw_trades = []
    # do some checks on parsed rss
    rss.channel.items.each do |item|

      trade = {}

      # Extract the numeric GUID from thelong form guid
      # https://kgc0418-tdw-data-0.s3.amazonaws.com/slices/FOREX_RSS_FEED.rss#935534398</guid>
      # we just want the number at the end = 935534398 to use as our unique hash key
      guid_str = "#{item.guid}"
      guid_reg_exp = /rss#(.*)<\/guid>$/
      guid = guid_str.match ( guid_reg_exp )

      # This is a new trade - create its own hash and start filling in some fields
      trade[:guid] = $1  if guid
      trade[:title] = item.title
      trade[:pub_date] = item.date
      trade[:description] = item.description.gsub("\n","")  

      raw_trades << trade
      $LOG.debug "Processed RSS trade #{trade.inspect}"
    end

    return raw_trades # => should never be empty and is an array of hashes 
  end


  # Returns an array of trades returned from polling the RSS feed
  def poll
    rss_trades = DTCC_listener::read_rss_feed()

    return nil if rss_trades.nil? 
    return nil if rss_trades.empty? 

    new_trades = []

    # Loop over each item on the feed
    rss_trades.each do |item|
      trade = {}
      trade[:title] = item[:title]
      trade[:rss_guid] = item[:guid]

      begin
        item[:pub_date] && trade[:pub_date] = Time_hash.new(item[:pub_date]).to_hash
      rescue 
        $LOG.warning "Error parsing RSS feed 'pub_date' field"
      end

      # item.description contains a CSV row with all the trade data
      # Add a header row and then use the CSV class to help us simply parse using the headers as keys
      csv_str = @csv_header + item[:description] 
      csv = CSV::parse(csv_str, :headers => :first_row, :return_headers => true)

      $LOG.debug "Parsed CSV: #{csv.to_a.inspect}"

      # Common fields
      trade[:dtcc_id] = csv['DISSEMINATION_ID'][1]
      trade[:orig_dtcc_id] = csv['ORIGINAL_DISSEMINATION_ID'][1]
      trade[:asset] = csv['ASSET_CLASS'][1]
      trade[:taxonomy] = csv['TAXONOMY'][1]
      trade[:status] = csv['PRICE_FORMING_CONTINUATION_DATA'][1]
      trade[:und] = csv['NOTIONAL_CURRENCY_1'][1]
      trade[:acc] = csv['NOTIONAL_CURRENCY_2'][1]
      begin
        csv['ROUNDED_NOTIONAL_AMOUNT_1'][1] && trade[:und_not] = csv['ROUNDED_NOTIONAL_AMOUNT_1'][1].delete(',').to_f
        csv['ROUNDED_NOTIONAL_AMOUNT_2'][1] && trade[:acc_not] = csv['ROUNDED_NOTIONAL_AMOUNT_2'][1].delete(',').to_f
      rescue
        $LOG.warning "Error parsing RSS feed 'notional' fields"
      end

      begin
        csv['EXECUTION_TIMESTAMP'][1] && trade[:time_stamp] = Time_hash.new(csv['EXECUTION_TIMESTAMP'][1]).to_hash
        # csv['EXECUTION_TIMESTAMP'][1] && trade[:time_stamp] = Time.parse(csv['EXECUTION_TIMESTAMP'][1]).utc
      rescue
        $LOG.warning "Error parsing RSS feed 'EXECUTION_TIMESTAMP' CSV field"
      end

      if trade[:taxonomy] == "ForeignExchange:NDF"
        # do something with NDFs

      elsif ( trade[:taxonomy] == "ForeignExchange:VanillaOption" || 
        trade[:taxonomy] == "ForeignExchange:NDO" || 
        trade[:taxonomy] == "ForeignExchange:SimpleExotic:Barrier" ||
        trade[:taxonomy] == "ForeignExchange:ComplexExotic" )
        # do something with vanilla options, NDOs 

        trade[:prem_ccy]  = csv['OPTION_CURRENCY'][1]
        
        begin
           csv['OPTION_PREMIUM'][1] && trade[:prem] = csv['OPTION_PREMIUM'][1].delete(',').to_f
           csv['OPTION_STRIKE_PRICE'][1] && trade[:strike] = csv['OPTION_STRIKE_PRICE'][1].delete(',').to_f
           csv['OPTION_TYPE'][1] && trade[:type]  =  csv['OPTION_TYPE'][1].delete('-')
         rescue
           $LOG.warning "Error parsing RSS feed 'OPTION_PREMIUM', 'OPTION_STRIKE_PRICE', 'OPTION_TYPE' fields"
         end
         
         begin
           csv['OPTION_EXPIRATION_DATE'][1] && trade[:expiry] = Time_hash.new(csv['OPTION_EXPIRATION_DATE'][1]).to_hash
           # csv['OPTION_EXPIRATION_DATE'][1] && trade[:expiry]  = Time.parse(csv['OPTION_EXPIRATION_DATE'][1]).utc
         rescue
           $LOG.warning "Error parsing RSS feed 'EXECUTION_TIMESTAMP' CSV field"
         end
      end

      # add a new trade to the new_trades array[]
      new_trades << Trade.new(trade)
      $LOG.info "Parsed new trade: #{trade.inspect}"

    end

    return new_trades
  end
end

