#
#  Parse the trade events and extract useful information and saves it to a file in YAML format
require "mongo"
require "logger"
require './dtcc_rss_client'
require './fx_feeds'

HOST = "centenary.local"
DB = "dtcc_trades"
COLLECTION = "trades"
SLEEP = 30

$LOG = Logger.new('/usr/local/var/log/dtcc_logger/dtcc_logger.log', 'daily')
$LOG.level = Logger::DEBUG

class DTCC_logger
  def initialize
    @mgo_cleint= nil
    @trades = nil
    @rss_client = nil
    @fx_cache = FX.new

    init_db()
    init_rss()
  end

  def init_db
    $LOG.info "Attempting connection to #{HOST}..."
    begin
      @mgo_client = Mongo::MongoClient.new(HOST) 
    rescue Mongo::ConnectionFailure => e
      $LOG.error "Attempting to connect to '#{HOST}' : #{exception_message(e)}"  
      raise e
    else
      $LOG.info "Success : connected to #{HOST}"
    end

    @trades = @mgo_client.db(DB).collection(COLLECTION)
  end

  def init_rss
    @rss_client = DTCC_listener.new
  end


  def start
    loop do
      new_trades = @rss_client.poll

      if (new_trades!= nil)
        i = 0

        $LOG.info "Received #{new_trades.count}"   if  i > 0

        new_trades.each do |trade|

          if @trades.find( :dtcc_id => trade[:dtcc_id] ).to_a.size != 0
            $LOG.info "#{trade[:dtcc_id]} : Ignoring trade : already in db"

            # handle case where we already have the id
          else

            add_meta_data (trade )
            handle_new_trade( trade )
            i += 1
          end
        end

        $LOG.info "Received #{i} new trades #{new_trades.count-i} repeat trades"   if  i > 0
      end
      sleep SLEEP
    end
  end


  def stop
    puts "Closing connection to #{HOST}:#{DB}"
    $LOG.info "Closing connection to #{HOST}:#{DB}"
    @mgo_client.close
  end

  # Add addition data to the trdae record
  def add_meta_data(trade)
    # Enhance trade with meta_data 
    # 1- add a spot ref
    begin
      pair = "#{trade[:und]}#{trade[:acc]}"
      spot_ref = @fx_cache.spot(pair)
      trade[:m_spot_ref] = spot_ref  if spot_ref
      $LOG.debug "#{trade[:dtcc_id]} : Added spot ref #{pair}: #{spot_ref}"

    rescue
      $LOG.warn "#{trade[:dtcc_id]} : Error adding spot_ref using FX service for pair #{pair}: ERROR : #{$!}"
    end

    # 2- add a usd equiv notional
    begin
      usd_equiv_not = -1

      if trade[:und].upcase == "USD"
        usd_equiv_not = trade[:und_not]
        trade[:m_usd_equiv_not] = usd_equiv_not
        $LOG.debug "#{trade[:dtcc_id]} : Added USD Equiv Notional of #{usd_equiv_not}"

      elsif trade[:acc].upcase == "USD"
        usd_equiv_not = trade[:acc_not]
        trade[:m_usd_equiv_not] = usd_equiv_not
        $LOG.debug "#{trade[:dtcc_id]} : Added USD Equiv Notional of #{usd_equiv_not}"

      else
        pair = "USD#{trade[:und]}"
        usdccy = @fx_cache.spot(pair)
        $LOG.debug "#{trade[:dtcc_id]} : #{pair} spot ref : #{usdccy}"

        if usdccy && usdccy != 0
          usd_equiv_not = trade[:und_not] / usdccy
          trade[:m_usd_equiv_not] = usd_equiv_not
          $LOG.debug "#{trade[:dtcc_id]} : Added USD Equiv Notional of #{usd_equiv_not}"
        end
      end
    rescue
      $LOG.warn "#{trade[:dtcc_id]} : Error adding USD equiv Not for pair #{pair}: ERROR : #{$!}"
    end
  end



  # Add to mongo database in collection trade_list in db dtcc_trades
  def handle_new_trade( trade )

    begin
      id = @trades.insert( trade )
    rescue Mongo::OperationFailure => e
      $LOG.error "#{trade[:dtcc_id]} : Failed to insert #{trade_data.inspect} into #{HOST}:#{DB}:#{COLLECTION} : Error #{e}"  
    else
      $LOG.info "#{trade[:dtcc_id]} : Added new trade into #{HOST}:#{DB}:#{COLLECTION} #{ @trades.find('_id' => id).to_a }"
    end
  
  end

  def exception_message(e)
    msg = [ "Exception #{e.class} -> #{e.message}" ]

    base = File.expand_path(Dir.pwd) + '/'
    e.backtrace.each do |t|
      msg << "   #{File.expand_path(t).gsub(/#{base}/, '')}"
    end

    msg.join("\n")
  end

end

if $0 == __FILE__ then

  dtcc = DTCC_logger.new
  dtcc.start

  # hit Control + C to stop
  Signal.trap("INT") { dtcc.stop }
  Signal.trap("TERM") { dtcc.stop }
  dtcc.stop
end


