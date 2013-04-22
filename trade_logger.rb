#
#  Parse the trade events and extract useful information and saves it to a file in YAML format
# require "mongo"
require "logger"
require 'mongoid'       # => Use Monogoid instead of mongo to access mongodb directly
$:.unshift File.dirname(__FILE__)
require 'dtcc_rss_client'
require 'fx_feeds'
require 'trade'

HOST = "centenary.local"
DB = "dtcc_trades"
COLLECTION = "trades_test"
SLEEP = 30

class DTCC_logger
  def initialize
    @rss_client = nil
    @fx_cache = FX.new
    $LOG = Logger.new('logs/logger.log', 'daily')
    $LOG.level = Logger::DEBUG

    init_db()
    init_rss()
  end

  def init_db
    $LOG.info "Attempt : connect to #{HOST}..."
    Mongoid.load!("mongoid.yml", :development)
    Mongoid.raise_not_found_error = true
  rescue 
    $LOG.error "Failed : to connect to '#{HOST}'" 
    raise 
  else
    $LOG.info "Success : connected to #{HOST}"
  end

  def init_rss
    @rss_client = DTCC_listener.new("logs/logger.log")
  end


  def start
    loop do
      new_trades = @rss_client.poll

      unless new_trades.nil? 
        $LOG.info "Received #{new_trades.count}"

        i = 0
        new_trades.each do |trade|
          begin
            Trade.find( dtcc_id: trade[:dtcc_id] )
          rescue Mongoid::Errors::DocumentNotFound => e   # =>  good it is a new trade - lets add it
            add_meta_data ( trade )
            handle_new_trade( trade )
            i += 1
          else    # => handle case where we already have the id
            $LOG.info "#{trade[:dtcc_id]} : Ignoring trade : already in db"
          end
        end
        $LOG.info "Received #{i} new trades #{new_trades.count-i} repeat trades"
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
      alpha_pair = trade[:und] < trade[:acc] ? trade[:und] + trade[:acc] : trade[:acc] + trade[:und]
      
      trade[:m_alpha_pair] = alpha_pair  if alpha_pair
      
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
      db_trade = Trade.create( trade )
    rescue 
      $LOG.error "#{trade[:dtcc_id]} : Failed to insert #{trade_data.inspect} into #{HOST}:#{DB}:#{COLLECTION} : Error #{$!}"  
    else
      $LOG.info "#{trade[:dtcc_id]} : Added new trade into #{HOST}:#{DB}:#{COLLECTION} #{ db_trade }"
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


