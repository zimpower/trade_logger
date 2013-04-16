#
#  Parse the trade events and extract useful information and saves it to a file in YAML format
require "mongo"
require "logger"
require './trade'  
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

          if @trades.find( :dtcc_id => trade.data[:dtcc_id] ).to_a.size != 0
            # handle case where we already have the id
          else

            # Enhance trade with meta_data 
            # 1- add a spot ref
            begin
              pair = "#{trade.data[:und]}#{trade.data[:acc]}"
              spot_ref = @fx_cache.spot(pair)
              trade.data[:m_spot_ref] = spot_ref  if spot_ref
              $LOG.debug "Added spot ref #{pair}: #{spot_ref}"

            rescue
              $LOG.warn "Error adding spot_ref using FX service for pair #{pair}: ERROR : #{$!}"
            end

            # 2- add a usd equiv notional
            begin
              usd_equiv_not = -1

              if trade.data[:und].upcase == "USD"
                usd_equiv_not = trade.data[:und_not]
                trade.data[:m_usd_equiv_not] = usd_equiv_not
                $LOG.debug "Added USD Equiv Notional of #{usd_equiv_not}"
                
              elsif trade.data[:acc].upcase == "USD"
                usd_equiv_not = trade.data[:acc_not]
                trade.data[:m_usd_equiv_not] = usd_equiv_not
                $LOG.debug "Added USD Equiv Notional of #{usd_equiv_not}"
                
              else
                pair = "USD#{trade.data[:und]}"
                usdccy = @fx_cache.spot(pair)
                $LOG.debug "#{pair} spot ref : #{usdccy}"

                if usdccy && usdccy != 0
                  usd_equiv_not = trade.data[:und_not] / usdccy
                  trade.data[:m_usd_equiv_not] = usd_equiv_not
                  $LOG.debug "Added USD Equiv Notional of #{usd_equiv_not}"
                end
              end
            rescue
              $LOG.warn "Error adding USD equiv Not for pair #{pair}: ERROR : #{$!}"
            end

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
    $LOG.info "Closing connection to #{HOST}"
    @mgo_client.close
  end


  # Add to mongo database in collection trade_list in db dtcc_trades
  def handle_new_trade( trade )

    begin
      id = @trades.insert( trade.data )
    rescue Mongo::OperationFailure => e
      $LOG.error "Attempting to insert #{trade_data.inspect} into #{HOST}:#{DB}:#{COLLECTION}: #{e}"  
    else
      $LOG.info "Added new trade: #{ @trades.find('_id' => id).to_a }"
    end

    #  Add to yaml log file
    # trade.append("logs/trade_log.yaml", :yaml)
    # 
    #  Add to json log file
    # trade.append("logs/trade_log.json", :json)            

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


